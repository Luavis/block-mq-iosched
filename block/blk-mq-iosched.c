/*
 * Copyright (C) 2017 Luavis
 *
 * Linux multiqueue block device scheduler
 *
 */

#include <linux/module.h>
#include <linux/bio.h>
#include <linux/blkdev.h>
#include <linux/sched.h>
#include <linux/list.h>
#include <linux/blk-mq-iosched.h>
// #define DEBUG_IOSCHED

static struct workqueue_struct *kthrotld_workqueue;
// do not working with cpuid: guess
static struct list_head *mq_iosched_lists;
static struct work_struct *mq_iosched_dispatch_works;
static struct timer_list pending_timer;
static struct blk_mq_iosched_payload **mq_iosched_min_payloads;


static void blk_mq_iosched_work_fn(struct work_struct *work);
bool blk_mq_iosched_checks(struct request_queue *q, struct bio *bio);
static void blk_mq_iosched_timer_fn(unsigned long arg);
static struct blk_mq_iosched_payload *
blk_mq_iosched_list_add
(struct task_struct *task, struct bio *bio, unsigned long long vruntime);
static int
blk_mq_apply_vruntime(struct blk_mq_iosched_payload *payload, struct bio *bio);
static struct blk_mq_iosched_payload *
blk_mq_iosched_get_min_payload(int cpu_id);
static void blk_mq_iosched_pending_timer(void);
static void blk_mq_iosched_cleanup_payload(int cpud_id);
static struct blk_mq_iosched_payload *
_blk_mq_iosched_get_min_payload(int cpu_id);
void blk_mq_iosched_refresh_min_payload(int cpu_id);
static void
blk_mq_iosched_flush_payload
(struct blk_mq_iosched_payload *min_payload);

/*
 * If it have to schedule return false,
 * If not return true.
 */
bool
blk_mq_iosched_checks(struct request_queue *q, struct bio *bio)
{
    struct blk_mq_iosched_payload *min_payload;
    struct blk_mq_iosched_payload *current_payload;
    int cpu_id = get_cpu();
    bool retval = true;
    unsigned long long min_vruntime = 0;
    // preempt_disable();
    if(q->nr_hw_queues < 2)
        goto exit;
    current_payload = bio->iosched_payload;

    if(current_payload != NULL) {
        blk_mq_apply_vruntime(current_payload, bio);
        goto exit;
    }
#ifdef DEBUG_IOSCHED
    pr_info("IOSCHED: checks cpu_id: %d\n", cpu_id);
#endif
    min_payload = blk_mq_iosched_get_min_payload(cpu_id);
    if(min_payload != NULL)
        min_vruntime = min_payload->vruntime;

#ifdef DEBUG_IOSCHED
    pr_info("IOSCHED: %p minvrun %llu\n", min_payload, min_vruntime);
#endif
    if(current_payload == NULL) {
        current_payload = blk_mq_iosched_list_add(current, bio, min_vruntime);
        bio->iosched_payload = current_payload;
    }
    // else if(min_payload == NULL)
    //    current_payload->vruntime = 0;

#ifdef DEBUG_IOSCHED
    pr_info("IOSCHED: %p vrun %llu\n", current_payload, current_payload->vruntime);
#endif
    if(current_payload->vruntime <= min_vruntime) {
        blk_mq_apply_vruntime(current_payload, bio);
        // refresh blk-mq iosched min task
        blk_mq_iosched_refresh_min_payload(cpu_id);
        goto exit;
    }
    else {
        bio_list_add(current_payload->queue, bio);
        retval = false;
    }

exit:
    put_cpu();
    // preempt_enable();
    return retval;
}

static void blk_mq_iosched_work_fn(struct work_struct *work) {
    int cpu_id = get_cpu();
    struct blk_mq_iosched_payload *min_payload = NULL;
    min_payload = blk_mq_iosched_get_min_payload(cpu_id);
#ifdef DEBUG_IOSCHED
//    pr_info("IOSCHED: min_payload: %p cpu_id: %d\n", min_payload, cpu_id);
#endif
    if(min_payload != NULL) {
        blk_mq_iosched_flush_payload(min_payload);
    }
    blk_mq_iosched_cleanup_payload(cpu_id);
    blk_mq_iosched_refresh_min_payload(cpu_id);

    put_cpu();
}

static void
blk_mq_iosched_flush_payload
(struct blk_mq_iosched_payload *min_payload) {
    struct blk_plug plug;
    struct bio *bio;
    blk_start_plug(&plug);

    while((bio = bio_list_pop(min_payload->queue))) {
#ifdef DEBUG_IOSCHED
        pr_info("IOSCHED: flush! bio[%p]\n", bio);
#endif
        generic_make_request(bio);
    }
    blk_finish_plug(&plug);
}

static void blk_mq_iosched_pending_timer(void) {
    // timer fire with 1ms
    mod_timer(&pending_timer, jiffies + usecs_to_jiffies(500));
}

static void blk_mq_iosched_timer_fn(unsigned long arg) {
    int cpu_id = 0;
    for(cpu_id = 0; cpu_id < nr_cpu_ids; cpu_id++) {
        queue_work_on(
            cpu_id,
            kthrotld_workqueue,
            &mq_iosched_dispatch_works[cpu_id]
        );
    }

    blk_mq_iosched_pending_timer();
}

static struct blk_mq_iosched_payload *
blk_mq_iosched_list_add
(struct task_struct *task, struct bio *bio, unsigned long long vruntime) {
    struct blk_mq_iosched_payload *p;
    int cpu_id = task_cpu(task);

    list_for_each_entry(p, &mq_iosched_lists[cpu_id], list) {
        if(task == p->task)
            goto exit;
    }
    p = kmalloc(sizeof(struct blk_mq_iosched_payload), GFP_KERNEL);
    memset(p, 0, sizeof(*p));
    p->task = task;
    p->vruntime = vruntime;
    p->queue = kmalloc(sizeof(struct bio_list), GFP_KERNEL);
    bio_list_init(p->queue);
    p->is_busy = true;
    list_add(&p->list, &mq_iosched_lists[cpu_id]);
exit:
    return p;
}

static int
blk_mq_apply_vruntime(struct blk_mq_iosched_payload *payload, struct bio *bio) {
    bool rw = bio_data_dir(bio);
    int size;
    int weight = 1;

    if(rw == READ)
        weight = 2;
    else if(rw == WRITE)
        weight = 3;

    size = bio->bi_iter.bi_size * weight;
    payload->vruntime += size;

    return size;
}

void blk_mq_iosched_refresh_min_payload(int cpu_id) {
    mq_iosched_min_payloads[cpu_id] = NULL;
    blk_mq_iosched_get_min_payload(cpu_id);
}

static void blk_mq_iosched_cleanup_payload(int cpu_id) {
    struct list_head *p;
    struct list_head *n;
    struct blk_mq_iosched_payload *payload;
    list_for_each_safe(p, n, &mq_iosched_lists[cpu_id]) {
        payload = list_entry(p, struct blk_mq_iosched_payload, list);

        if(payload->is_busy && bio_list_empty(payload->queue)) {
            payload->is_busy = false;
        }
        else if(!payload->is_busy && bio_list_empty(payload->queue)) {
            list_del(&payload->list);
            // kfree(payload->queue);
            // payload->queue = NULL;
            // kfree(payload);
        }
        else {
            payload->is_busy = true;
        }
    }
}

static struct blk_mq_iosched_payload *
blk_mq_iosched_get_min_payload(int cpu_id) {
    if(mq_iosched_min_payloads[cpu_id] == NULL)
        mq_iosched_min_payloads[cpu_id] =
            _blk_mq_iosched_get_min_payload(cpu_id);

     return mq_iosched_min_payloads[cpu_id];
}

struct blk_mq_iosched_payload *
_blk_mq_iosched_get_min_payload(int cpu_id) {
    struct blk_mq_iosched_payload *ret = NULL;
    struct blk_mq_iosched_payload *p;
    unsigned long long min_vruntime = ULLONG_MAX;

    list_for_each_entry(p, &mq_iosched_lists[cpu_id], list) {
        if(p->vruntime < min_vruntime) {
            min_vruntime = p->vruntime;
            ret = p;
        }
    }

    return ret;
}

static int __init blk_mq_iosched_init(void)
{
    int i = 0;
#ifdef DEBUG_IOSCHED
    pr_info("IOSCHED: init cpu count: %d\n", nr_cpu_ids);
 #endif
    kthrotld_workqueue = alloc_workqueue("kblkschedd", WQ_MEM_RECLAIM, 0);
    mq_iosched_lists = kmalloc(
            sizeof(struct list_head) * nr_cpu_ids,
            GFP_KERNEL
    );
    memset(mq_iosched_lists, 0, sizeof(struct list_head) * nr_cpu_ids);
    mq_iosched_min_payloads = kmalloc(
            sizeof(struct blk_mq_iosched_payload *) * nr_cpu_ids,
            GFP_KERNEL
    );
    memset(
            mq_iosched_min_payloads,
            0,
            sizeof(struct blk_mq_iosched_payload *) * nr_cpu_ids
    );

    mq_iosched_dispatch_works = kmalloc(
            sizeof(struct work_struct) * nr_cpu_ids,
            GFP_KERNEL
    );
    memset(
            mq_iosched_dispatch_works,
            0,
            sizeof(struct work_struct) * nr_cpu_ids
    );
    for(i = 0; i < nr_cpu_ids; i++) {
        INIT_LIST_HEAD(&mq_iosched_lists[i]);
        INIT_WORK(&mq_iosched_dispatch_works[i], blk_mq_iosched_work_fn);
    }

    setup_timer(
        &pending_timer,
        blk_mq_iosched_timer_fn,
        0
    );

    blk_mq_iosched_pending_timer();

    return 0;
}

module_init(blk_mq_iosched_init);
