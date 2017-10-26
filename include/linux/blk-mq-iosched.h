/*
 * Copyright (C) 2017 Luavis
 *
 * Linux multiqueue block device scheduler
 *
 */

#ifndef __BLK_MQ_IOSCHED_H
#define __BLK_MQ_IOSCHED_H

#include <linux/list.h>

struct bio;
struct bio_list;
struct task_struct;

struct blk_mq_iosched_payload {
    unsigned long long vruntime;
    struct task_struct *task;
    struct bio_list *queue;
    bool is_busy;
    struct list_head list;
};

#endif
