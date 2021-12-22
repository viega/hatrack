/*
 * Copyright © 2021 John Viega
 *
 * See LICENSE.txt for licensing info.
 *
 *  Name:           swimcap2.c
 *  Description:    Single WrIter, Multiple-read, Crappy, Albeit Parallel.
 *  Author:         John Viega, john@zork.org
 */

#ifndef __SWIMCAP2_H__
#define __SWIMCAP2_H__

#include "hatrack_common.h"
#include "pthread.h"

typedef struct {
    alignas(32) hatrack_hash_t hv;
    void *item;
    bool  deleted;
} swimcap2_bucket_t;

typedef struct {
    alignas(32) uint64_t last_slot;
    uint64_t          threshold;
    uint64_t          used_count;
    swimcap2_bucket_t buckets[];
} swimcap2_store_t;

typedef struct {
    alignas(32) uint64_t item_count;
    swimcap2_store_t *store;
    pthread_mutex_t   write_mutex;
} swimcap2_t;

void  swimcap2_init(swimcap2_t *);
void *swimcap2_get(swimcap2_t *, hatrack_hash_t *, bool *);
void *swimcap2_base_put(swimcap2_t *, hatrack_hash_t *, void *, bool, bool *);
void *swimcap2_put(swimcap2_t *, hatrack_hash_t *, void *, bool *);
bool  swimcap2_put_if_empty(swimcap2_t *, hatrack_hash_t *, void *);
void *swimcap2_remove(swimcap2_t *, hatrack_hash_t *, bool *);
void  swimcap2_delete(swimcap2_t *);
uint64_t        swimcap2_len(swimcap2_t *);
hatrack_view_t *swimcap2_view(swimcap2_t *, uint64_t *);

#endif
