/*
 * Copyright © 2021 John Viega
 *
 * See LICENSE.txt for licensing info.
 *
 *  Name:           swimcap.c
 *  Description:    Single WrIter, Multiple-read, Crappy, Albeit Parallel.
 *  Author:         John Viega, john@zork.org
 */

#ifndef __SWIMCAP_H__
#define __SWIMCAP_H__

#include "hatrack_common.h"
#include "pthread.h"

typedef struct {
    alignas(32) hatrack_hash_t hv;
    void *item;
    bool  deleted;
#ifndef HATRACK_DONT_SORT
    uint64_t epoch;
#endif
} swimcap_bucket_t;

typedef struct {
    alignas(32) uint64_t last_slot;
    uint64_t         threshold;
    uint64_t         used_count;
    _Atomic uint64_t readers;
    swimcap_bucket_t buckets[];
} swimcap_store_t;

typedef struct {
    alignas(32) uint64_t item_count;
    swimcap_store_t *store;
    pthread_mutex_t  write_mutex;

#ifndef HATRACK_DONT_SORT
    uint64_t next_epoch;
#endif
} swimcap_t;

static inline swimcap_store_t *
swimcap_reader_enter(swimcap_t *self)
{
    swimcap_store_t *ret;

    // We could use a read-write mutex, but that would unnecessarily
    // block both reads and writes.  The store won't be retired until
    // there are definitely no readers, so as long as we can
    // atomically read the pointer to the store and register our
    // reference to the store, we are fine. As an alternative, we
    // could do this without locks using a 128-bit CAS that holds the
    // pointer, or use mmm_alloc() for the stores.
    pthread_mutex_lock(&self->write_mutex);
    ret = self->store;
    atomic_fetch_add(&ret->readers, 1);
    pthread_mutex_unlock(&self->write_mutex);

    return ret;
}

static inline void
swimcap_reader_exit(swimcap_store_t *store)
{
    atomic_fetch_sub(&store->readers, 1);

    return;
}

void     swimcap_init(swimcap_t *);
void    *swimcap_get(swimcap_t *, hatrack_hash_t *, bool *);
void    *swimcap_base_put(swimcap_t *, hatrack_hash_t *, void *, bool, bool *);
void    *swimcap_put(swimcap_t *, hatrack_hash_t *, void *, bool *);
bool     swimcap_put_if_empty(swimcap_t *, hatrack_hash_t *, void *);
void    *swimcap_remove(swimcap_t *, hatrack_hash_t *, bool *);
void     swimcap_delete(swimcap_t *);
uint64_t swimcap_len(swimcap_t *);
hatrack_view_t *swimcap_view(swimcap_t *, uint64_t *);

#endif
