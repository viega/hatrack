/*
 * Copyright © 2021 John Viega
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *  Name:           swimcap.h
 *  Description:    Single WrIter, Multiple-read, Crappy, Albeit Parallel.
 *
 *                  This uses a per-data structure lock that writers hold
 *                  for their entire operation.
 *
 *                  Readers also use the lock, but only for a minimal
 *                  amount of time-- enough time to grab a pointer to
 *                  the current store, and to increment a reference
 *                  count in that store.  The lock does not need to
 *                  be held when readers exit.
 *
 *  Author:         John Viega, john@zork.org
 *
 */

#ifndef __SWIMCAP_H__
#define __SWIMCAP_H__

#include "hatrack_common.h"

#include <pthread.h>

// clang-format off
typedef struct {
    hatrack_hash_t      hv;
    void               *item;
    bool                deleted;
#ifndef HATRACK_DONT_SORT
    uint64_t            epoch;
#endif
} swimcap_bucket_t;

typedef struct {
    uint64_t            last_slot;
    uint64_t            threshold;
    uint64_t            used_count;
    _Atomic uint64_t    readers;
    swimcap_bucket_t    buckets[];
} swimcap_store_t;

typedef struct {
    uint64_t            item_count;
    swimcap_store_t    *store;
    pthread_mutex_t     write_mutex;

#ifndef HATRACK_DONT_SORT
    uint64_t            next_epoch;
#endif
} swimcap_t;
// clang-format on

static inline swimcap_store_t *
swimcap_reader_enter(swimcap_t *self)
{
    swimcap_store_t *ret;

    /* We could use a read-write mutex, but that would unnecessarily
     * block both reads and writes.  The store won't be retired until
     * there are definitely no readers, so as long as we can
     * atomically read the pointer to the store and register our
     * reference to the store, we are fine. As an alternative, we
     * could do this without locks using a 128-bit CAS that holds the
     * pointer, or use mmm_alloc() for the stores (which we do in
     * the swimcap2 implementation).
     */
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

// clang-format off
void            swimcap_init        (swimcap_t *);
void           *swimcap_get         (swimcap_t *, hatrack_hash_t *, bool *);
void           *swimcap_base_put    (swimcap_t *, hatrack_hash_t *, void *,
				     bool, bool *);
void           *swimcap_put         (swimcap_t *, hatrack_hash_t *, void *,
				     bool *);
bool            swimcap_put_if_empty(swimcap_t *, hatrack_hash_t *, void *);
void           *swimcap_remove      (swimcap_t *, hatrack_hash_t *, bool *);
void            swimcap_delete      (swimcap_t *);
uint64_t        swimcap_len         (swimcap_t *);
hatrack_view_t *swimcap_view        (swimcap_t *, uint64_t *);

#endif
