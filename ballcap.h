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
 *  Name:           ballcap.h
 *  Description:    Besides a Lot of Locking, Clearly Awesomely Parallel
 *
 *                  Uses pthread locks on a per-bucket basis, and
 *                  allows multiple simultaneous writers, except when
 *                  performing table migration.
 *
 *                  Also uses our strategy from Lohat to ensure we can
 *                  provide a fully consistent ordered view of the hash
 *                  table.
 *
 *  Author:         John Viega, john@zork.org
 *
 */

#ifndef __BALLCAP_H__
#define __BALLCAP_H__

#include "hatrack_common.h"

#include <pthread.h>

// clang-format off
typedef struct ballcap_record_st ballcap_record_t;

struct ballcap_record_st {
    bool                 deleted;
    void                *item;
    ballcap_record_t    *next;
};

typedef struct {
    hatrack_hash_t       hv;
    ballcap_record_t    *record;
    bool                 record_retired;
    bool                 migrated;
    pthread_mutex_t      mutex;
} ballcap_bucket_t;

typedef struct {
    uint64_t             last_slot;
    uint64_t             threshold;
    uint64_t             used_count;
    ballcap_bucket_t     buckets[];
} ballcap_store_t;

typedef struct {
    uint64_t             item_count;
    uint64_t             next_epoch;
    ballcap_store_t     *store;
    pthread_mutex_t      migrate_mutex;
} ballcap_t;

void            ballcap_init   (ballcap_t *);
void           *ballcap_get    (ballcap_t *, hatrack_hash_t *, bool *);
void           *ballcap_put    (ballcap_t *, hatrack_hash_t *, void *, bool *);
void           *ballcap_replace(ballcap_t *, hatrack_hash_t *, void *, bool *);
bool            ballcap_add    (ballcap_t *, hatrack_hash_t *, void *);
void           *ballcap_remove (ballcap_t *, hatrack_hash_t *, bool *);
void            ballcap_delete (ballcap_t *);
uint64_t        ballcap_len    (ballcap_t *);
hatrack_view_t *ballcap_view   (ballcap_t *, uint64_t *, bool);

#endif
