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
 *  Name:           swimcap2.h
 *  Description:    Single WrIter, Multiple-read, Crappy, Albeit Parallel, v2.
 *
 *                  This uses a per-data structure lock that writers hold
 *                  for their entire operation.
 *
 *                  In this version, readers do NOT use the lock;
 *                  in fact, they are fully wait free.
 *
 *                  Instead, we use an epoch-based memory management
 *                  scheme on our current data store, to make sure that
 *                  a store cannot be deleted while we are reading it,
 *                  even if a resize has completed.
 *
 *  Author:         John Viega, john@zork.org
 *
 */

#ifndef __SWIMCAP2_H__
#define __SWIMCAP2_H__

#include "hatrack_common.h"

#include <pthread.h>

// clang-format off
typedef struct {
    hatrack_hash_t hv;
    void          *item;
    bool           deleted;
} swimcap2_bucket_t;

typedef struct {
    alignas(8) uint64_t last_slot;
    uint64_t            threshold;
    uint64_t            used_count;
    swimcap2_bucket_t   buckets[];
} swimcap2_store_t;

typedef struct {
    alignas(8) uint64_t item_count;
    swimcap2_store_t   *store;
    pthread_mutex_t     write_mutex;
} swimcap2_t;

void            swimcap2_init(swimcap2_t *);
void           *swimcap2_get(swimcap2_t *, hatrack_hash_t *, bool *);
void           *swimcap2_base_put(swimcap2_t *, hatrack_hash_t *, void *,
				  bool, bool *);
void           *swimcap2_put(swimcap2_t *, hatrack_hash_t *, void *, bool *);
bool            swimcap2_put_if_empty(swimcap2_t *, hatrack_hash_t *, void *);
void           *swimcap2_remove(swimcap2_t *, hatrack_hash_t *, bool *);
void            swimcap2_delete(swimcap2_t *);
uint64_t        swimcap2_len(swimcap2_t *);
hatrack_view_t *swimcap2_view(swimcap2_t *, uint64_t *);
// clang-format on

#endif
