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
 *  Name:           newshat.h
 *  Description:    Now Everyone Writes Simultaneously (HAsh Table)
 *
 *                  Uses pthread locks on a per-bucket basis, and
 *                  allows multiple simultaneous writers, except when
 *                  performing table migration.
 *
 *  Author:         John Viega, john@zork.org
 *
 */

#ifndef __NEWSHAT_H__
#define __NEWSHAT_H__

#include "hatrack_common.h"

#include <pthread.h>

// clang-format off
typedef struct {
    hatrack_hash_t       hv;
    void                *item;
    bool                 deleted;
    bool                 migrated;
    pthread_mutex_t      write_mutex;
} newshat_bucket_t;

typedef struct {
    uint64_t             last_slot;
    uint64_t             threshold;
    uint64_t             used_count;
    newshat_bucket_t     buckets[];
} newshat_store_t;

typedef struct {
    uint64_t             item_count;
    newshat_store_t     *store;
    pthread_mutex_t      migrate_mutex;
} newshat_t;

void            newshat_init        (newshat_t *);
void           *newshat_get         (newshat_t *, hatrack_hash_t *, bool *);
void           *newshat_base_put    (newshat_t *, hatrack_hash_t *, void *,
				     bool, bool *);
void           *newshat_put         (newshat_t *, hatrack_hash_t *, void *,
				     bool *);
bool            newshat_put_if_empty(newshat_t *, hatrack_hash_t *, void *);
void           *newshat_remove      (newshat_t *, hatrack_hash_t *, bool *);
void            newshat_delete      (newshat_t *);
uint64_t        newshat_len         (newshat_t *);
hatrack_view_t *newshat_view        (newshat_t *, uint64_t *);

#endif
