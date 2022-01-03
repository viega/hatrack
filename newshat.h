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

/* newshat_record_t
 *
 * Each newshat bucket is individually locked, so that only one thread
 * can be writing to the bucket at a time. However, there may be
 * multiple readers in parallel. The things a reader might need should
 * be updated atomically, and are stored in newshat_record_t.
 *
 * item -- The item passed to the hash table, usually a key : value
 *         pair of some sort.
 *
 * info -- If the field is 0, then it indicates a deleted item.
 *         Otherwise, it represents the "epoch"-- an indication of the
 *         creation time of the item, relative to other items. Note
 *         that, since this table does not provide fully consistent
 *         views, the epoch is not quite as accurate as with other
 *         table implementations in the hatrack. In particular, bumps
 *         to the newshat_t data structure's next_epoch value (see
 *         below), are racy, so multiple data items can definitely get
 *         the same epoch value, meaning we have no linearization
 *         point on which to construct a consistent sort order.
 */

typedef struct {
    void                *item;
    uint64_t             info;
} newshat_record_t;

#if 0
    void                *item;
    bool                 deleted;
    uint64_t             epoch;
#endif

typedef struct {
    alignas(16)
    _Atomic newshat_record_t contents;
    hatrack_hash_t           hv;
    bool                     migrated;
    pthread_mutex_t          mutex;
} newshat_bucket_t;

typedef struct {
    uint64_t             last_slot;
    uint64_t             threshold;
    uint64_t             used_count;
    newshat_bucket_t     buckets[];
} newshat_store_t;

typedef struct {
    uint64_t             item_count;
    uint64_t             next_epoch;
    newshat_store_t     *store;
    pthread_mutex_t      migrate_mutex;
} newshat_t;

void            newshat_init   (newshat_t *);
void           *newshat_get    (newshat_t *, hatrack_hash_t *, bool *);
void           *newshat_put    (newshat_t *, hatrack_hash_t *, void *, bool *);
void           *newshat_replace(newshat_t *, hatrack_hash_t *, void *, bool *);
bool            newshat_add    (newshat_t *, hatrack_hash_t *, void *);
void           *newshat_remove (newshat_t *, hatrack_hash_t *, bool *);
void            newshat_delete (newshat_t *);
uint64_t        newshat_len    (newshat_t *);
hatrack_view_t *newshat_view   (newshat_t *, uint64_t *, bool);

#endif
