/*
 * Copyright © 2021-2022 John Viega
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
 */

#ifndef __NEWSHAT_H__
#define __NEWSHAT_H__

#include <hatrack/hatrack_common.h>

#include <pthread.h>

// clang-format off

/* newshat_record_t
 *
 * Each newshat bucket is individually locked, so that only one thread
 * can be writing to the bucket at a time. However, there may be
 * multiple readers in parallel. The things a reader might need should
 * be updated atomically, and are stored in newshat_record_t.
 *
 * item --  The item passed to the hash table, usually a key : value
 *          pair of some sort.
 *
 * epoch -- If the field is 0, then it indicates a deleted item.
 *          Otherwise, it represents the "epoch"-- an indication of
 *          the creation time of the item, relative to other
 *          items. Note that, since this table does not provide fully
 *          consistent views, the epoch is not quite as accurate as
 *          with other table implementations in the hatrack. In
 *          particular, bumps to the newshat_t data structure's
 *          next_epoch value (see below), are racy, so multiple data
 *          items can definitely get the same epoch value, meaning we
 *          have no linearization point on which to construct a
 *          consistent sort order.
 */

typedef struct {
    void                *item;
    uint64_t             epoch;
} newshat_record_t;

/* newshat_bucket_t
 *
 * The representation of a bucket.  Each bucket gets its own write
 * mutex, to support multiple writers. The 'record' field is
 * atomically updated, to support reads that can operate in parallel
 * with any write operation.
 *
 * We force alignment to 128-bits, for the sake of performance of
 * atomic operations, just in case some C compiler somewhere gets it
 * wrong (Clang certainly does not).
 *
 * Note that the hash value does not need to be updated atomically,
 * even though it is 128 bits. If the hash value is half-stored,
 * readers will experience a 'miss', which is the correct outcome, as
 * if the hash had not been written at all yet.
 *
 * record   -- The contents, per newshat_record_t above.
 *
 * hv       -- The hash value associated with the contents / bucket, 
 *             if any.  Note that the all-zero value maps to "bucket 
 *             is empty". But, as long as the hash function is 
 *             sufficiently random, the hash function doesn't have to 
 *             worry about whether or not it produces an all-zero value;
 *             with 128-bit hashes, the odds should be way too low to 
 *             ever worry about in practice.
 *
 * migrated -- An indication for other writers whether the bucket (and
 *             thus the entire store) has been migrated. We need this,
 *             because writers waiting on the write-lock for a bucket
 *             might be waiting through a table migration. If they do,
 *             then they need to go get a reference to the new table.
 *             So, checking this value is their first order of business
 *             after acquiring a bucket's lock.
 * 
 * mutex    -- The lock associated with this bucket. Only writers need
 *             to acquire this lock; readers may read, even when the
 *             lock is being held by a writer.  Note that mutexes
 *             generally take up about 40 bytes of space each, making
 *             this our least memory efficient data structure.
 *
 *             Readers acquiring this mutex means that readers in this
 *             implementation are not lock-free. And if we switched to
 *             the atomic store approach, they would actually be
 *             wait-free.
 */
// clang-format off
typedef struct {
    alignas(16)
    _Atomic newshat_record_t record;
    hatrack_hash_t           hv;
    bool                     migrated;
    pthread_mutex_t          mutex;
} newshat_bucket_t;

/* newshat_store_t
 * 
 * The data type representing our current store. We migrate between stores
 * whenever our table gets too cluttered, at which point we might also
 * resize the store.
 *
 * last_slot  -- The array index of the last bucket, so this will be
 *               one less than the total number of buckets. We store
 *               it this way, because we're going to use this value
 *               far more frequently than the total number.
 *
 * threshold  -- We use a simple metric to decide when we need to
 *               migrate the hash table buckets to a different set of
 *               buckets-- when an insertion would lead to 75% of the
 *               buckets in the current table being used.  This field
 *               olds 75% of the total table size.  Note that, when
 *               we actually migrate the buckets, the allocated size
 *               could grow, shrink or stay the same, depending on
 *               how many removed items are cluttering up the table.
 *
 * used_count -- Indicates how many buckets in the table have a hash
 *               value associated with it. This includes both items
 *               currently in the table and buckets that are reserved,
 *               because they have a hash value associated with them,
 *               but the item has been removed since the last
 *               resizing.
 *
 * buckets    -- The actual bucket objects associated with this store. 
 *               Note that we use a variable-sized array here, and 
 *               dynamically allocate the store to the correct size, 
 *               so that we can avoid an extra indirection.
 */
typedef struct {
    uint64_t             last_slot;
    uint64_t             threshold;
    uint64_t             used_count;
    newshat_bucket_t     buckets[];
} newshat_store_t;

/* newshat_t
 *
 * The top-level newshat object.
 *
 * item_count    -- The number of items in the table, approximately.
 *                  This value isn't used in anything critical, just
 *                  to return a result when querying the length.
 *
 * store_current -- The current store to use. When we migrate the
 *                  table, this will change at the very end of the
 *                  migration process. Note that some readers might
 *                  still be reading from the old store after the
 *                  migration is completed, so we'll have to be sure
 *                  not to delete it prematurely.
 *
 * migrate_mutex -- This algorithm uses per-bucket locking to control
 *                  write access to buckets, but when it comes time to
 *                  migrate a table, we also require threads to
 *                  acquire the migrate mutex, so that we can limit to
 *                  one thread migrating.
 *
 *                  Threads that don't get the honor of migrating wait
 *                  on this mutex anyway, and start back up when they
 *                  get the lock, seeing that the store migrated
 *                  before they got the lock.
 *
 * next_epoch    -- The next epoch value to give to an insertion
 *                  operation, for the purposes of sort ordering.
 */
typedef struct {
    newshat_store_t     *store_current;
    uint64_t             item_count;
    uint64_t             next_epoch;
    pthread_mutex_t      migrate_mutex;
} newshat_t;

/* This API requires that you deal with hashing the key external to
 * the API.  You might want to cache hash values, use different
 * functions for different data objects, etc.
 *
 * We do require 128-bit hash values, and require that the hash value
 * alone can stand in for object identity. One might, for instance,
 * choose a 3-universal keyed hash function, or if hash values need to
 * be consistent across runs, something fast and practical like XXH3.
 */
newshat_t      *newshat_new      (void);
newshat_t      *newshat_new_size (char);
void            newshat_init     (newshat_t *);
void            newshat_init_size(newshat_t *);
void            newshat_cleanup  (newshat_t *);
void            newshat_delete   (newshat_t *);
void           *newshat_get      (newshat_t *, hatrack_hash_t, bool *);
void           *newshat_put      (newshat_t *, hatrack_hash_t, void *, bool *);
void           *newshat_replace  (newshat_t *, hatrack_hash_t, void *, bool *);
bool            newshat_add      (newshat_t *, hatrack_hash_t, void *);
void           *newshat_remove   (newshat_t *, hatrack_hash_t, bool *);
uint64_t        newshat_len      (newshat_t *);
hatrack_view_t *newshat_view     (newshat_t *, uint64_t *, bool);

#endif
