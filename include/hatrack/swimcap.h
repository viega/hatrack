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
 *  Name:           swimcap.h
 *  Description:    Single Writer, Independent Multiple-readers.
 *                  Crappy, Albeit Parallel.
 *
 *                  This uses a per-data structure lock that writers hold
 *                  for their entire operation.
 *
 *                  It is based on duncecap, but in this version,
 *                  readers do NOT use the lock; in fact, they are
 *                  fully wait free.
 *
 *                  Instead, we use an epoch-based memory management
 *                  scheme on our current data store, to make sure that
 *                  a store cannot be deleted while we are reading it,
 *                  even if a resize has completed.
 *
 *  Author:         John Viega, john@zork.org
 */

#ifndef __SWIMCAP_H__
#define __SWIMCAP_H__

#include <hatrack/hatrack_common.h>

#include <pthread.h>

// clang-format off

/* swimcap_record_t
 *
 * This is unchanged from duncecap_record_t.
 *
 * In this implementation, readers only use a mutex long enough to
 * register as a reader (to make sure no writer deletes the store
 * they're reading from), and to make sure they're getting a reference
 * to the current 'store'.

 * In hatrack, a store is a set of buckets the table is using-- when
 * we talk about migrating the table during a resize, we migrate from
 * store to store.
 *
 * Since we allow readers and writers to run in parallel, we need to
 * make sure that readers get a consistent view of the
 * data. Particularly, we need to be able to read both the existing
 * item (if there is one), and meta-data about that item atomically.
 *
 * We could use the lock to do that, but instead we use an atomic read
 * operation.  The data type swimcap_record_t represents the 128-bit
 * value we read atomically (actually, could be 64 bits on platforms
 * w/ 32-bit pointers). Note that, on architectures without a 128-bit
 * atomic load operation, C will transparently use locking to simulate
 * the instruction.
 *
 * item --  The item passed to the hash table, usually a key : value
 *          pair of some sort.
 *
 * epoch -- If the field is 0, then it indicates a deleted item.
 *          Otherwise, it represents the "epoch"-- an indication of the
 *          creation time of the item, relative to other items. Note
 *          that, since this table does not provide fully consistent
 *          views, the epoch is not quite as accurate as with other
 *          table implementations in the hatrack. In particular, bumps
 *          to the swimcap_t data structure's next_epoch value (see
 *          below), are racy, so multiple data items can definitely get
 *          the same epoch value, meaning we have no linearization
 *          point on which to construct a consistent sort order.
 */
typedef struct {
    void    *item;
    uint64_t epoch;
} swimcap_record_t;

/* swimcap_bucket_t
 *
 * This is also unchanged from duncecap_bucket_t.
 *
 * Writers will have a write-lock on the hash table before writing to
 * the items in a bucket, but will still need to update the 'record'
 * field atomically, due to the possiblity of readers operating in
 * parallel to the write. 
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
 * record -- The contents, per swimcap_record_t above.
 *
 * hv     -- The hash value associated with the contents / bucket, if
 *           any.  Note that the all-zero value maps to "bucket is
 *           empty". But, as long as the hash function is sufficiently
 *           random, the hash function doesn't have to worry about
 *           whether or not it produces an all-zero value; with
 *           128-bit hashes, the odds should be way too low to ever
 *           worry about in practice.
 */
typedef struct {
    alignas(16)
    _Atomic swimcap_record_t record;
    hatrack_hash_t           hv;
} swimcap_bucket_t;

/* swimcap_store_t
 *
 * The data type representing our current store. This is similar to
 * swimcap_store_t, except that we remove the 'readers' field, because
 * we use an alternative approach to ensuring that writers don't
 * delete stores that a reader might be using (see the swimcap.c
 * source).
 *
 * last_slot --  The array index of the last bucket, so this will be
 *               one less than the total number of buckets. We store
 *               it this way, because we're going to use this value
 *               far more frequently than the total number.
 *
 * threshold --  We use a simple metric to decide when we need to
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
    uint64_t         last_slot;
    uint64_t         threshold;
    uint64_t         used_count;
    swimcap_bucket_t buckets[];
} swimcap_store_t;

/* swimcap_t
 *
 * This is the same as with duncecap_t
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
 * mutex         -- This algorithm uses a single mutex, which writers
 *                  hold for the entire operation, and readers only 
 *                  hold briefly at the start of their operation.
 *
 *                  Writers do not interfere with each other, and
 *                  readers use the lock to make sure that they do not
 *                  end up referencing a store right before it gets
 *                  deleted.
 *
 *                  We could further minimize contention by splitting
 *                  this into two mutexes, one just for writing, and
 *                  one for readers, which writers would only need to
 *                  acquire at the END of their operation, if and only
 *                  if they resize the store (they'd wait for the
 *                  readers to drain before updating the value of the
 *                  store_current variable).
 *
 *                  However, we will reduce contention far more when
 *                  we move to allowing multiple simultaneous
 *                  writers. When we do that, we will allow readers
 *                  during resize, without using a lock.
 *
 * next_epoch    -- The next epoch value to give to an insertion
 *                  operation, for the purposes of sort ordering.
 */
typedef struct {
    swimcap_store_t   *store_current;
    uint64_t           item_count;
    uint64_t           next_epoch;
    pthread_mutex_t    write_mutex;
} swimcap_t;

/* This API requires that you deal with hashing the key external to
 * the API.  You might want to cache hash values, use different
 * functions for different data objects, etc.
 *
 * We do require 128-bit hash values, and require that the hash value
 * alone can stand in for object identity. One might, for instance,
 * choose a 3-universal keyed hash function, or if hash values need to
 * be consistent across runs, something fast and practical like XXH3.
 */
swimcap_t      *swimcap_new    (void);
void            swimcap_init   (swimcap_t *);
void            swimcap_cleanup(swimcap_t *);
void            swimcap_delete (swimcap_t *);
void           *swimcap_get    (swimcap_t *, hatrack_hash_t, bool *);
void           *swimcap_put    (swimcap_t *, hatrack_hash_t, void *, bool *);
void           *swimcap_replace(swimcap_t *, hatrack_hash_t, void *, bool *);
bool            swimcap_add    (swimcap_t *, hatrack_hash_t, void *);
void           *swimcap_remove (swimcap_t *, hatrack_hash_t, bool *);
uint64_t        swimcap_len    (swimcap_t *);
hatrack_view_t *swimcap_view   (swimcap_t *, uint64_t *, bool);

#endif
