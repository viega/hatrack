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
 *  Name:           duncecap.h
 *  Description:    Don't Use: Crappy Educational Code, Albeit Parallel.
 *
 *                  This uses a per-data structure lock that writers hold
 *                  for their entire operation.
 *
 *                  Readers also use the lock, but only for a minimal
 *                  amount of time-- enough time to grab a pointer to
 *                  the current store, and to increment a reference
 *                  count in that store.  The lock does not need to
 *                  be held through the exit.
 *
 *                  On architectures without an atomic 128-bit load
 *                  instruction, bucket access will be implemented
 *                  under the hood using locks, thanks to C-11
 *                  atomics.
 *
 *  Author:         John Viega, john@zork.org
 */

#ifndef __DUNCECAP_H__
#define __DUNCECAP_H__

#include <hatrack/hatrack_common.h>

#include <pthread.h>

// clang-format off

/* duncecap_record_t
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
 * operation.  The data type duncecap_record_t represents the 128-bit
 * value we read atomically (actually, could be 64 bits on platforms
 * w/ 32-bit pointers). Note that, on architectures without a 128-bit
 * atomic load operation, C will transparently use locking to simulate
 * the instruction.
 *
 * item --  The item passed to the hash table, usually a key : value
 *          pair of some sort.
 *
 * epoch -- The "epoch" is an indication of the creation time of the
 *          item, relative to other items. Note that, since this table
 *          does not provide fully consistent views, the epoch is not
 *          quite as accurate as with other table implementations in
 *          the hatrack. In particular, bumps to the duncecap_t data
 *          structure's next_epoch value (see below), are racy, so
 *          multiple data items can definitely get the same epoch
 *          value, meaning we have no linearization point on which to
 *          construct a consistent sort order.
 */
typedef struct {
    void    *item;
    uint64_t epoch;
} duncecap_record_t;

/* duncecap_bucket_t
 *
 * Writers will have a write-lock on the hash table before writing to
 * the items in a bucket, but will still need to update the contents
 * field atomically, due to the possiblity of readers operating in
 * parallel to the write. 
 * 
 * We force alignment to 128-bits, for the sake * of performance of
 * atomic operations.
 *
 * Note that the hash value does not need to be updated atomically,
 * even though it is 128 bits. If the hash value is half-stored,
 * readers will experience a 'miss', which is the correct outcome, as
 * if the hash had not been written at all yet.
 *
 * record -- The contents, per above.
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
    _Atomic duncecap_record_t record;
    hatrack_hash_t            hv;
} duncecap_bucket_t;

/* duncecap_store_t
 *
 * The data type representing our current store.
 *
 * readers   --  The number of readers currently visiting the store. 
 *               This is essentially a reference count that writers 
 *               use to make sure that, when migrating the store, 
 *               they don't free the store when another thread might 
 *               still read from it.
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
    alignas(8)
    _Atomic uint64_t    readers;
    uint64_t            last_slot;
    uint64_t            threshold;
    uint64_t            used_count;
    duncecap_bucket_t   buckets[];
} duncecap_store_t;

/* duncecap_t
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
    duncecap_store_t   *store_current;
    uint64_t            item_count;
    uint64_t            next_epoch;
    pthread_mutex_t     mutex;
} duncecap_t;
// clang-format on

/* duncecap_reader_enter()
 *
 * Used to register readers to the current store, so that we can make
 * sure not to deallocate the store while readers are using it. The
 * basic idea here is that the reader grabs the lock, so that it knows
 * no writer thread is visiting anything in the data structure. While
 * holding the lock, we get a pointer to the current store, and bump
 * up the reader count.  Readers can then forego holding the lock;
 * they finish by decrementing the counter atomically (see
 * duncecap_reader_exit() below).
 *
 * We could use a read-write mutex, but that would unnecessarily block
 * both reads and writes, when a writer comes along.  The store won't
 * be retired until there are definitely no readers, so as long as we
 * can atomically read the pointer to the store and register our
 * reference to the store, we are fine. As an alternative, we could do
 * this without locks using a 128-bit CAS that holds the pointer, or
 * use mmm_alloc() for the stores (which we do in the duncecap2
 * implementation).
 */
static inline duncecap_store_t *
duncecap_reader_enter(duncecap_t *self)
{
    duncecap_store_t *ret;

    pthread_mutex_lock(&self->mutex);
    ret = self->store_current;
    atomic_fetch_add(&ret->readers, 1);
    pthread_mutex_unlock(&self->mutex);

    return ret;
}

/* duncecap_reader_exit()
 *
 * This simply needs to decrement the reader count associated with the
 * store, atomically.
 */
static inline void
duncecap_reader_exit(duncecap_store_t *store)
{
    atomic_fetch_sub(&store->readers, 1);

    return;
}

/* This API requires that you deal with hashing the key external to
 * the API.  You might want to cache hash values, use different
 * functions for different data objects, etc.
 *
 * We do require 128-bit hash values, and require that the hash value
 * alone can stand in for object identity. One might, for instance,
 * choose a 3-universal keyed hash function, or if hash values need to
 * be consistent across runs, something fast and practical like XXH3.
 */
// clang-format off
duncecap_t     *duncecap_new      (void);
duncecap_t     *duncecap_new_size (char);
void            duncecap_init     (duncecap_t *);
void            duncecap_init_size(duncecap_t *, char);
void            duncecap_cleanup  (duncecap_t *);
void            duncecap_delete   (duncecap_t *);
void           *duncecap_get      (duncecap_t *, hatrack_hash_t, bool *);
void           *duncecap_put      (duncecap_t *, hatrack_hash_t, void *,
				   bool *);
void           *duncecap_replace  (duncecap_t *, hatrack_hash_t, void *,
				   bool *);
bool            duncecap_add      (duncecap_t *, hatrack_hash_t, void *);
void           *duncecap_remove   (duncecap_t *, hatrack_hash_t, bool *);
uint64_t        duncecap_len      (duncecap_t *);
hatrack_view_t *duncecap_view     (duncecap_t *, uint64_t *, bool);

#endif
