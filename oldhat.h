/*
 * Copyright © 2022 John Viega
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
 *  Name:           oldhat.c
 *  Description:    Old, Legacy, Dated Hardware-Acceptable Table
 *
 *                  This table stays away from 128-bit compare-and
 *                  swap operations.  It does so by keeping all bucket
 *                  information in a single structure, and only ever
 *                  CASing a pointer to said structure.
 *
 *                  The net result is we require a lot of dynamic
 *                  memory allocation.
 *
 *  Author:         John Viega, john@zork.org
 *
 */

#ifndef __OLDHAT_H__
#define __OLDHAT_H__

#include "hatrack_common.h"

/* oldhat_record_t
 *
 * This is the representation of a bucket in oldhat. None of the
 * individual elements need to worry about atomicity; the entire
 * record is updated at once (by swapping the pointer to the record,
 * which is how we limit ourselves to a 64-bit CAS operation.
 *
 * The individual "hash buckets" only hold a pointer to a record of
 * this type; those bucket entries start out as the NULL pointer, and
 * as we mutate the table, we swap in oldhat_record_t objects (taking
 * care to properly dispose of swapped out objects when we're sure
 * that no thread has a reference to those objects).
 *
 * If we insert something into the table and then delete it, we do NOT
 * go back to a null pointer sitting in the bucket. Instead, we point
 * to a record that will indicate the bucket is empty... but reserved
 * for future re-insertions for items with the same hash value. These
 * records DO get cleared out if there's no insertion by the time we
 * begin migrating a new set of buckets (usually due to table
 * expansion, but sometimes to clean up if we have a lot of deleted
 * entries).
 *
 * We use several boolean fields in this structure that could easily
 * be moved into a bitmask, and could even steal those bits from the
 * hash value quite safely, if so desired.
 *
 *
 * hv       -- The hash value associated with the contents / bucket,
 *             if any.  Note that, in this implementation, unlike all our
 *             others, the all-zero value does not need to be an
 *             indiciation that he bucket is empty.  We have the "used"
 *             flag for that. Not that it matters if you select a good
 *             hash function!
 *
 * item     -- The item passed to the hash table, usually a key : value
 *             pair of some sort.
 *
 * moving   -- We set this to true to indicate to writers that they
 *             need to help us migrate the table.
 *
 * moved    -- We set this to true to indicate to other threads helping
 *             to migrate the table that the bucket in question is
 *             fully migrated.
 *
 * used     -- We set this to true when there is a value present.
 *
 */
// clang-format off
typedef struct {
    hatrack_hash_t hv;
    void          *item;
    bool           moving;
    bool           moved;
    bool           used;
} oldhat_record_t;

typedef struct oldhat_store_st oldhat_store_t;


/* oldhat_store_t
 * 
 * The data type representing our current store.  When we need to
 * resize or clean out our table, the top-level oldhat_t object will
 * stay the same; we instead replace the internal storage (we call
 * this migrating the table).
 *
 * All of our tables use the same metrics for when to perform a table
 * migration. We do it when approximately 3/4 of the total number of
 * buckets have a RECORD in them, even if that record corresponds to
 * an item that was deleted.
 *
 * We then use a different metric to figure out how big to make the
 * next store-- if about 25% of the current buckets (or fewer) have an
 * item in it, we will shrink the table size by 50%.  If about 50% of
 * the current buckets (or more) have an item in it, we will double
 * the table size.  Otherwise, we will use the same size, and just 
 * clear out the dead entries, to make room for more inserts.
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
 * store_next -- When writer threads realize it's time to migrate,
 *               they will try to create the next store, if it hasn't
 *               been put here by the time they read it. Once they
 *               find the agreed upon store, they all race to migrate.
 *               Only writers care about this variable, and only during
 *               migration.
 *
 * buckets    -- The actual bucket objects associated with this store.
 *               Note that we use a variable-sized array here, and
 *               dynamically allocate the store to the correct size,
 *               so that we can avoid an extra indirection.
 */
// clang-format off
struct oldhat_store_st {
    alignas(8)
    uint64_t                   last_slot;
    uint64_t                   threshold;
    _Atomic uint64_t           used_count;
    _Atomic(oldhat_store_t *)  store_next;
    _Atomic(oldhat_record_t *) buckets[];
};

/* oldhat_t
 *
 * The top-level oldhat object.
 *
 * store_current -- The current store to use. When we migrate the
 *                  table, this will change at the very end of the
 *                  migration process. Note that some readers might
 *                  still be reading from the old store after the
 *                  migration is completed, so we'll have to be sure
 *                  not to delete it prematurely.
 *
 * item_count    -- The number of items in the table, approximately.
 *                  This value isn't used in anything critical, just
 *                  to return a result when querying the length.
 */
// clang-format off
typedef struct {
    alignas(8)
    _Atomic(oldhat_store_t *) store_current;
    _Atomic uint64_t          item_count;
} oldhat_t;

/* This API requires that you deal with hashing the key external to
 * the API.  You might want to cache hash values, use different
 * functions for different data objects, etc.
 *
 * We do require 128-bit hash values, and require that the hash value
 * alone can stand in for object identity. One might, for instance,
 * choose a 3-universal keyed hash function, or if hash values need to
 * be consistent across runs, something fast and practical like XXH3.
 */
void            oldhat_init   (oldhat_t *);
void           *oldhat_get    (oldhat_t *, hatrack_hash_t, bool *);
void           *oldhat_put    (oldhat_t *, hatrack_hash_t, void *, bool *);
void           *oldhat_replace(oldhat_t *, hatrack_hash_t, void *, bool *);
bool            oldhat_add    (oldhat_t *, hatrack_hash_t, void *);
void           *oldhat_remove (oldhat_t *, hatrack_hash_t, bool *);
void            oldhat_delete (oldhat_t *);
uint64_t        oldhat_len    (oldhat_t *);
hatrack_view_t *oldhat_view   (oldhat_t *, uint64_t *, bool);

#endif
