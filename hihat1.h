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
 *  Name:           hihat1.h
 *  Description:    Half-Interesting HAsh Table.
 *                  This is a lock-free hash table, with wait-free
 *                  read operations. This table allows for you to
 *                  recover the approximate order when getting a view,
 *                  but does not guarantee the consistency of that view.
 *
 *  Author:         John Viega, john@zork.org
 *
 */

#ifndef __HIHAT1_H__
#define __HIHAT1_H__

#include "hatrack_common.h"

/* hihat1_record_t
 *
 * hihat records are atomically compare-and-swapped.
 *
 * The item field is the item passed to the hash table, usually
 * a key : value pair or some sort.
 *
 * The top two bits of the info field are used for carrying status
 * information about migrations. The rest is an epoch field, used for
 * sorting. If the epoch portion is zero, then it indicates the item
 * either has not been set, or has been deleted.
 *
 * The high bit signals whether this is a used item or not (HIHAT_F_MOVING).
 * The next big signals whether we're migrating (HIHAT_F_MOVED).
 *
 * With our locking implementations, we have not needed to do any such
 * bit stealing. But now we need for writers to be able to tell
 * atomically, and without locks, when they need to participate in
 * migration, and switch stores.
 *
 * Note that, with views, there are no consistency guarantees the way
 * there are with lohat.
 *
 */
typedef struct {
    void    *item;
    uint64_t info;
} hihat1_record_t;

// The aforementioned flags, along with a bitmask that allows us to
// extract the epoch in the info field, ignoring any migration flags.
enum : uint64_t
{
    HIHAT_F_MOVING   = 0x8000000000000000,
    HIHAT_F_MOVED    = 0x4000000000000000,
    HIHAT_EPOCH_MASK = 0x3fffffffffffffff
};

/* hihat1_bucket_t
 *
 * The representation of a bucket. The hash value and the record can
 * live in separate atomic variables, because the hash value cannot
 * change once it is written, and anything trying to write out a
 * record will first make sure that the correct hash value gets
 * written out.
 *
 * hv       -- The hash value associated with the contents / bucket,
 *             if any.  Note that the all-zero value maps to "bucket
 *             is empty". But, as long as the hash function is
 *             sufficiently random, the hash function doesn't have to
 *             worry about whether or not it produces an all-zero value;
 *             with 128-bit hashes, the odds should be way too low to
 *             ever worry about in practice.
 *
 * record   -- The contents of the bucket, per hihat1_record_t above.
 */
typedef struct {
    _Atomic hatrack_hash_t  hv;
    _Atomic hihat1_record_t record;
} hihat1_bucket_t;

typedef struct hihat1_store_st hihat1_store_t;

/* hihat1_store_t
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
 * item_count -- The number of items in the table, approximately.
 *               This value isn't used in anything critical, just to
 *               return a result when querying the length.
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
 *
 */
// clang-format off
struct hihat1_store_st {
    alignas(8)
    uint64_t                   last_slot;
    uint64_t                   threshold;
    _Atomic uint64_t           used_count;
    _Atomic uint64_t           item_count;
    _Atomic(hihat1_store_t *)  store_next;
    alignas(16)
    hihat1_bucket_t            buckets[];
};

/* hihat1_t
 *
 * The top-level newshat object.
 *
 * store_current -- The current store to use. When we migrate the
 *                  table, this will change at the very end of the
 *                  migration process. Note that some readers might
 *                  still be reading from the old store after the
 *                  migration is completed, so we'll have to be sure
 *                  not to delete it prematurely.
 *
 * next_epoch    -- The next epoch value to give to an insertion
 *                  operation, for the purposes of sort ordering.
 */
typedef struct {
    alignas(8)
    _Atomic(hihat1_store_t *) store_current;
    uint64_t                  next_epoch;
} hihat1_t;


void            hihat1_init    (hihat1_t *);
void           *hihat1_get     (hihat1_t *, hatrack_hash_t *, bool *);
void           *hihat1_put     (hihat1_t *, hatrack_hash_t *, void *, bool *);
void           *hihat1_replace (hihat1_t *, hatrack_hash_t *, void *, bool *);
bool            hihat1_add     (hihat1_t *, hatrack_hash_t *, void *);
void           *hihat1_remove  (hihat1_t *, hatrack_hash_t *, bool *);
void            hihat1_delete  (hihat1_t *);
uint64_t        hihat1_len     (hihat1_t *);
hatrack_view_t *hihat1_view    (hihat1_t *, uint64_t *, bool);

/*
 * Note that hihat1a is almost identical to hihat1. It has the same
 * data representation, and the only difference is the migration
 * function, which experiments with reducing work on migration, by
 * having late writers wait a bit for the migration to finish.  
 *
 * Instead of adding an extra indirection for that second migration
 * function, we just copy all the methods.
 */
void            hihat1a_init   (hihat1_t *);
void           *hihat1a_get    (hihat1_t *, hatrack_hash_t *, bool *);
void           *hihat1a_put    (hihat1_t *, hatrack_hash_t *, void *, bool *);
void           *hihat1a_replace(hihat1_t *, hatrack_hash_t *, void *, bool *);
bool            hihat1a_add    (hihat1_t *, hatrack_hash_t *, void *);
void           *hihat1a_remove (hihat1_t *, hatrack_hash_t *, bool *);
void            hihat1a_delete (hihat1_t *);
uint64_t        hihat1a_len    (hihat1_t *);
hatrack_view_t *hihat1a_view   (hihat1_t *, uint64_t *, bool);

#endif
