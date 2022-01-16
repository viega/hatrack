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
 *  Name:           refhat.h
 *  Description:    A reference hashtable that only works single-threaded.
 *
 *  Author:         John Viega, john@zork.org
 */

#ifndef __REFHAT_H__
#define __REFHAT_H__

#include <hatrack/hatrack_common.h>

// clang-format off

/* refhat_bucket_t
 *
 * For consistency with our other (parallel) implementations, our
 * refhat hash table doesn't move things around the hash table on a
 * deletion.  Instead, it marks buckets as "deleted". If the same key
 * gets reinserted before a table resize, the same bucket will be
 * reused.
 *
 * hv      -- The hash value associated with a bucket, if any.
 *
 * item    -- A pointer to the item being stored in the hash table,
 *            which will generally be a key/value pair in the case
 *            of dictionaries, or just a single value in the case of
 *            sets.
 *
 * deleted -- Set to true if the item has been removed.
 *
 * epoch   -- An indication of insertion time, which we will use to
 *            sort items in the dictionary, when we produce a "view"
 *            (views are intended for iteration or set operations).
 *            The epoch number is chosen relative to other insertions,
 *            and monotonically increases from 1 (see refhat_t below).
 *            If an item is already in the table during a write
 *            operation, where we end up replacing the stored item,
 *            then the value of the epoch field is NOT updated, to
 *            keep insertion order sorting consistent with how Python
 *            does it.
 *
 *            If this value is zero, it indicates there's no item in
 *            the bucket.
 */
typedef struct {
    hatrack_hash_t    hv;
    void             *item;
    uint64_t          epoch;
} refhat_bucket_t;

/* refhat_t
 *
 * The main type for our reference hash table; it contains any
 * information that persists across a table resize operation
 * (everything else lives in the refhat_bucket_t type).
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
 * item_count -- The number of items in the table, NOT counting
 *               deletion entries.
 *
 * buckets    -- The current set of refhat_bucket_t objects.
 *
 * next_epoch -- The next epoch value to give to an insertion
 *               operation, for the purposes of sort ordering.
 */
typedef struct {
    alignas(8)    
    uint64_t          last_slot;
    uint64_t          threshold;
    uint64_t          used_count;
    uint64_t          item_count;
    refhat_bucket_t  *buckets;
    uint64_t          next_epoch;
} refhat_t;



/* This API requires that you deal with hashing the key external to
 * the API.  You might want to cache hash values, use different
 * functions for different data objects, etc.
 *
 * We do require 128-bit hash values, and require that the hash value
 * alone can stand in for object identity. One might, for instance,
 * choose a 3-universal keyed hash function, or if hash values need to
 * be consistent across runs, something fast and practical like XXH3.
 */

void            refhat_init   (refhat_t *);
void           *refhat_get    (refhat_t *, hatrack_hash_t, bool *);
void           *refhat_put    (refhat_t *, hatrack_hash_t, void *, bool *);
void           *refhat_replace(refhat_t *, hatrack_hash_t, void *, bool *);
bool            refhat_add    (refhat_t *, hatrack_hash_t, void *);
void           *refhat_remove (refhat_t *, hatrack_hash_t, bool *);
void            refhat_delete (refhat_t *);
uint64_t        refhat_len    (refhat_t *);
hatrack_view_t *refhat_view   (refhat_t *, uint64_t *, bool);

//clang-format on

#endif
