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
 *  Name:           hatrack_common.h
 *  Description:    Data structures and utility functions used across all
 *                  our default hash tables.
 *
 *  Author:         John Viega, john@zork.org
 *
 */

#ifndef __HATRACK_COMMON_H__
#define __HATRACK_COMMON_H__

#include "mmm.h"

/* hatrack_hash_t
 *
 *The below type represents a hash value.
 *
 * We use 128-bit hash values and a universal hash function to make
 * accidental collisions so improbable, we can use hash equality as a
 * standin for identity, so that we never have to worry about
 * comparing keys.
 */
typedef struct {
    uint64_t w1;
    uint64_t w2;
} hatrack_hash_t;

/* hatrack_view_t
 *
 * All our calls to return a view of a hash table (generally for the
 * purposes of some kind of iteration) return the hatrack_view_t.
 *
 * When requested, views can be sorted by insertion time (to the best
 * of the algorithm's ability... some are better than others). The
 * semantics here is identical to Python dictionaries:
 *
 * 1) If { K : V1 } is in the table, and we write { K : V2 } over it,
 *    the new item in the 'replace' operation keeps the logical
 *    insertion order of the original item.
 *
 * 2) If { K : V1 } is removed, then a new item inserted with the same
 *    key (even { K : V1 }), the time of re-insertion is the new
 *    insertion time for the purposes of sort ordering.
 *
 * Our view operations all go through the hash table, pick out live
 * entries, and stick them in an array of hatrack_view_t objects, and
 * then sort that array (either via qsort, or in some cases an
 * insertion sort).
 *
 * While the end user probably doesn't need the sort epoch, we give it
 * to them anyway, instead of coping just the items post-sort.  We
 * even give it to them if we are NOT sorting.
 */
typedef struct {
    void    *item;
    uint64_t sort_epoch;
} hatrack_view_t;

/* These inline functions are used across all the hatrack
 * implementations.
 */

static inline uint64_t
hatrack_compute_table_threshold(uint64_t size)
{
    /* size - (size >> 2) calculates 75% of size (100% - 25%).
     *
     * When code checks to see if the current store has hit its
     * threshold,, implementations generally are adding to the used
     * count, and do atomic_fetch_add(), which returns the original
     * value.
     *
     * So, when checking if used_count >= 75%, the -1 gets us to 75%,
     * otherwise we're resizing at one item more full than 75%.
     *
     * That in itself is not a big deal, of course. And if there's
     * anywhere that we check to resize where we're not also reserving
     * a bucket, we would resize an item too early, but again, so what
     * and who cares.
     *
     * Frankly, if I didn't want to be able to just say "75%" without
     * an asterisk, I'd just drop the -1.
     */
    return size - (size >> 2) - 1;
}

/*
 * We always perform a migration when the number of buckets used is
 * 75% of the total number of buckets in the current store. But, we
 * reserve buckets in a store, even if those items are deleted, so the
 * number of actual items in a table could be small when the store is
 * getting full (never changing the hash in a store's bucket makes our
 * lives MUCH easier given parallel work being one in the table).
 *
 * This function figures out, when it's time to migrate, what the new
 * size of the table should be. Our metric here is:
 *
 *  1) If the CURRENT table was at least 50% full, we double the new
 *     table size.
 *
 *  2) If the CURRENT table was up to 25% full, we HALF the new table
 *     size.
 *
 *  3) Otherwise, we leave the table size the same.
 *
 *  Also, we never let the table shrink TOO far... which we base on
 *  the preprocessor variable HATRACK_MIN_SIZE.
 */
static inline uint64_t
hatrack_new_size(uint64_t last_bucket, uint64_t size)
{
    uint64_t table_size = last_bucket + 1;

    if (size >= table_size >> 1) {
        return table_size << 1;
    }
    // We will never bother to size back down to the smallest few
    // table sizes.
    if (size <= (HATRACK_MIN_SIZE << 2)) {
        HATRACK_CTR(HATRACK_CTR_STORE_SHRINK);
        return HATRACK_MIN_SIZE << 3;
    }
    if (size <= (table_size >> 2)) {
        HATRACK_CTR(HATRACK_CTR_STORE_SHRINK);
        return table_size >> 1;
    }

    return table_size;
}

static inline bool
hatrack_hashes_eq(hatrack_hash_t *hv1, hatrack_hash_t *hv2)
{
    return (hv1->w1 == hv2->w1) && (hv1->w2 == hv2->w2);
}

/* Since we use 128-bit hash values, we can safely use the null hash
 * value to mean "unreserved" (and even "empty" in our locking
 * tables).
 */
static inline bool
hatrack_bucket_unreserved(hatrack_hash_t *hv)
{
    return !hv->w1 && !hv->w2;
}

/*
 * Calculates the starting bucket that a hash value maps to, given the
 * table size.  This is exactly the hash value modulo the table size.
 *
 * Since our table sizes are a power of two, "x % y" gives the same
 * result as "x & (y-1)", but the later is appreciably faster on most
 * architectures (probably on all architectures, since the processor
 * basically gets to skip an expensive division). And it gets run
 * enough that it could matter a bit.
 *
 * Also, since our table sizes will never get to 2^64 (and since we
 * can use the bitwise AND due to the power of two table size), we
 * only need to look at one of the two 64-bit chunks in the hash
 * (conceptually the one we look at we consider the most significant
 * chunk).
 */
static inline uint64_t
hatrack_bucket_index(hatrack_hash_t *hv, uint64_t last_slot)
{
    return hv->w1 & last_slot;
}

/* These are just basic bitwise operations, but performing them on
 * pointers requires some messy casting.  These inline functions just
 * hide the casting for us, improving code readability.
 */
static inline int64_t
hatrack_pflag_test(void *ptr, uint64_t flags)
{
    return ((int64_t)ptr) & flags;
}

static inline void *
hatrack_pflag_set(void *ptr, uint64_t flags)
{
    return (void *)(((uint64_t)ptr) | flags);
}

static inline void *
hatrack_pflag_clear(void *ptr, uint64_t flags)
{
    return (void *)(((uint64_t)ptr) & ~flags);
}

int hatrack_quicksort_cmp(const void *, const void *);

#endif
