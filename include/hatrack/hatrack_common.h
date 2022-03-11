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
 */

#ifndef __HATRACK_COMMON_H__
#define __HATRACK_COMMON_H__

#include <hatrack/mmm.h>

/* hatrack_hash_t
 *
 *The below type represents a hash value.
 *
 * We use 128-bit hash values and a universal hash function to make
 * accidental collisions so improbable, we can use hash equality as a
 * standin for identity, so that we never have to worry about
 * comparing keys.
 */

#ifdef HAVE___INT128_T
typedef __int128_t hatrack_hash_t;
#else
typedef struct {
    uint64_t w1;
    uint64_t w2;
} hatrack_hash_t;
#endif

/* hatrack_hash_func_t
 *
 * A generic type for function pointers to functions that hash for
 * us. This is used in the higher-level dict and set implementations.
 *
 * Note that several implementations of hash functions (using XXH3)
 * are in hash.h.
 */
typedef hatrack_hash_t (*hatrack_hash_func_t)(void *);

/* hatrack_mem_hook_t
 *
 * Function pointer to a function of two arguments. This is used for
 * memory management hooks in our higher-level dict and set
 * implementations:
 *
 * 1) A hook that runs to notify when a user-level record has been
 *    ejected from the hash table (either via over-write or
 *    deletion). Here, it's guaranteed that the associated record is
 *    no longer being accessed by other threads. 
 *
 *    However, it says nothing about threads using records read from
 *    the hash table. Therefore, if you're using dynamic records, this
 *    hook should probably be used to decrement a reference count.
 *
 * 2) A hook that runs to notify when a user-level record is about to
 *    be returned.  This is primarily intended for clients to
 *    increment a reference count on objects being returned BEFORE the
 *    table would ever run the previous hook.
 *
 * The first argument will be a pointer to the data structure, and the
 * second argument will be a pointer to the item in question. In
 * dictionaries, if both a key and a value are getting returned
 * (through a call to dict_items() for example), then the callback
 * will get used once for the key, and once for the item.
 *
 * This type happens to have the same signature as a different memory
 * management hook that MMM uses, which is indeed used in implementing
 * these higher level hooks.  However, the meaning of the arguments is
 * different, so we've kept them distinct types.
 */
typedef void (*hatrack_mem_hook_t)(void *, void *);

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
    int64_t  sort_epoch;
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

#ifdef HAVE___INT128_T

static inline bool
hatrack_hashes_eq(hatrack_hash_t hv1, hatrack_hash_t hv2)
{
    return hv1 == hv2;
}

#else

static inline bool
hatrack_hashes_eq(hatrack_hash_t hv1, hatrack_hash_t hv2)
{
    return (hv1.w1 == hv2.w1) && (hv1.w2 == hv2.w2);
}

#endif

#ifdef HAVE___INT128_T
static inline bool
hatrack_hash_gt(hatrack_hash_t hv1, hatrack_hash_t hv2)
{
    return hv1 > hv2;
}

#else

static inline bool
hatrack_hash_gt(hatrack_hash_t hv1, hatrack_hash_t hv2)
{
    if (hv1.w1 > hv2.w1) {
	return true;
    }

    if ((hv1.w1 == hv2.w1) && (hv1.w2 > hv2.w2)) {
	return true;
    }
    
    return false;
}

#endif

/* Since we use 128-bit hash values, we can safely use the null hash
 * value to mean "unreserved" (and even "empty" in our locking
 * tables).
 */

#ifdef HAVE___INT128_T

static inline bool
hatrack_bucket_unreserved(hatrack_hash_t hv)
{
    return !hv;
}

#else
static inline bool
hatrack_bucket_unreserved(hatrack_hash_t hv)
{
    return !hv.w1 && !hv.w2;
}

#endif

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
 * can use the bitwise AND due to the power of two table size), when
 * we don't have a native 128-bit type, we only need to look at one of
 * the two 64-bit chunks in the hash (conceptually the one we look at
 * we consider the most significant chunk).
 */

#ifdef HAVE___INT128_T

static inline uint64_t
hatrack_bucket_index(hatrack_hash_t hv, uint64_t last_slot)
{
    return hv & last_slot;
}

#else

static inline uint64_t
hatrack_bucket_index(hatrack_hash_t hv, uint64_t last_slot)
{
    return hv.w1 & last_slot;
}

#endif

#ifdef HAVE___INT128_T

static inline void
hatrack_bucket_initialize(hatrack_hash_t *hv)
{
    *hv = 0;
}

#else

static inline void
hatrack_bucket_initialize(hatrack_hash_t *hv)
{
    hv->w1 = 0;
    hv->w2 = 0;
}
#endif

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

static inline uint64_t
hatrack_round_up_to_power_of_2(uint64_t n)
{
    // n & (n - 1) only returns 0 when n is a power of two.
    // If n's already a power of 2, we're done.
    if (!(n & (n - 1))) {
	return n;
    }

    // CLZ returns the number of zeros before the leading one.
    // The next power of two will have one fewer leading zero,
    // and that will be the only bit set.
    return 0x8000000000000000 >> (__builtin_clzll(n) - 1);
}

static inline void *
hatrack_found(bool * found, void *ret)
{
    if (found) {
	*found = true;
    }

    return ret;
}

static inline void *
hatrack_not_found(bool *found)
{
    if (found) {
	*found = false;
    }

    return NULL;
}

static inline void *
hatrack_found_w_mmm(bool *found, void *ret)
{
    mmm_end_op();
    return hatrack_found(found, ret);
}

static inline void *
hatrack_not_found_w_mmm(bool *found)
{
    mmm_end_op();
    return hatrack_not_found(found);
}


typedef struct
{
    uint64_t h;
    uint64_t l;
} generic_2x64_t;

typedef union {
    generic_2x64_t      st;
    _Atomic __uint128_t atomic_num;
    __uint128_t         num;
} generic_2x64_u;

static inline generic_2x64_u
hatrack_or2x64(generic_2x64_u *s1, generic_2x64_u s2)
{
    return (generic_2x64_u)atomic_fetch_or(&s1->atomic_num, s2.num);
}

static inline generic_2x64_u
hatrack_or2x64l(generic_2x64_u *s1, uint64_t l)
{
    generic_2x64_u n = { .st = {.h = 0, .l = l } };

    return (generic_2x64_u)atomic_fetch_or(&s1->atomic_num, n.num);
}

static inline generic_2x64_u
hatrack_or2x64h(generic_2x64_u *s1, uint64_t h)
{
    generic_2x64_u n = { .st = { .h = h, .l = 0 } };

    return (generic_2x64_u)atomic_fetch_or(&s1->atomic_num, n.num);
}

#define OR2X64(s1, s2) hatrack_or2x64((generic_2x64_u *)(s1), s2)
#define OR2X64L(s1, s2) hatrack_or2x64l((generic_2x64_u *)(s1), s2)
#define OR2X64H(s1, s2) hatrack_or2x64h((generic_2x64_u *)(s1), s2)
#define ORPTR(s1, s2) atomic_fetch_or((_Atomic uint64_t *)(s1), s2)

#define hatrack_cell_alloc(container_type, cell_type, n)                       \
    (container_type *)calloc(1, sizeof(container_type) + sizeof(cell_type) * n)

int  hatrack_quicksort_cmp(const void *, const void *);
#endif
