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

/* The below type represents a hash value.
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

// This is used in copying and ordering records.

typedef struct {
    hatrack_hash_t hv;
    void          *item;
    uint64_t       sort_epoch;
} hatrack_view_t;

// clang-format on

// These inline functions are used across all the hatrack
// implementations.

static inline uint64_t
hatrack_compute_table_threshold(uint64_t size)
{
    return size - (size >> 2);
}

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

// Since we use 128-bit hash values, we can safely use the null hash
// value to mean "unreserved".
static inline bool
hatrack_bucket_unreserved(hatrack_hash_t *hv)
{
    return !hv->w1 && !hv->w2;
}

static inline uint64_t
hatrack_bucket_index(hatrack_hash_t *hv, uint64_t last_slot)
{
    return hv->w1 & last_slot;
}

// Inline to hide the pointer casting.
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

#ifndef HATRACK_DONT_SORT
int hatrack_quicksort_cmp(const void *, const void *);
#endif

#endif
