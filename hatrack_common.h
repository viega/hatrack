/*
 * Copyright © 2021 John Viega
 *
 * See LICENSE.txt for licensing info.
 *
 *  Name:           hatrack_common.h
 *  Description:    Items shared across hash tables.
 *  Author:         John Viega, john@zork.org
 */

#ifndef __HATRACK_COMMON_H__
#define __HATRACK_COMMON_H__

#include "mmm.h"

// Doing the macro this way forces you to pick a power-of-two boundary
// for the hash table size, which is best for alignment, and allows us
// to use an & to calculate bucket indicies, instead of the more
// expensive mod operator.

#ifndef HATRACK_MIN_SIZE_LOG
#define HATRACK_MIN_SIZE_LOG 3
#endif

// The below type represents a hash value.
//
// We use 128-bit hash values and a universal hash function to make
// accidental collisions so improbable, we can use hash equality as a
// standin for identity, so that we never have to worry about
// comparing keys.

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

// By default, we keep vtables of the operations to make it easier to
// switch between different algorithms for testing. These types are
// aliases for the methods that we expect to see.
//
// We use void * in the first parameter to all of these methods to
// stand in for an arbitrary pointer to a hash table.

// clang-format off

typedef void            (*hatrack_init_func)(void *);
typedef void *          (*hatrack_get_func)(void *, hatrack_hash_t *, bool *);
typedef void *          (*hatrack_put_func)(void *, hatrack_hash_t *,
					    void *, bool, bool *);
typedef void *          (*hatrack_remove_func)(void *, hatrack_hash_t *,
					       bool *);
typedef void            (*hatrack_delete_func)(void *);
typedef uint64_t        (*hatrack_len_func)(void *);
typedef hatrack_view_t *(*hatrack_view_func)(void *, uint64_t *);

typedef struct {
    hatrack_init_func   init;
    hatrack_get_func    get;
    hatrack_put_func    put;
    hatrack_remove_func remove;
    hatrack_delete_func delete;
    hatrack_len_func    len;
    hatrack_view_func   view;
} hatrack_vtable_t;

// clang-format on

// These inline functions are used across all the hatrack
// implementations.

static inline uint64_t
hatrack_compute_table_threshold(uint64_t num_slots)
{
    return num_slots - (num_slots >> 2);
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
    return hv->w2 & last_slot;
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
