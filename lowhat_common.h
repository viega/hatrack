/*
 * Copyright © 2021 John Viega
 *
 * See LICENSE.txt for licensing info.
 *
 *  Name:           lowhat_common.h
 *  Description:    Items shared between the three lowhat versions.
 *  Author:         John Viega, john@zork.org
 */

// We use an "epoch" counter that is incremented with every write
// committment, that gives us an insertion order that we can sort on,
// when proper ordering is desired.  We can also use a second array to
// store key/value pairs and index into it from the unordered array.
// When we do that, there will be a natural ordering, but it will be
// the order in which buckets are "reserved" for writing, and not
// (necessarily) the order in which writes were COMMITTED.  Generally,
// the write order is more desirable than the committment order, but,
// if we choose to, we can leverage the committment order to speed up
// calculating the write order. Here are some options:

// 1) We can just have typical unordered buckets, and only sort the
//    contents at a linearization point, when required (i.e., by the
//    unique "epoch" associated with the write at the time the write
//    was committed). The list is not really being kept in any natual
//    "order" per se, but the insertion order is retained, and thus we
//    can recover it, even though the expected computational
//    complexity of an appropriate sort would be about O(n log n).
//
// 2) We can just care about the bucket reservation order, and call it
//    "close enough".
//
//    However, in this scenario, the bucket reservation ordering
//    drifts from the write committment ordering in multiple
//    situations:
//
//      a) If two writers are racing, and the writer who reserves
//         the first ordered bucket is the last to commit their
//         write.
//      b) If we DELETE an key/value pair from the table, then use
//         the same key for a re-insertion, the second insertion will
//         be in the same place in the table as the ORIGINAL item
//         (unless the table expanded after deletion, but before
//          re-insertion).
//
// 3) We can ignore the write committment order, as with option 2, but
//    keep the reservation order closer to it by requiring REINSERT
//    operations to get a new bucket reservation, to make the
//    reservation ordering more intuitive (and, in most cases with low
//    write contention, correct).  In such a case, we can co-opt space
//    from the deletion record to point to the location of a new
//    reservation, IF ANY.
//
// 4) We can use approach 2 for storage, and then, when ordering is
//    important, sort the buckets by write committment / epoch. This
//    will use up much more space than option #1, and slow down writes
//    a little bit, but will drive down the expected time of sort
//    operations, when ordering is needed.
//
//    The actual complexity will be impacted by the number of deletes,
//    but if the number is low, the complexity will approach O(n), the
//    same hit we have to pay to copy anyway.
//
//    Note that ordering is important mainly when iterating over
//    dictionaries, at which point we will copy out the contents of a
//    linearized point, and sort that. We will never do an in-place
//    sort; it will always involve copying.
//
// 5) We can use approach 3) for storage, and then sort when needed,
//    as with #4. Here, the computational complexity of the sort will
//    generally be VERY close to O(n), but if there are a lot of
//    deletes, we will run out of buckets and need to migrate the
//    table much earlier (and more often) than we would have if we
//    weren't requiring new bucket reservations for re-inserts.
//
// We implement all these options, and do some benchmarking of the
// tradeoffs. The programmer just needs to specify at allocation time:
//
// 1) Whether to use bucket reservations.
// 2) If so, whether to require re-inserts to get new reservations.

#ifndef __LOWHAT_COMMON_H__
#define __LOWHAT_COMMON_H__

#include "mmm.h"

// Doing the macro this way forces you to pick a power-of-two boundary
// for the hash table size, which is best for alignment, and allows us
// to use an & to calculate bucket indicies, instead of the more
// expensive mod operator.

#ifndef LOWHAT_MIN_SIZE_LOG
#define LOWHAT_MIN_SIZE_LOG 3
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
} lowhat_hash_t;

typedef struct lowhat_record_st lowhat_record_t;

// Our buckets must keep a "history" that consists of pending commits
// and actual commits that might still be read by a current reader.
// Older commits will be cleaned up automatically, based on epoch data
// hidden in the allocation header. Specifically, the hidden header
// has two fields, one for the commit epoch, and one for the retire
// epoch. When a newer record comes in on top of us, once the newer
// record is committed (meaning, its commit epoch is set), then it will
// change our "retire epoch" to the same value as its commit epoch,
// which we then use to ensure that the record does not have its
// memory reclaimed until all reads that started before its retirement
// epoch have finished their reads.

// The non-hidden fields are more or less what you'd expect to
// see... a pointer to the next record associated with the bucket, and
// a pointer the key/value pair (opaquely called item here-- if we are
// using this implementation for sets, the data item might not have a
// value at all.
//
// Note that we will, at table migration time, need to steal the least
// significant two bits of the item pointer to assist with the
// migration. This is discussed in a bit more detail below.

struct lowhat_record_st {
    lowhat_record_t *next;
    void            *item;
};

// This flag indicates whether the CURRENT record is currently
// considered present or not. Not present can be because it's been
// deleted, or because it hasn't been written yet.  It's implemented
// by stealing a bit from the record's "next" pointer.

enum : uint64_t
{
    LOWHAT_F_USED = 0x0000000000000001
};

// These two flags are used in table migration, and are also implemented
// by stealing bits from pointers. In this case, we steal two bits
// from the head pointer to the first record.
//
// When the table is large enough that a resize is warranted, we pause
// all writes as quickly as possible, by setting the LOWHAT_F_MOVING
// flag in each history bucket. This tells new writers to help migrate
// the table before finishing their write, even if they are not adding
// a new key (i.e., if they only had a modify operation to do).  The
// LOWHAT_F_MOVED flag is used during the migration to tell other
// threads they don't need to bother trying to migrate a bucket, as
// the migration is already done.
//
// LOWHAT_F_MOVED is not strictly necessary. We can let each thread
// that aids in the migration determine on its own that the migration
// of a bucket was successful. However, we use it to avoid unnecessary
// cycles.
//
// Readers can safely ignore either of these flags. Even a late
// arriving reader can ignore them-- let's say a reader shows up,
// reserving an epoch when a table migration is nearly complete, and
// writes are about to start up again. Any writes to the new table
// will always be uninteresting to this reader, because they
// necessarily will have a later epoch than the reader cares
// about... even if the reader gets suspended.

enum : uint64_t
{
    LOWHAT_F_MOVING = 0x0000000000000001,
    LOWHAT_F_MOVED  = 0x0000000000000002
};

// This is used in copying and ordering records.

typedef struct {
    lowhat_hash_t hv;
    void         *item;
    uint64_t      sort_epoch;
} lowhat_view_t;

// By default, we keep vtables of the operations to make it easier to
// switch between different algorithms for testing. These types are
// aliases for the methods that we expect to see.
//
// We use void * in the first parameter to all of these methods to
// stand in for an arbitrary pointer to a hash table.

// clang-format off
typedef void           (*lowhat_init_func)(void *);
typedef void *         (*lowhat_get_func)(void *, lowhat_hash_t *, bool *);
typedef void *         (*lowhat_put_func)(void *, lowhat_hash_t *,
					  void *, bool, bool *);
typedef void *         (*lowhat_remove_func)(void *, lowhat_hash_t *,
					     bool *);
typedef void           (*lowhat_delete_func)(void *);
typedef uint64_t       (*lowhat_len_func)(void *);
typedef lowhat_view_t *(*lowhat_view_func)(void *, uint64_t *);

typedef struct {
    lowhat_init_func   init;
    lowhat_get_func    get;
    lowhat_put_func    put;
    lowhat_remove_func remove;
    lowhat_delete_func delete;
    lowhat_len_func    len;
    lowhat_view_func   view;
} lowhat_vtable_t;

// clang-format on

// These inline functions are used across all the lowhat
// implementations.

static inline uint64_t
lowhat_compute_table_threshold(uint64_t num_slots)
{
    return num_slots - (num_slots >> 2);
}

static inline bool
lowhat_hashes_eq(lowhat_hash_t *hv1, lowhat_hash_t *hv2)
{
    return (hv1->w1 == hv2->w1) && (hv1->w2 == hv2->w2);
}

// Since we use 128-bit hash values, we can safely use the null hash
// value to mean "unreserved".
static inline bool
lowhat_bucket_unreserved(lowhat_hash_t *hv)
{
    return !hv->w1 && !hv->w2;
}

static inline uint64_t
lowhat_bucket_index(lowhat_hash_t *hv, uint64_t last_slot)
{
    return hv->w2 & last_slot;
}

// Inline to hide the pointer casting.
static inline int64_t
lowhat_pflag_test(void *ptr, uint64_t flags)
{
    return ((int64_t)ptr) & flags;
}

static inline void *
lowhat_pflag_set(void *ptr, uint64_t flags)
{
    return (void *)(((uint64_t)ptr) | flags);
}

static inline void *
lowhat_pflag_clear(void *ptr, uint64_t flags)
{
    return (void *)(((uint64_t)ptr) & ~flags);
}

#endif
