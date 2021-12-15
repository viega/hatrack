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

#define LOWHAT_MIN_SIZE_LOG 3

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

// The dict_history data structure is the top of the list of
// modification records assoiated with a bucket (which will be the
// unordered array when we're using only one array, and the ordered
// array otherwise).
//
// This data structure contains the following:
//
// 1) A copy of the hash value, which we'll need when we grow the
//    table.
//
// 2) A pointer to the top of the record list for the bucket.
//
// 3) The first "write epoch" for purposes of sorting.  While this
//    value lives in the allocation header for lowest-most record
//    after the most recent delete (if any delete is associated with
//    the key, the lowest record, if not), The record it lives in
//    eventually could go away as writes supercede it.
//
//    If a sort begins before a delete and subsequent reinsertion,
//    that's okay too. We'll ignore the write epoch in that case.
//
//    Similarly, it's possible for the write to be committed, but the
//    write epoch value to not have been written yet, in which case
//    we will go calculate it an try to "help" by writing it out.
//
// As for the pointer to the record list, we do NOT care about the ABA
// problem here, so do not need a counter.  In particular, let's say a
// writer is about to insert its new record C into the head, and sees
// record A at the top.  If that thread suffers from a long
// suspension, B might link to A, and then A can get reclaimed.
// Another thread could go to the memory manager, and get the memory
// back, and re-add it to the exact same bucket, all before C wakes
// up.  Yes, the A is not the "same" A we saw before in some sense,
// but we do not care, because our operation is a push, not a pop.
// The item we're pushing correctly points to the next item in the
// list if the CAS succeeds.
//
// Note also that we "push" records onto the record list like a stack,
// but we never really remove items from the list at all. Instead,
// when we can prove that no thread will ever algorithmically descend
// into that record, we can safely reclaim the memory, but we never
// actually bother unlink the items.
//
// Note that when we go to add a new record associated with a bucket,
// we have multiple strategies for handling any CAS failure:
//
// 1) We can continue to retry until we succeed. This should be fine
//    in practice, but in theory, other threads could update the value
//    so frequenty, we could have to try an unbounded number of
//    times. Therefore, this approach is lock free, but not wait free.
//
// 2) We can treat the losing thread as if it were really the
//    "winning" thread... acting as if it has inserted a fraction of a
//    second before the competing thread, but in the exact same
//    epoch. In such a case, no reader could possibly see this value,
//    and so it is safe to forego inserting it into the table. This
//    approach is trivially wait free, since it doesn't loop.
//
// 3) We can use the first approach, but with a bounded number of
//    loops, before switching to the 2nd approach. This is also
//    wait-free.
//
// The second two options open up some minor memory management
// questions.
//
// In this implementation, we go with approach #2, as it's not only
// more efficient to avoid retries, but it's in some sense more
// satisfying to me to move the commit time, in the cases where two
// threads essentially combine, a miniscule time backwards to resolve
// the colision than a potentially large time forward.
//
// Also note that we need to think about memory management here. While
// handle record retirement properly, we should also consider what to
// do with the items in the table. Here are the scenarios:
//
// 1) If there's an explicit call to delete the entry associated with a
//    key.
// 2) If we overwrite the entry with a new entry.
//
// One option for dealing with this scenario is to explicitly return
// items through the API. For instance, if you call delete, you'll get
// back the previous key / value pair. Similarly for a put() operation
// that overwrites another.
//
// A slight problem here is that a single delete can effectively
// remove multiple entries from a bucket, if there's contention on the
// writing. If there are conflicting writes and we decide to silently
// drop one on the floor, per our wait-free strategy above, the
// conceptual "overwrite" won't even have awareness that it's
// overwriting the data.

// A solution here is to have the operation that we're really dropping
// from the table return its own key/value as previous entries that
// may need to be deleted. That has the advantage of giving the
// programmer the opportunity to choose to retry instead of accepting
// the default behavior. However, in practice, people aren't really
// going to care, and they're far more likely to forget to do the
// memory management.
//
// A second solution is to have the user register a memory management
// handler that gets called on any table deletion.
//
// Currently, we're taking the former approach, and expecting a
// wrapper API to handle this, since such a thing is also needed for
// applying the actual hash function.

typedef struct lowhat_history_st lowhat_history_t;

struct lowhat_history_st {
    _Atomic lowhat_hash_t       hv;
    _Atomic(lowhat_record_t *)  head;
    _Atomic(lowhat_history_t *) fwd;
};

// This flag indicates whether the CURRENT record is currently
// considered present or not. Not present can be because it's been
// deleted, or because it hasn't been written yet.  It's implemented
// by stealing a bit from the record's "next" pointer.

enum : uint64_t
{
    LOWHAT_F_USED = 0x0000000000000001
};

// These two flags are used in the migration, and are also implemented
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

// If we're using a second array to improve our sorting costs, then
// the unordered array will have these hash buckets, to point us to
// where the records are. Note that contents of this bucket do not
// indicate whether an item is actually in the hash table or not; it
// only keeps "reservations"... hv being set reserves the bucket for
// the particular hash item, and ptr being set reserves a particular
// location in the other array.

typedef struct {
    _Atomic lowhat_hash_t       hv;
    _Atomic(lowhat_history_t *) ptr;
} lowhat_indirect_t;

// This is used in copying and ordering records.

typedef struct {
    lowhat_hash_t hv;
    void         *item;
    uint64_t      sort_epoch;
} lowhat_view_t;

// lowhat_t is the only data type that should be exposed to the
// end-user; all operations should happen on a lowhat_t, which will
// always delegate to the newest store at the time of the request.
//
// We keep vtables of the operations to make it easier to switch
// between different algorithms for testing.

typedef struct lowhat_st lowhat_t;

// clang-format off
typedef void           (*lowhat_init_func)(lowhat_t *);
typedef void *         (*lowhat_get_func)(lowhat_t *, lowhat_hash_t *, bool *);
typedef void *         (*lowhat_put_func)(lowhat_t *, lowhat_hash_t *,
					  void *, bool, bool *);
typedef void *         (*lowhat_remove_func)(lowhat_t *, lowhat_hash_t *,
					     bool *);
typedef void           (*lowhat_delete_func)(lowhat_t *);
typedef uint64_t       (*lowhat_len_func)(lowhat_t *);
typedef lowhat_view_t *(*lowhat_view_func)(lowhat_t *, uint64_t *);

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

// When the table gets full and we need to migrate, we'll need to keep
// two copies of the hash table at the same time. To that end, we need
// to be able to swap out and eventually delete old copies of the hash
// table.
//
// To that end, we have the "lowhat_t" hashtable type contain a
// "lowhat_store_t", that represents the current table.  When we
// migrate the table, the lowhat_store_t will point to the one we're
// working on migrating to, and when the migration is complete, the
// lowhat_t reference to the current store will be atomically shifted
// to the new table.  At that point, the old table will be "retired",
// meaning it will be freed when there are definitely no more threads
// attempting to operate on the table.
//
// Fields in this table:
//
// last_slot     Indicates the last bucket index for unordered buckets
//               (one less than the total number of buckets). In our
//               implementation, tables are always a power of two in
//               size, because we want to use & whenever we need to
//               calculate our bucket index, instead of the generally
//               much more expensive % operator.
//
// threshold     This is set when the table is created, to 75% of the
//               number of unsorted buckets.  If we're using one array
//               then this is used in the resize determination. If
//               it's a two-array table, it's a bit of wasted space
//               (we do use it on resize, but not often enough where
//               it'd be worth caching normally).  But hey, it's a
//               hash table... space is not a big problem for you!
//
// used_count    This counts how many hash buckets have a hash value
//               stored in them. If we are NOT using two arrays, then
//               We will migrate the table when this reaches 75% of
//               the total number of buckets.  Technically, it's an
//               approximate value, because it's possible to reserve a
//               bucket and then suspend indefinititely (or be killed)
//               right before we update the counter. In practice, that
//               will be incredibly uncommon, and this will good
//               enough.  If we ARE using two arrays, the counter is
//               still kept, but is not used in the resizing logic.
//
//               It is, however, used to help approximate the number
//               of items in the hash table, if requested (we
//               approximate by calculating used_count -
//               del_count). Note that any call to get the length is
//               only an approximation-- the number could be off
//               depending on several variables based on the state of
//               any active writers. If, for some reason, you want a
//               "correct" count, then dump the hash table buckets to
//               an array (intended for iteration), which will
//               linearize the table, and give the exact number of
//               buckets from that epoch.
//
// del_count     This indicates how many buckets have been reserved,
//               but are not currently in use. As with used_count, we
//               update it every time it's appropriate, but it's just
//               an approximation. We use the approximation not just
//               in calculating the length, but to determine, when
//               it's time to migrate, whether we should GROW the
//               table when we migrate, or if we should just migrate
//               to a table of the same size. We will never shrink
//               the table.  Currently, if about 1/2 the overall
//               unordered buckets are empty (or more), we migrate
//               to an identically sized table. Any more full than
//               that, and we double the table size.
//
// ptr_buckets   For tables with two arrays, this points to memory
//               representing the unordered buckets. These buckets
//               don't hold key/value pairs, they just point into
//               the history buckets, which are ordered by the time
//               in which they were reserved.
//
//               For tables with one array, this is wasted space.
//
// hist_buckets  For tables with two arrays, these are the ordered
//               buckets. For one-array tables, they are unordered.
//               In both cases, it contains all our active key/value
//               pairs.
//
// hist_end      For two-array tables, this pointer is used to
//               decide when to migrate the table-- once a bucket
//               reservation would be given this pointer, then we
//               know we've reached our 75% threshold.
//
//               Unused in tables with one array.
//
// hist_next     A pointer to the next reservable bucket.
//               Unused in tables with one array.
//
// store_next    A pointer to the store to which we are currently
//               migrating.
//
// get_func      The implementation for the get operation used by this
//               table.
//
// put_func      The implementation for the put operation used by this
//               table.
//
// remove_func   The implemenration for the remove operation used by
//               this table.

typedef struct lowhat_store_st lowhat_store_t;

struct lowhat_store_st {
    uint64_t                    last_slot;
    uint64_t                    threshold;
    _Atomic uint64_t            used_count;
    _Atomic uint64_t            del_count;
    lowhat_indirect_t          *ptr_buckets;
    lowhat_history_t           *hist_buckets;
    lowhat_history_t           *hist_end;
    _Atomic(lowhat_history_t *) hist_next;
    _Atomic(lowhat_store_t *)   store_next;
};

struct lowhat_st {
    _Atomic(lowhat_store_t *) store_current;
    lowhat_vtable_t           vtable;
};

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

lowhat_t *lowhat_new(uint64_t);

#endif
