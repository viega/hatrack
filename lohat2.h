/*
 * Copyright © 2021 John Viega
 *
 * See LICENSE.txt for licensing info.
 *
 *  Name:           lohat2.h
 *  Description:    Linearizeable, Ordered HAsh Table (LOHAT)
 *                  This version keeps two tables, for partial ordering.
 *  Author:         John Viega, john@zork.org
 *
 */

#ifndef __LOHAT2_H__
#define __LOHAT2_H__

#include "lohat_common.h"

// This API requires that you deal with hashing the key external to
// the API.  You might want to cache hash values, use different
// functions for different data objects, etc.
//
// We do require 128-bit hash values, and require that the hash value
// alone can stand in for object identity. One might, for instance,
// choose a 3-universal keyed hash function, or if hash values need to
// be consistent across runs, something fast and practical like XXH3.

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

typedef struct lohat2_history_st lohat2_history_t;

struct lohat2_history_st {
    alignas(32) _Atomic hatrack_hash_t hv;
    _Atomic(lohat_record_t *)   head;
    _Atomic(lohat2_history_t *) fwd;
};

// We're using a second array to improve our sorting costs. These are
// the buckets that the hash function points to... their contents just
// point us to where the actual records are. Note that contents of
// this bucket do not indicate whether an item is actually in the hash
// table or not; it only keeps "reservations"... hv being set reserves
// the bucket for the particular hash item, and ptr being set reserves
// a particular location in the other array.

typedef struct {
    alignas(32) _Atomic hatrack_hash_t hv;
    _Atomic(lohat2_history_t *) ptr;
} lohat2_indirect_t;

// When the table gets full and we need to migrate, we'll need to keep
// two copies of the hash table at the same time. To that end, we need
// to be able to swap out and eventually delete old copies of the hash
// table.
//
// To that end, we have the "lohat_t" hashtable type contain a
// "lohat_store_t", that represents the current table.  When we
// migrate the table, the lohat_store_t will point to the one we're
// working on migrating to, and when the migration is complete, the
// lohat_t reference to the current store will be atomically shifted
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
//               number of unsorted buckets.
//
// del_count     This indicates how many buckets have been reserved,
//               but are not currently in use. We update it every time
//               it's appropriate, but it's just an approximation. We
//               use the approximation not just in calculating the
//               length, but to determine, when it's time to migrate,
//               whether we should GROW the table when we migrate, or
//               if we should just migrate to a table of the same
//               size. We will never shrink the table.  Currently, if
//               about 1/2 the overall unordered buckets are empty (or
//               more), we migrate to an identically sized table. Any
//               more full than that, and we double the table size.
//
// ptr_buckets   This points to memory representing the unordered
//               buckets. These buckets don't hold key/value pairs,
//               they just point into the history buckets, which are
//               ordered by the time in which they were reserved.
//
// hist_buckets  These are the ordered buckets, containing all of
//               our active key/value pairs.
//
// hist_end      This pointer is used to decide when to migrate the
//               table-- once a bucket reservation would be given
//               this pointer, then we know we've reached our 75%
//               threshold.
//
// hist_next     A pointer to the next reservable bucket.
//
// store_next    A pointer to the store to which we are currently
//               migrating.

typedef struct lohat2_store_st lohat2_store_t;

struct lohat2_store_st {
    alignas(32) uint64_t last_slot;
    uint64_t                    threshold;
    _Atomic uint64_t            del_count;
    lohat2_indirect_t          *ptr_buckets;
    lohat2_history_t           *hist_buckets;
    lohat2_history_t           *hist_end;
    _Atomic(lohat2_history_t *) hist_next;
    _Atomic(lohat2_store_t *)   store_next;
};

typedef struct {
    alignas(32) _Atomic(lohat2_store_t *) store_current;
} lohat2_t;

// clang-format off

void            lohat2_init(lohat2_t *);
void           *lohat2_get(lohat2_t *, hatrack_hash_t *, bool *);
void           *lohat2_put(lohat2_t *, hatrack_hash_t *, void *, bool,
			    bool *);
void           *lohat2_remove(lohat2_t *, hatrack_hash_t *, bool *);
void            lohat2_delete(lohat2_t *);
uint64_t        lohat2_len(lohat2_t *);
hatrack_view_t *lohat2_view(lohat2_t *, uint64_t *);

// clang-format on

#endif
