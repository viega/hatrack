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
 *  Name:           lohat_a.h
 *  Description:    Linearizeable, Ordered HAsh Table (LOHAT), Variant A
 *
 *                  This version keeps two tables, for partial ordering.
 *
 *                  It's derived from lohat: The main difference is
 *                  that the top-level buckets don't store the actual
 *                  items. They instead store only pointers into a
 *                  "history array".
 *
 *                  The history array is the same size as the main
 *                  array, but the slots are given out in-order, based
 *                  on the time a thread requests one. The hope is
 *                  that, by having two tiers of buckets, we can get
 *                  the inserted items living in a near-sorted order,
 *                  when they're in the second array.
 *
 *                  The reason that could be interesting is to improve
 *                  times when sorting based on insertion order. It's
 *                  well known that arbitrary sorting problems have an
 *                  n log n lower bound, which can start to become a
 *                  problem when we get very large arrays.
 *
 *                  For instance, if we have an array of 1,000,000
 *                  items, an O(n) operation that costs c units of cpu
 *                  per item will cost c * 1,000,000 units of work. An
 *                  n log n algorithm is likely to cost closer to c *
 *                  13,815,511 work units.
 *
 *                  Often this cost won't matter in practice,
 *                  especially for small values. But the cost
 *                  obviously rises at a greater-than-linear rate
 *                  (even though it's well less than O(n^2) time).
 *
 *                  But, constrained sorting problems, can have better
 *                  bounds. Particularly, with mostly sorted arrays,
 *                  intertion sort's performance approaches O(n).
 *
 *                  Back to lohat-a. If there are no parallel writes,
 *                  it will definitely assign history buckets in write
 *                  order.  When there ARE parallel writes, the items
 *                  hopefully would appear in an order close to the
 *                  one in which they were committed. So, if we trade
 *                  off space, and use a sort algorithm that does well
 *                  on nearly sorted data, perhaps we can come closer
 *                  to O(n) time for our ordering operations.
 *
 *                  Note, however, that there has to be one source of
 *                  truth for what item is in the table, for a given
 *                  key. The indirection without locks makes that
 *                  challenging, if both the pointer to the history
 *                  bucket and the contents in the history bucket can
 *                  both change.
 *
 *                  The easiest tact to take that performs well is the
 *                  same one we use in all the other tables for the
 *                  top-level buckets; once a pointer to the history
 *                  array is installed, we never change it. The
 *                  problem with that is, if an item gets removed from
 *                  the table and re-inserted, then the place it sits
 *                  in the history array is close to the ORIGINAL
 *                  insertion time, not the re-insertion time (all
 *                  assuming no migrations).
 *
 *                  Still, this should be much more sorted than a
 *                  random hash, which means we should be able to get
 *                  sorting times down, without too significant a
 *                  penalty on other operations, relative to lohat.
 *
 *                  We could keep items in the history bucket a bit
 *                  more sorted if we always insert new items into
 *                  a recent history bucket, never re-inserting into
 *                  the old history bucket.
 *
 *                  I experimented with that in the lohat-b variant,
 *                  and sorting times can indeed get very
 *                  good. However, it's at the expense of the number
 *                  of table migrations going up dramatically in cases
 *                  with a reasonable number of deletions. That makes
 *                  it not-so-great for general-purpose use, and I've
 *                  currently removed lohat-b from this project
 *                  (though I probably will add it back in later; I
 *                  just haven't kept it consistent with my other
 *                  implementations, as I realized quickly its value
 *                  was limited).
 *
 *                  Note that I built this based off of lohat, since
 *                  lohat has a clear table-wide linarization, but
 *                  there's ultimately no reason why you couldn't do
 *                  the same thing for hihat, if we're willing to
 *                  accept a bit less accuracy.
 *
 *                  Still, for most practical applications, sorts
 *                  aren't going to be incredibly frequent, so it
 *                  seems much more practical to just pay the n log n
 *                  cost to sort, if (and only if) it's required.
 *
 *                  The comments below assume you've already got a
 *                  decent understanding of the lohat algorithm; we
 *                  skip basic exposition.
 *
 *
 *  Author:         John Viega, john@zork.org
 *
 */

#ifndef __LOHATa_H__
#define __LOHATa_H__

#include "lohat_common.h"

/* lohat_a_history_t
 *
 * The dict_history data structure is the top of the list of
 * modification records assoiated with a bucket (which will be the
 * unordered array when we're using only one array, and the ordered
 * array otherwise).
 *
 * This data structure contains the following:
 *
 * hv   -- A copy of the hash value, which ALSO will be stored in the
 *         top-level of buckets. It needs to live in the top-level
 *         bucket to map hash values to buckets. And it also helps to
 *         have it here, for when we need to migrate stores. This way,
 *         we can just iterate through the history array linearly, and
 *         re-insert into the new store.
 *
 * head -- A pointer to the top of the record list for the bucket.
 *
 */

// clang-format off

typedef struct {
    alignas(16)
    _Atomic hatrack_hash_t    hv;
    _Atomic(lohat_record_t *) head;
} lohat_a_history_t;

/* lohat_a_indirect_t
 *
 * This is the type for the buckets that the hash function points us
 * to... their contents just point us to where the actual records are
 * in the (somewhat ordered) history array. Note that contents of this
 * bucket do not indicate whether an item is actually in the hash
 * table or not; it only keeps "reservations"... hv being set reserves
 * the bucket for the particular hash item, and ptr being set reserves
 * a particular location in the other array.
 */
typedef struct {
    alignas(16)
    _Atomic hatrack_hash_t       hv;
    _Atomic(lohat_a_history_t *) ptr;
} lohat_a_indirect_t;

/* lohat_a_store_t 
 *
 * last_slot     Indicates the last bucket index for unordered buckets
 *               (one less than the total number of buckets). In our
 *               implementation, tables are always a power of two in
 *               size, because we want to use & whenever we need to
 *               calculate our bucket index, instead of the generally
 *               much more expensive % operator.
 *
 * del_count     This indicates how many buckets have been reserved,
 *               but are not currently in use. We update it every time
 *               it's appropriate, but it's just an approximation. We
 *               use the approximation not just in calculating the
 *               length, but to determine, when it's time to migrate,
 *               whether we should GROW the table when we migrate, or
 *               if we should just migrate to a table of the same
 *               size. We will never shrink the table.  Currently, if
 *               about 1/2 the overall unordered buckets are empty (or
 *               more), we migrate to an identically sized table. Any
 *               more full than that, and we double the table size.
 *
 * ptr_buckets   This points to memory representing the unordered
 *               buckets. These buckets don't hold key/value pairs,
 *               they just point into the history buckets, which are
 *               ordered by the time in which they were reserved.
 *
 * hist_buckets  These are the ordered buckets, containing all of
 *               our active key/value pairs.
 *
 * hist_end      This pointer is used to decide when to migrate the
 *               table-- once a bucket reservation would be given
 *               this pointer, then we know we've reached our 75%
 *               threshold, and need to migrate the table.
 *
 * hist_next     A pointer to the next reservable bucket.
 *
 * store_next    A pointer to the store to which we are currently
 *               migrating.
 */
typedef struct lohat_a_store_st lohat_a_store_t;

// clang-format off
struct lohat_a_store_st {
    alignas(8)
    uint64_t                     last_slot;
    _Atomic uint64_t             del_count;
    lohat_a_history_t           *hist_end;
    _Atomic(lohat_a_history_t *) hist_next;
    _Atomic(lohat_a_store_t *)   store_next;
    lohat_a_history_t           *hist_buckets;
    lohat_a_indirect_t           ptr_buckets[];
};

typedef struct {
    alignas(8)
    _Atomic(lohat_a_store_t *) store_current;
    _Atomic(uint64_t)          item_count;
} lohat_a_t;

void            lohat_a_init   (lohat_a_t *);
void           *lohat_a_get    (lohat_a_t *, hatrack_hash_t *, bool *);
void           *lohat_a_put    (lohat_a_t *, hatrack_hash_t *, void *, bool *);
void           *lohat_a_replace(lohat_a_t *, hatrack_hash_t *, void *, bool *);
bool            lohat_a_add    (lohat_a_t *, hatrack_hash_t *, void *);
void           *lohat_a_remove (lohat_a_t *, hatrack_hash_t *, bool *);
void            lohat_a_delete (lohat_a_t *);
uint64_t        lohat_a_len    (lohat_a_t *);
hatrack_view_t *lohat_a_view   (lohat_a_t *, uint64_t *, bool);

#endif
