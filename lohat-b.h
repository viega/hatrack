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
 *  Name:           lohat-b.h
 *  Description:    Linearizeable, Ordered HAsh Table (LOHAT)
 *                  This version keeps two tables, for partial ordering.
 *                  It does so in a way that insertion sorts have very
 *                  little to do in most practical cases, but at the
 *                  expense of requiring more migrations when there
 *                  are lots of deletes. So this probably isn't a great
 *                  general-purpose table, but might be good when there
 *                  aren't likely to be many deletes.
 *
 *                  The implementation has only minor changes,
 *                  relative to lohat-a; the source code only
 *                  documents those changes.
 *
 *  Author:         John Viega, john@zork.org
 *
 */

#ifndef __LOHATb_H__
#define __LOHATb_H__

#include "lohat_common.h"

typedef struct lohat_b_history_st lohat_b_history_t;

// clang-format off
/*
 * The main difference between lohat-a and lohat-b is that we try
 * harder to keep the history array in its insertion order, by giving
 * re-insertions a more up-to-date location in the history array.
 *
 * TODO-- you are here.
 */
struct lohat_b_history_st {
    alignas(16)
    _Atomic hatrack_hash_t       hv;
    _Atomic(lohat_record_t *)    head;
    _Atomic(lohat_b_history_t *) fwd;
};

/* We're using a second array to improve our sorting costs. These are
 * the buckets that the hash function points to... their contents just
 * point us to where the actual records are. Note that contents of
 * this bucket do not indicate whether an item is actually in the hash
 * table or not; it only keeps "reservations"... hv being set reserves
 * the bucket for the particular hash item, and ptr being set reserves
 * a particular location in the other array.
 */

// clang-format off
typedef struct {
    alignas(16)
    _Atomic hatrack_hash_t       hv;
    _Atomic(lohat_b_history_t *) ptr;
} lohat_b_indirect_t;

/* When the table gets full and we need to migrate, we'll need to keep
 * two copies of the hash table at the same time. To that end, we need
 * to be able to swap out and eventually delete old copies of the hash
 * table.
 *
 * To that end, we have the "lohat_t" hashtable type contain a
 * "lohat_store_t", that represents the current table.  When we
 * migrate the table, the lohat_store_t will point to the one we're
 * working on migrating to, and when the migration is complete, the
 * lohat_t reference to the current store will be atomically shifted
 * to the new table.  At that point, the old table will be "retired",
 * meaning it will be freed when there are definitely no more threads
 * attempting to operate on the table.
 *
 * Fields in this table:
 *
 * last_slot     Indicates the last bucket index for unordered buckets
 *               (one less than the total number of buckets). In our
 *               implementation, tables are always a power of two in
 *               size, because we want to use & whenever we need to
 *               calculate our bucket index, instead of the generally
 *               much more expensive % operator.
 *
 * threshold     This is set when the table is created, to 75% of the
 *               number of unsorted buckets.
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
 *               threshold.
 *
 * hist_next     A pointer to the next reservable bucket.
 *
 * store_next    A pointer to the store to which we are currently
 *               migrating.
 */
typedef struct lohat_b_store_st lohat_b_store_t;

// clang-format off
struct lohat_b_store_st {
    alignas(8)
    uint64_t                     last_slot;
    uint64_t                     threshold;
    _Atomic uint64_t             del_count;
    lohat_b_history_t           *hist_end;
    _Atomic(lohat_b_history_t *) hist_next;
    _Atomic(lohat_b_store_t *)   store_next;
    lohat_b_history_t           *hist_buckets;
    lohat_b_indirect_t           ptr_buckets[];
};

typedef struct {
    alignas(8)
    _Atomic(lohat_b_store_t *) store_current;
} lohat_b_t;

void            lohat_b_init   (lohat_b_t *);
void           *lohat_b_get    (lohat_b_t *, hatrack_hash_t *, bool *);
void           *lohat_b_put    (lohat_b_t *, hatrack_hash_t *, void *, bool *);
void           *lohat_b_replace(lohat_b_t *, hatrack_hash_t *, void *, bool *);
bool            lohat_b_add    (lohat_b_t *, hatrack_hash_t *, void *);
void           *lohat_b_remove (lohat_b_t *, hatrack_hash_t *, bool *);
void            lohat_b_delete (lohat_b_t *);
uint64_t        lohat_b_len    (lohat_b_t *);
hatrack_view_t *lohat_b_view   (lohat_b_t *, uint64_t *, bool);

#endif
