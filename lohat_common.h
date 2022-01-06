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
 *  Name:           lohat_common.h
 *  Description:    Data structures and constants shared across the
 *                  lohat family.
 *
 *  Author:         John Viega, john@zork.org
 *
 */

#ifndef __LOHAT_COMMON_H__
#define __LOHAT_COMMON_H__

#include "hatrack_common.h"

/* We use an "epoch" counter that is incremented with every write
 * committment, that gives us an insertion order that we can sort on,
 * when proper ordering is desired.  We can also use a second array to
 * store key/value pairs and index into it from the unordered array.
 * When we do that, there will be a natural ordering, but it will be
 * the order in which buckets are "reserved" for writing, and not
 * (necessarily) the order in which writes were COMMITTED.  Generally,
 * the write order is more desirable than the committment order, but,
 * if we choose to, we can leverage the committment order to speed up
 * calculating the write order. Here are some options:
 *
 * 1) We can just have typical unordered buckets, and only sort the
 *    contents at a linearization point, when required (i.e., by the
 *    unique "epoch" associated with the write at the time the write
 *    was committed). The list is not really being kept in any natual
 *    "order" per se, but the insertion order is retained, and thus we
 *    can recover it, even though the expected computational
 *    complexity of an appropriate sort would be about O(n log n).
 *
 * 2) We can just care about the bucket reservation order, and call it
 *    "close enough".
 *
 *    However, in this scenario, the bucket reservation ordering
 *    drifts from the write committment ordering in multiple
 *    situations:
 *
 *      a) If two writers are racing, and the writer who reserves
 *         the first ordered bucket is the last to commit their
 *         write.
 *      b) If we DELETE an key/value pair from the table, then use
 *         the same key for a re-insertion, the second insertion will
 *         be in the same place in the table as the ORIGINAL item
 *         (unless the table expanded after deletion, but before
 *          re-insertion).
 *
 * 3) We can ignore the write committment order, as with option 2, but
 *    keep the reservation order closer to it by requiring REINSERT
 *    operations to get a new bucket reservation, to make the
 *    reservation ordering more intuitive (and, in most cases with low
 *    write contention, correct).  In such a case, we can co-opt space
 *    from the deletion record to point to the location of a new
 *    reservation, IF ANY.
 *
 * 4) We can use approach 2 for storage, and then, when ordering is
 *    important, sort the buckets by write committment / epoch. This
 *    will use up much more space than option #1, and slow down writes
 *    a little bit, but will drive down the expected time of sort
 *    operations, when ordering is needed.
 *
 *    The actual complexity will be impacted by the number of deletes,
 *    but if the number is low, the complexity will approach O(n), the
 *    same hit we have to pay to copy anyway.
 *
 *    Note that ordering is important mainly when iterating over
 *    dictionaries, at which point we will copy out the contents of a
 *    linearized point, and sort that. We will never do an in-place
 *    sort; it will always involve copying.
 *
 * 5) We can use approach 3) for storage, and then sort when needed,
 *    as with #4. Here, the computational complexity of the sort will
 *    generally be VERY close to O(n), but if there are a lot of
 *    deletes, we will run out of buckets and need to migrate the
 *    table much earlier (and more often) than we would have if we
 *    weren't requiring new bucket reservations for re-inserts.
 *
 * We implement all these options, and do some benchmarking of the
 * tradeoffs. The programmer just needs to specify at allocation time:
 *
 * 1) Whether to use bucket reservations.
 * 2) If so, whether to require re-inserts to get new reservations.
 */

typedef struct lohat_record_st lohat_record_t;

/* Our buckets must keep a "history" that consists of pending commits
 * and actual commits that might still be read by a current reader.
 * Older commits will be cleaned up automatically, based on epoch data
 * hidden in the allocation header. Specifically, the hidden header
 * has two fields, one for the commit epoch, and one for the retire
 * epoch. When a newer record comes in on top of us, once the newer
 * record is committed (meaning, its commit epoch is set), then it will
 * change our "retire epoch" to the same value as its commit epoch,
 * which we then use to ensure that the record does not have its
 * memory reclaimed until all reads that started before its retirement
 * epoch have finished their reads.
 *
 * The non-hidden fields are more or less what you'd expect to
 * see... a pointer to the next record associated with the bucket, and
 * a pointer the key/value pair (opaquely called item here-- if we are
 * using this implementation for sets, the data item might not have a
 * value at all.
 *
 * Note that we will, at table migration time, need to steal the least
 * significant two bits of the item pointer to assist with the
 * migration. This is discussed in a bit more detail below.
 */
struct lohat_record_st {
    lohat_record_t *next;
    void           *item;
};

/* This flag indicates whether the CURRENT record is currently
 * considered present or not. Not present can be because it's been
 * deleted, or because it hasn't been written yet.  It's implemented
 * by stealing a bit from the record's "next" pointer.
 *
 * Note that we could do without this flag, the way we do in some of
 * our other tables that use dynamically allocated records, like
 * oldhat. However, stealing the pointer means that we often will NOT
 * need to dereference the pointer unnecessarily.
 */
enum : uint64_t
{
    LOHAT_F_USED = 0x0000000000000001
};

/* These two flags are used in table migration, and are also implemented
 * by stealing bits from pointers. In this case, we steal two bits
 * from the head pointer to the first record.
 *
 * When the table is large enough that a resize is warranted, we pause
 * all writes as quickly as possible, by setting the LOHAT_F_MOVING
 * flag in each history bucket. This tells new writers to help migrate
 * the table before finishing their write, even if they are not adding
 * a new key (i.e., if they only had a modify operation to do).  The
 * LOHAT_F_MOVED flag is used during the migration to tell other
 * threads they don't need to bother trying to migrate a bucket, as
 * the migration is already done.
 *
 * LOHAT_F_MOVED is not strictly necessary. We can let each thread
 * that aids in the migration determine on its own that the migration
 * of a bucket was successful. However, we use it to avoid unnecessary
 * cycles.
 *
 * Readers can safely ignore either of these flags. Even a late
 * arriving reader can ignore them-- let's say a reader shows up,
 * reserving an epoch when a table migration is nearly complete, and
 * writes are about to start up again. Any writes to the new table
 * will always be uninteresting to this reader, because they
 * necessarily will have a later epoch than the reader cares
 * about... even if the reader gets suspended.
 */
enum : uint64_t
{
    LOHAT_F_MOVING = 0x0000000000000001,
    LOHAT_F_MOVED  = 0x0000000000000002
};

#endif
