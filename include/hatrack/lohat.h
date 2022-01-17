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
 *  Name:           lohat.h
 *  Description:    Linearizeable, Ordered, Wait-free HAsh Table (LOHAT)
 *                  This version never orders, it just sorts when needed.
 *                  Views are fully consistent.
 *
 *                  Note that, even though this was one of the
 *                  earliest hats in my hatrack, it is the one where I
 *                  still plan to make significant additions.
 *
 *                  As I make those changes, I will migrate them to
 *                  the two lohat variants, woolhat and to ballcap
 *                  (our locking-based table that keeps proper
 *                  ordering), where appropriate.
 *
 *                  As a result, this algorithm may not stay quite as
 *                  well documented as the others, until those
 *                  enhancements are completed.
 *
 *                  Note that all of the logic for things like bucket
 *                  acquisition is essentially identical to hihat,
 *                  which is thoroughly documented. Getting a good
 *                  understanding of that algorithm before coming to
 *                  lohat is not a bad idea.  But currently, you
 *                  should be able to get a full understanding of
 *                  lohat without looking at the other tables.
 *
 *
 *                  In our more basic hash tables, out buckets have
 *                  all looked pretty similar. They've contained a
 *                  hash value, an item, and some associated state
 *                  (generally, the things we've cared about are:
 *                  whether the bucket has an actual item in it, sort
 *                  ordering, or migration status).
 *
 *                  In a few of those implementations, we've used mmm,
 *                  our memory management wrapper, to give us a unique
 *                  "epoch" as a time stamp that we can use for sort
 *                  ordering. But that time stamp has generally been
 *                  created at the moment where the record was
 *                  allocated, not when it got added to the
 *                  table. Since a thread can be suspended for a
 *                  significant amount of time between allocation and
 *                  inserts, it's quite possible in those other
 *                  algorithms for a record overwrite to replace
 *                  something with a HIGHER sort ordering.
 *
 *                  Really, that's not too much of a problem-- the
 *                  linearization point for mutation operations in
 *                  those algorithms is generally when the content
 *                  record is properly installed (either via an atomic
 *                  store when we use mutexes, or CAS when we do
 *                  not). It just means that our sort order is just an
 *                  approximization of the fully linearized insertion
 *                  order.
 *
 *                  However, on this issue, we can do much, much, much
 *                  better. First, lohat can make the sort ordering
 *                  match the linearization point of the
 *                  algorithm. The "write committment" epochs it uses
 *                  BECOME the linearization point for the algorithm--
 *                  each write will have a later epoch than the item
 *                  it overwrote, and each reader will see a view that
 *                  is completely consistent from their point of view,
 *                  as well.
 *
 *                  More than that, we can fully linearize operations
 *                  ACROSS buckets. What does that mean? Let's revisit
 *                  a problem we discussed with some of our other hash
 *                  tables... getting inconsistent results from a view
 *                  operation.
 *
 *                  In many of our other algorithms, their view can be
 *                  inconsistent, meaning that it might not capture
 *                  the state of the hash table at ANY given moment in
 *                  time. In the case of tables with frequent writes,
 *                  views are likely to be inconsistent in those
 *                  tables.
 *
 *                  The contents of individual buckets will always be
 *                  independently consistent; we will see them
 *                  atomically.  But relative to each other, there can
 *                  be issues.
 *
 *                  For instance, imagine there are two threads, one
 *                  writing, and one creating a view.
 *
 *                  The writing thread might do the following ordered
 *                  operations:
 *
 *                    1) Add item A, giving us the state:    { A }
 *                    2) Add item B, giving us the state:    { A, B }
 *                    3) Remove item A, giving us the state: { B }
 *                    4) Add item C, giving us the state:    { B, C }
 *
 *                  The viewing thread, going through the bucket in
 *                  parallel, might experience the following:
 *
 *                    1) It reads A, sometime after write event 1, but
 *                       before event 2.
 *                    2) It reads the bucket B will end up in, before B
 *                       gets there.
 *                    3) The viewer going slowly, B gets written, then C
 *                       gets written.
 *                    4) The viewer reads C.
 *
 *                  The resulting view is { A, C }, which was never
 *                  the state of the hash table in any logical sense.
 *                  Similarly, we could end up with { A, B, C },
 *                  another incorrect view.
 *
 *                  With lohat, however, we can get fully consistent
 *                  views, meaning that the results of a view
 *                  operation conceptually map to a single moment in
 *                  time across the entire table.
 *
 *                  Indeed, we could even use lohat's linearization
 *                  data to provide a full "change history", starting
 *                  either from the the creation of the hash table,
 *                  or, more practically, from the beginning of a view
 *                  operation.
 *
 *                  That means you could have some sort of iteration
 *                  object on dictionaries that captures the current
 *                  snapshot, iterates over that, and then yields any
 *                  deltas when the original view is exhausted.
 *
 *                  Right now, I've only implemented a single,
 *                  moment-in-time view operation, that returns all of
 *                  the items from the hash table, at the
 *                  linearization point. And, it can return them in
 *                  insertion order, if desired.  However, the more
 *                  dynamic changelog feature is straightforward given
 *                  our implementation, and I expect to implement a
 *                  proof of concept, time permitting (I'll at least
 *                  explain how in my external documentation).
 *
 *                  One key thing that makes lohat different from our
 *                  other algorithms is that, whereas our other
 *                  algorithms keep the current state of the bucket,
 *                  lohat can keep old bucket state around. When we go
 *                  to replace a record, instead of overwriting it, we
 *                  push our new record onto a stack of records.
 *
 *                  That sounds like it could be wasteful, keeping the
 *                  full history of a large, dynamic hash table (in
 *                  fact, immutable hash tables in functional
 *                  languages may do something quite like this). But,
 *                  we don't keep the entire state around; we
 *                  definitely delete old records.
 *
 *                  You might imagine that the memory management might
 *                  be expensive and/or difficult, but it's not, as we
 *                  will explain in a moment!
 *
 *                  The next key difference between lohat our less
 *                  sophisticated tables, is the fact we mentioned
 *                  above-- that the epoch of a record is tied to the
 *                  linearization point.
 *
 *                  Whereas other algorithms get their epoch when
 *                  allocated, we get it AFTER we've successfully
 *                  inserted a record into the table.
 *
 *                  Of course, we have to worry that our thread can
 *                  get pre-empted, and other threads could come in
 *                  and see a record without an epoch timestamp.
 *
 *                  Lohat solves this problem by having other writers
 *                  visiting the bucket, and any operation depending
 *                  on getting a linearized view, to pause to "help"
 *                  the record get an epoch timestamp (Note that a
 *                  typical "get" operation really only cares about
 *                  linearization of the bucket, and so, in this
 *                  implementation, we do NOT have readers "help";
 *                  they safely ignore the epoch, and look only at the
 *                  topmost value.  You can certainly have readers
 *                  help if you like).
 *
 *                  So, even if a write committment is slow (usually
 *                  due to thread preemption), the associated write is
 *                  guaranteed to get an epoch before any other writer
 *                  overwrites the bucket, and before any operation
 *                  that cares about a linearized view (right now,
 *                  just the lohat_view() function) reads the
 *                  contents.
 *
 *                  Our final trick! In terms of the memory
 *                  management, when lohat performs a write operation
 *                  whose record replaces a previous record, we not
 *                  only put the previous record below us on the
 *                  stack, we call mmm_retire() on the previous
 *                  record.
 *
 *                  The mmm algorithm will ensure that records that
 *                  have been "retired" stay in the table until all
 *                  operations that have registered an epoch that is
 *                  lower than or equal to the retirement epoch, have
 *                  completed.
 *
 *                  That means, even if we have a slow thread running
 *                  lohat_view(), we're guaranteed that any record
 *                  alive during or after the linearization epoch is
 *                  still alive, and available to or operation. The
 *                  view operation just needs to look at the top of
 *                  the stack, see if it was added AFTER the beginning
 *                  of the operation, and if it was, descend down the
 *                  stack, until we find one of two things:
 *
 *                  1) A record put there BEFORE our linearization
 *                     epoch.
 *
 *                  2) The bottom of the stack, which indicates EVERY
 *                     record in the bucket was added after our
 *                     linearization epoch.
 *
 *                  Since each record records its write committment time,
 *                  and since the previous record was not retired until
 *                  AFTER we committed our write, we can be sure that the
 *                  view operation does not traverse freed records.
 *
 *                  Basically, the history automatically fades away as
 *                  time progresses, much like the stars during a
 *                  sunrise.
 *
 *                  One final note, to mimic Python's behavior when
 *                  providing views of a table sorted by insertion
 *                  time, mmm data records actually keep THREE epochs:
 *
 *                  1) The epoch in which the record was write-committed
 *                     (aka, the write epoch).
 *
 *                  2) The epoch in which the record was retired (the
 *                     retire epoch).
 *
 *                  3) The "creation epoch", which maps to the write
 *                     committment epoch of the oldest record in a
 *                     chain of overwrites (the original insertion or
 *                     re-insertion epoch).
 *
 *                  See lohat_common.h for some more documentation on
 *                  the approach, along with he core data structures
 *                  that are common to the various lohats.
 *
 *  Author:         John Viega, john@zork.org
 */

#ifndef __LOHAT_H__
#define __LOHAT_H__

#include <hatrack/lohat_common.h>

/* lohat_history_t
 *
 * The lohat_history_t data structure is the top of the list of
 * modification records assoiated with a bucket (which will be the
 * unordered array when we're using only one array, and the ordered
 * array otherwise).
 *
 * This data structure contains the following:
 *
 * hv  -- A copy of the hash value. When this is zero, the bucket is
 *        considered "unreserved", and definitely empty.  Once it's
 *        set, it cannot be removed in the lifetime of the current
 *        table store; the item can be removed, but if an item with
 *        the same hash value is re-inserted in the lifetime of the
 *        bucket store, it will end up in the same bucket.
 *
 * head -- A pointer to the top of the record list for the bucket
 *         (From the point of view of write operations, it can be seen
 *         as a stack where we just never pop; we only ever push, read
 *         the top, or traverse down to read other items. Since we
 *         don't pop, I am more likely to call it a (linked) list than
 *         a stack.
 *
 *
 * As for the pointer to the record list, we do NOT care about the ABA
 * problem here, so do not need a counter.  In particular, let's say a
 * writer is about to insert its new record C into the variable head,
 * and sees record A at the top.  If that thread suffers from a long
 * suspension, B might link to A, and then A can get reclaimed.
 * Another thread could go to the memory manager, and get the memory
 * back, and re-add it to the exact same bucket, all before C wakes
 * up.  Yes, the A is not the "same" A we saw before in some sense,
 * but we do not care, because our operation is a push, not a pop.
 * The item we're pushing correctly points to the next item in the
 * list (from a linearization perspective), if the CAS succeeds.
 *
 * As mentioned above, we "push" records onto the record list like a
 * stack, but we never really remove items from the list at
 * all. Instead, when items are "retired", they are passed off to mmm,
 * who sticks them on a list, and keeps them there until it can prove
 * that no thread will ever algorithmically try to acquire that
 * record, at which time it can safely reclaim the memory.  So we
 * never actually bother unlink the items.  Records basically will
 * magically disappear from this stack. And our algorithms will keep
 * us from descending into freed memory (see above).
 *
 * Note that when we go to add a new record associated with a bucket,
 * we have multiple strategies for handling any CAS failure:
 *
 * 1) We can continue to retry until we succeed. This should be fine
 *    in practice, but in theory, other threads could update the value
 *    so frequenty, we could have to try an unbounded number of
 *    times. Therefore, this approach is lock free, but not wait
 *    free. That means such an operation can, in theory, take an
 *    unbounded amount of time to complete.
 *
 * 2) We can treat the losing thread as if it were really the
 *    "winning" thread... acting as if it has inserted a fraction of a
 *    second before the competing thread, but in the exact same
 *    epoch. In such a case, no reader could possibly see this value,
 *    and so it is safe to forego inserting it into the table. This
 *    approach is trivially wait free, since it doesn't loop.
 *
 * 3) We can use the first approach, but with a bounded number of
 *    loops, before switching to the 2nd approach (or some other
 *    "helping" mechanism). This is also wait-free.
 *
 * The second and third options do open up some minor memory
 * management questions, whereas the first is straightforward from a
 * memory management perspective.
 *
 * Still, in this implementation, we go with approach #2, as it's not
 * only more efficient to avoid retries, but it's in some sense more
 * satisfying to me to move the commit time, in the cases where two
 * threads essentially combine, a miniscule time backwards to resolve
 * the colision, rather than a potentially large (and in the first
 * case, theoretically unbounded) amount of time forward.
 *
 * The memory management for this problem does require some
 * care. While MMM does make it easy to handle record retirement
 * properly, we should also consider memory management for the items
 * IN the table, even though we expect the actual memory management to
 * be handled by the caller.  Here are the scenarios where an item is
 * being conceptually deleted from the table, and so memory management
 * might be appropriate:
 *
 * 1) If there's an explicit call to delete the entry associated with a
 *    key.
 * 2) If we overwrite the entry with a new entry.
 *
 * One option for dealing with this scenario is to explicitly return
 * items through the API. For instance, if you call delete, you'll get
 * back the previous key / value pair. Similarly for a put() operation
 * that overwrites another.
 *
 * A slight problem here is that a single delete can effectively
 * remove multiple entries from a bucket, if there's contention on the
 * writing. If there are conflicting writes and we decide to silently
 * drop one on the floor, per our wait-free strategy above, the
 * conceptual "overwrite" won't even have awareness that it's
 * overwriting the data.
 *
 * A solution here is to have the operation that we're really dropping
 * from the table return its own key/value as previous entries that
 * may need to be deleted. That has the advantage of giving the
 * programmer the opportunity to choose to retry instead of accepting
 * the default behavior. However, in practice, people aren't really
 * going to care, and they're far more likely to forget to do the
 * memory management.
 *
 * A second solution is to have the user register a memory management
 * handler that gets called on any deletion from the table.
 *
 * Currently, we're taking the former approach, and expecting a
 * wrapper API to handle this, since such a thing is also needed for
 * applying the actual hash function.
 *
 * Note that, if you have an application where parallel readers can
 * get the same values, it can be tough to figure out when a deletion
 * from the table should REALLY result in the underlying object
 * getting deleted.
 *
 * You *could* use an epoch-based system, as we do for our
 * records. But a global epoch based system would make it easy to end
 * up with a system that never frees much of anything, so you'd have
 * to be careful with how you use it, to avoid this issues.
 *
 * Obviously, you could also use reference counting, garbage
 * collection, etc. We expect this all to be application dependent,
 * and outside of the scope of this project.
 */
// clang-format off

typedef struct {
    alignas(16)
    _Atomic hatrack_hash_t    hv;
    _Atomic(lohat_record_t *) head;
} lohat_history_t;

// clang-format on

typedef struct lohat_store_st lohat_store_t;

/* lohat_history_t
 *
 * When the table gets full and we need to migrate, we'll need to keep
 * two copies of the underlying data store at the same time-- the old
 * one, and the one we're migrating into. To that end, we need to be
 * able to swap out and eventually delete old copies of the hash
 * table.
 *
 * To that end, the "lohat_t" hashtable type contains a
 * "lohat_store_t", which represents the current table.  When we
 * migrate the table, the lohat_store_t will point to the one we're
 * working on migrating to, and when the migration is complete, the
 * lohat_t reference to the current store will be atomically shifted
 * to the new table.  At that point, the old table will be "retired",
 * meaning it will be freed when there are definitely no more threads
 * attempting to operate on the table.
 *
 * Readers always read from the table that's currently installed when
 * they read the store pointer. A new store may get subsequently
 * installed, but that's okay. Basically, the reader's linearization
 * point is set BEFORE it gets a table reference, which needs to be
 * done after calling mmm_start_basic_op(). The reader is asking to
 * read SOME state of the table no earlier than that epoch.
 *
 * Note that we are NOT guaranteeing that the read is linearized at
 * the moment of time that the thread made its epoch reservation.
 *
 * Nor are we guaranteeing readers the most recent value at the time
 * of read.
 *
 * Instead, our guarantee is that the read time will be bounded-- on
 * the lower end by the epoch reserved via mmm_start_basic_op().
 *
 * We WILL always get the most recent read out of the *current* store
 * (the one installed when we grabbed a reference to it), at whatever
 * epoch it is, when we do the read.  If the store has migrated before
 * the read operation has finished, the read linearizes to the time of
 * the migration.
 *
 * Therefore, the read operation's linearization point is bounded on
 * the lower end by the epoch of the time in which it acquired a
 * pointer to a store, and on the upper end by the time at which a
 * migration operation causes each bucket in the current store to be
 * locked for writing.
 *
 * If it were desirable to always try to get the most recent data,
 * even in the face of migrations, it's easy for us to do so. We could
 * change the algorithm to simply read from its current store, then
 * before it returns, check to see if a new store has been
 * installed. If it has, retry. If it hasn't, return.
 *
 * Unlike our current operation, the simple implementation of that
 * operation isn't wait-free, it would be lock free (tables with a lot
 * of thrash could migrate a lot without changing sizes). Though, it's
 * simple to make it wait-free without cost, using the helping
 * mechanism used for table migrations in witchhat (and woolcap).
 *
 * We may experiment with such an operation in the future, but we
 * believe that the 'right' behavior for most applications is for read
 * operations to return as fast as possible with a consistent view,
 * not to always try to get the most-up-to-date result when there
 * might be races for a bucket.
 *
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
 *               number of unsorted buckets. This is used in the
 *               resize determination.
 *
 * used_count    This counts how many hash buckets in the current store
 *               have a hash value stored in them.  We will migrate
 *               the table when this reaches 75% of the total number
 *               of buckets.  Technically, it's an approximate value,
 *               because it's possible to reserve a bucket and then
 *               suspend indefinititely (or be killed) right before we
 *               update the counter. In practice, that will be
 *               incredibly uncommon, and this will good enough.
 *
 *               If, for some reason, you want a "correct" count, then
 *               dump the hash table buckets to an array (intended for
 *               iteration), which will linearize the table, and give
 *               the exact number of buckets from that epoch.
 *
 * hist_buckets  In this table, these buckets are unordered (unlike
 *               lohat1 and lohat2). As with the other tables,
 *               this field contains the actual active key/value
 *               pairs.
 *
 * store_next    When writer threads realize it's time to migrate,
 *               they will try to create the next store, if it hasn't
 *               been put here by the time they read it. Once they
 *               find the agreed upon store, they all race to migrate.
 *               Only writers care about this variable, and only during
 *               migration.
 */
// clang-format off

struct lohat_store_st {
    alignas(8)
    uint64_t                 last_slot;
    uint64_t                 threshold;
    _Atomic uint64_t         used_count;
    _Atomic(lohat_store_t *) store_next;
    lohat_history_t          hist_buckets[];
};

/* lohat_store_t
 *
 * The top-level lohat object. Most of the state is store-specific,
 * so there's not much here.
 *
 * item_count     This indicates how many ACTUAL ITEMS are in the
 *                table, in the current epoch... approximately.  As
 *                with used_count, we update it every time it's
 *                appropriate, but again, it's just an
 *                approximation. We use the approximation not just in
 *                calculating the length, but to determine, when it's
 *                time to migrate, whether we should GROW the table
 *                when we migrate, or if we should just migrate to a
 *                table of the same size. We will never shrink the
 *                table.  Currently, if about 1/2 the overall
 *                unordered buckets are empty (or more), we migrate to
 *                an identically sized table. Any more full than that,
 *                and we double the table size.
 *
 * store_current  The current store to use. When we migrate the table,
 *                this will change at the very end of the migration
 *                process. Note that some readers might still be
 *                reading from the old store after the migration is
 *                completed, so we'll have to be sure not to delete it
 *                prematurely.
 *
 */
// clang-format off

typedef struct lohat_st {
    alignas(8)
    _Atomic(lohat_store_t *) store_current;
    _Atomic uint64_t         item_count;
} lohat_t;


/* This API requires that you deal with hashing the key external to
 * the API.  You might want to cache hash values, use different
 * functions for different data objects, etc.
 *
 * We do require 128-bit hash values, and require that the hash value
 * alone can stand in for object identity. One might, for instance,
 * choose a 3-universal keyed hash function, or if hash values need to
 * be consistent across runs, something fast and practical like XXH3.
 */

// clang-format off
lohat_t        *lohat_new    (void);
void            lohat_init   (lohat_t *);
void            lohat_cleanup(lohat_t *);
void            lohat_delete (lohat_t *);
void           *lohat_get    (lohat_t *, hatrack_hash_t, bool *);
void           *lohat_put    (lohat_t *, hatrack_hash_t, void *, bool *);
void           *lohat_replace(lohat_t *, hatrack_hash_t, void *, bool *);
bool            lohat_add    (lohat_t *, hatrack_hash_t, void *);
void           *lohat_remove (lohat_t *, hatrack_hash_t, bool *);
uint64_t        lohat_len    (lohat_t *);
hatrack_view_t *lohat_view   (lohat_t *, uint64_t *, bool);

// clang-format on

#endif
