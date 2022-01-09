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
 *  Name:           refhat-a.h
 *  Description:    A reference hashtable that only works single-threaded.
 *
 *  Author:         John Viega, john@zork.org
 *
 */

#ifndef __REFHAT_A_H__
#define __REFHAT_A_H__

#include "hatrack_common.h"

/* refhat_a_record_t
 *
 * We keep the items in a bucket that might change in a single 128-bit
 * value that we can update with C-11's standard atomic API. On the
 * modern architectures we're targeting, there will be architectural
 * support for this. On other systems, there will be an implicit
 * per-bucket lock added.
 *
 * item  -- A pointer to the item being stored in the hash table, which
 *         will generally be a key/value pair in the case of
 *         dictionaries, or just a single value in the case of sets.
 *
 * epoch -- An indication of insertion time, which we will use to sort
 *          items in the dictionary, when we produce a "view" (views
 *          are intended for iteration or set operations).  The epoch
 *          number is chosen relative to other insertions, and
 *          monotonically increases from 1.  If an item is already in
 *          the table, then the value is not updated.
 *
 *          If an item is deleted from the table, epoch will be set to
 *          0, so that's our indication that an item has been removed
 *          from the table (setting item to NULL wouldn't tell us
 *          that, since it's valid to insert a NULL item).
 *
 */
//clang-format off
typedef struct {
    void    *item;
    uint64_t epoch;
} refhat_a_record_t;

/* refhat_a_bucket_t
 *
 * For consistency with our other (parallel) implementations, our
 * refhat hash table doesn't move things around the hash table on a
 * deletion.  Instead, it marks buckets as "deleted" (by setting the
 * epoch value in the record to 0). If the same key gets reinserted
 * before a table resize, the same bucket will be reused.
 *
 * hv      -- The hash value associated with a bucket, if any.
 *
 * record --   Holds the item and sort value, per above. Writers update
 *            this atomically, and readers read it atomically. This
 *            way, our table can actually support multiple readers in
 *            parallel.
 *
 *            Though, writes should not happen in parallel with reads,
 *            because there's no logic to prevent writers from
 *            deleting the store out from under the readers who are
 *            using it.
 *
 *            Duncecap fixes this problem, allowing one writer to work
 *            in parallel. Swimcap addresses it a different way, and
 *            newshat does so while adding multiple writer support.
 *            All of the other tables we have also address this
 *            problem, while supporting multiple writers, and
 *            generally do so without resorting to the use of locks.
 *
 *            One key thing to note is that Tophat, while addressing
 *            the problem, does so a bit differently. It allows you to
 *            use this table, until the point where a writer comes
 *            along, and then it migrates safely to a new
 *            implementation, dispatches readers there, and then waits
 *            for all readers to exit the old store.
 *
 *            While single-threaded, that adds an indirection and two
 *            atomic_fetch_add() calls per read, which is a minimal
 *            cost to single threaded applications.
 *
 * deleted -- Set to true if the item has been removed.
 *
 * epoch   -- An indication of insertion time, which we will use to
 *            sort items in the dictionary, when we produce a "view"
 *            (views are intended for iteration or set operations).
 *            The epoch number is chosen relative to other insertions,
 *            and monotonically increases from 0.  If an item is
 *            already in the table, then the value is not updated.
 */
// clang-format off
typedef struct {
    hatrack_hash_t            hv;
    _Atomic refhat_a_record_t record;
} refhat_a_bucket_t;

/* refhat_a_t
 *
 * The main type for our reference hash table; it contains any
 * information that persists across a table resize operation
 * (everything else lives in the refhat_a_bucket_t type).
 * 
 * last_slot --  The array index of the last bucket, so this will be
 *               one less than the total number of buckets. We store
 *               it this way, because we're going to use this value
 *               far more frequently than the total number.
 *
 * threshold --  We use a simple metric to decide when we need to
 *               migrate the hash table buckets to a different set of
 *               buckets-- when an insertion would lead to 75% of the
 *               buckets in the current table being used.  This field
 *               olds 75% of the total table size.  Note that, when
 *               we actually migrate the buckets, the allocated size
 *               could grow, shrink or stay the same, depending on
 *               how many removed items are cluttering up the table.
 *
 * used_count -- Indicates how many buckets in the table have a hash
 *               value associated with it. This includes both items
 *               currently in the table and buckets that are reserved,
 *               because they have a hash value associated with them,
 *               but the item has been removed since the last
 *               resizing.
 *
 * item_count -- The number of items in the table, NOT counting
 *               deletion entries.
 *
 * buckets    -- The current set of refhat_a_bucket_t objects.
 *
 * next_epoch -- The next epoch value to give to an insertion
 *               operation, for the purposes of sort ordering.
 *
 * There are a few additional fields here that refhat_a does not use,
 * but are intended to support the "tophat" hash table. Tophat uses
 * refhat as a store, until it notices multiple threads accessing the
 * table at once, where at least one of the threads is a writer
 * (multiple concurrent readers are fine).
 *
 * These
 *
 * mutex   -- We put this around the hash table to protect for when
 *            multiple threads come along. If we were doing a
 *            programming language implementation, we'd probably
 *            actually leave off the mutex and memory management work
 *            we do, and do one-time work on first thread startup, to
 *            ensure that we only incur cost when we switch to
 *            multiple threads.
 *
 * backref -- This is used to recover the original tophat instance,
 *            when we're dealing w/ a refhat and realize we need to
 *            switch to another table type. See tophat.h / tophat.c
 */
typedef struct {
    alignas(8)    
    uint64_t          last_slot;
    uint64_t          threshold;
    uint64_t          used_count;
    uint64_t          item_count;
    refhat_a_bucket_t  *buckets;
    uint64_t          next_epoch;
    pthread_mutex_t    mutex;
    _Atomic uint64_t   readers;
    void              *backref;
} refhat_a_t;


/* This API requires that you deal with hashing the key external to
 * the API.  You might want to cache hash values, use different
 * functions for different data objects, etc.
 *
 * We do require 128-bit hash values, and require that the hash value
 * alone can stand in for object identity. One might, for instance,
 * choose a 3-universal keyed hash function, or if hash values need to
 * be consistent across runs, something fast and practical like XXH3.
 */

void            refhat_a_init   (refhat_a_t *);
void           *refhat_a_get    (refhat_a_t *, hatrack_hash_t, bool *);
void           *refhat_a_put    (refhat_a_t *, hatrack_hash_t, void *, bool *);
void           *refhat_a_replace(refhat_a_t *, hatrack_hash_t, void *, bool *);
bool            refhat_a_add    (refhat_a_t *, hatrack_hash_t, void *);
void           *refhat_a_remove (refhat_a_t *, hatrack_hash_t, bool *);
void            refhat_a_delete (refhat_a_t *);
uint64_t        refhat_a_len    (refhat_a_t *);
hatrack_view_t *refhat_a_view   (refhat_a_t *, uint64_t *, bool);

//clang-format on

#endif
