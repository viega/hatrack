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
 *  Name:           tophat.h
 *
 *  Description:    Adaptive hash table that starts off fast, but
 *                  migrates to a multi-reader / multi-writer
 *                  implementation once the table is accessed by
 *                  multiple threads simultaneously. This is really
 *                  meant to be a proof-of-concept to show how
 *                  language implementations can get the full benefit
 *                  of performance in single-threaded apps, and then
 *                  dynamically adapt to multiple threads.
 *
 *                  The basic idea is to start with a table that is
 *                  geared toward single-threaded use, detect when a
 *                  differnt kind of table is needed, and then
 *                  migrate to the different table.
 *
 *                  Though, note that, in initial testing, the
 *                  performance of algorithms like witchhat is so
 *                  similar to our single threaded reference
 *                  implementation in terms of single-threaded
 *                  performance that I'm not sure it's worth the extra
 *                  complexity.
 *
 *                  The single-threaded table is a modification of
 *                  refhat, that keeps everything critical to readers
 *                  in a 128-bit value so that we can atomically read
 *                  and write, just so we can keep readers going when
 *                  parallel writers start up, and begin migrating.
 *
 *                  The reader has a minimal amount of extra work to
 *                  do-- it makes a couple of check to see if it's
 *                  still single threaded, and then wraps its activity
 *                  in mmm, which is very cheap for readers -- this is
 *                  done to ensrure that, after migration, the table
 *                  the reader is working from doesn't go away, if the
 *                  reader is slow.
 *
 *                  We could, instead, stall all readers as well when
 *                  a second thread comes in, but this approach seems
 *                  to be at least as cheap (at least when you have a
 *                  128-bit atomic load / store), and has the
 *                  advantage of allowing multiple simultaneous
 *                  readers, and not migrating things until there are
 *                  multiple writers.
 *
 *                  Writers look similar, except they use a mutex
 *                  instead of mmm, partially to detect when we have
 *                  multiple writers (so that we know to migrate), but
 *                  also to prevent multiple writers from running in
 *                  parallel. There's also a vtable here, so that we
 *                  can dynamically select the target algorithm (most
 *                  language implementations are using vtables
 *                  extensively anyway).
 *
 *                  When looking at single-thread performance,
 *                  assuming a 128-bit CAS, my initial testing
 *                  indicates that this adds about 5-10% overhead to
 *                  refhat, depending on workload.  Without a 128-bit
 *                  CAS, switching to locking basically gives us
 *                  duncecap when single threaded (and in fact, when
 *                  single threaded, this is effectively a
 *                  ever-so-slightly faster swimpcap).
 *
 *                  But the performance difference between refhat and
 *                  hihat is similarly minimal; this approach seems to
 *                  make the most sense when looking for fully
 *                  consistent hash tables.
 *
 *                  Note that, in real programming language
 *                  implementation, the checking for the switch to
 *                  multiple threads could be handled in the threading
 *                  subsystem, in code that only runs when the first
 *                  thread launches. This would basically eliminate
 *                  the checking we need to do in the table. We could
 *                  just get the signal to migrate during the move to
 *                  threading, and then swap out a vtable.
 *
 *  Author:         John Viega, john@zork.org
 *
 */

#ifndef __TOPHAT_H__
#define __TOPHAT_H__

#include "hatrack_common.h"
#include "hatvtable.h"

/* tophat_migration_t
 *
 * We use this enumeration only to figure out, once we've decided to
 * switch table types, which migration function to run. This isn't
 * user-accessable; the value is set based on which initialization
 * function is called, per below.
 *
 * The thinking behind these four tables is as follows:
 *
 * First, you may want to select between faster tables without
 * consistency, and consistent tables.
 *
 * This could happen in the same application, for instance if you're
 * using both standard dictionaries as well as sets, where
 * intersection and union operations are important.
 *
 * Second, while the wait free versions generally seem to perform
 * better on architectures w/ a 128-bit compare and swap, you may not
 * have such a thing, and you might prefer to stick with locks for
 * performance.
 *
 * That second concern probably merits a compile-time option. And,
 * indeed, my initial version of this used one, but I thought it was
 * more valuable to keeping implementations in lock-step if I made it
 * all run-time configurable, since there's no extra performance
 * penalty to doing so.
 *
 * Currently, TOPHAT_T_FAST_LOCKING         is newshat
 *            TOPHAT_T_FAST_WAIT_FREE       is witchhat
 *            TOPHAT_T_CONSISTENT_LOCKING   is ballcap
 *            TOPHAT_T_CONSISTENT_WAIT_FREE is woolhat
 */
typedef enum
{
    TOPHAT_T_FAST_LOCKING,
    TOPHAT_T_FAST_WAIT_FREE,
    TOPHAT_T_CONSISTENT_LOCKING,
    TOPHAT_T_CONSISTENT_WAIT_FREE
} tophat_migration_t;

/* tophat_st_record_t and tophat_st_bucket_t are straightforward,
 * and together constitute the single-threaded bucket layout.
 */
// clang-format off
typedef struct {
    void     *item;
    uint64_t  epoch;
} tophat_st_record_t;

typedef struct {
            hatrack_hash_t     hv;
    _Atomic tophat_st_record_t record; 
} tophat_st_bucket_t;

/* tophat_st_ctx_t 
 *
 * This is the context object for hash tables, when a table is running
 * single-threaded. It is basically the same as the top-level
 * refhat_t.
 */
typedef struct {
    uint64_t            last_slot;
    uint64_t            threshold;
    uint64_t            used_count;
    uint64_t            item_count;
    tophat_st_bucket_t *buckets;
    uint64_t            next_epoch;
} tophat_st_ctx_t;

/* tophat_t
 *
 * This data structure starts out single-threaded, using one set of
 * variables, and then migrating to a different set of variables, once
 * it switches implementations to support multiple writers.
 *
 * st_table  -- A pointer to the single-threaded hash table instance.
 *              Here, we have inlined a version of refhat_t (the
 *              inlined operations do some additional work).
 *
 * mutex     -- This is used by writers to detect when we need to
 *              migrate to a multi-threaded implementation. The mutex
 *              is completely ignored by readers.
 *
 * dst_type  -- This is where we store information on which
 *              implementation to which we want to migrate this table,
 *              should it be necessary.
 *
 * mt_table  -- The multi-threaded implementation object, which will
 *              be one of the tables listed above (see the notes with
 *              tophat_migration_t).
 *
 * mt_vtable -- A virtual call table, that we use to call the correct
 *              functions in whatever multi-threaded implementation we
 *              selected. We do pay the small price of an extra
 *              indirection, that we can eliminate easily if we only
 *              ever migrate to a static table type.
 */
typedef struct {
    alignas(16)
    tophat_st_ctx_t   *st_table;
    pthread_mutex_t    mutex;
    tophat_migration_t dst_type;
    _Atomic (void *)   mt_table;
    hatrack_vtable_t   mt_vtable;
} tophat_t;        


/* Here, we see that we have four different initialization functions,
 * each of which selects which multi-threaded implementation we want
 * to use (if necessary).
 *
 * We do this instead of passing in a parameter, mainly to not have 
 * to special-case tophat in our testing infrastructure.
 *
 * _fast = Faster table, without fully consistent views
 * _cst  = Consistent views across the table (as opposed to faster table)
 * _mx   = Mutex variant
 * _wf   = Wait-Free variant
 */
void            tophat_init_fast_mx(tophat_t *);
void            tophat_init_fast_wf(tophat_t *);
void            tophat_init_cst_mx (tophat_t *);
void            tophat_init_cst_wf (tophat_t *);
void           *tophat_get         (tophat_t *, hatrack_hash_t, bool *);
void           *tophat_put         (tophat_t *, hatrack_hash_t, void *, bool *);
void           *tophat_replace     (tophat_t *, hatrack_hash_t, void *, bool *);
bool            tophat_add         (tophat_t *, hatrack_hash_t, void *);
void           *tophat_remove      (tophat_t *, hatrack_hash_t, bool *);
void            tophat_delete      (tophat_t *);
uint64_t        tophat_len         (tophat_t *);
hatrack_view_t *tophat_view        (tophat_t *, uint64_t *, bool);

#endif
