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
 *                  multiple threads simultaneously.
 *
 *                  This uses an extra layer of indirection, so there
 *                  is a very small bit of additional overhead to
 *                  single-threaded applications.
 *
 *                  Specifically, on reads, there's an extra indirect
 *                  call (requiring a vtable lookup), and two addition
 *                  operations of a constant, on an atomic variable
 *                  stored in the data structure.  Actually, we also
 *                  use a second static call that's completely
 *                  unnecessary, but really just for modularity.
 *                  Anyway, it's still not much!
 *
 *                  Writes will lock and unlock a mutex, and will test
 *                  the value of the atomic mutex, and will do the
 *                  extra indirect call.
 *
 *  Author:         John Viega, john@zork.org
 *
 */

#ifndef __TOPHAT_H__
#define __TOPHAT_H__

#include "hatrack_common.h"
#include "witchhat.h"
#include "woolhat.h"
#include "refhat-a.h"
#include "hatvtable.h"

/* tophat_algo_info_t
 *
 * Keeps a pointer to the hash table implementation we're currently
 * using, along with a vtable holding methods on that implementation.
 *
 * The basic idea here is that, when we're single-threaded, htable
 * will be of type refhat_a_t, and it will be passed to a function in
 * the vtable, based on the requested operation. See hatvtable.h for
 * the structure, but there's just one pointer for each function in
 * our API.
 *
 * Then, when we get a concurrent writer thread, it will create a new
 * table, migrate the contents existing table, and then "replace" the
 * implementation in the top-level tophat_t instance by
 * compare-and-swapping the tophat_algo_info_t object in one go.
 *
 * The only remaining challenge then is the memory management-- we
 * need to make sure that, after migration, the old table gets freed,
 * but only after any writers using the table are done.  See tophat.c
 * for the details on how we do that.
 *
 * htable -- Either a pointer to a refhat_a_t, or to the
 *           multi-threaded type to which we migrate.
 *
 * vtable -- A pointer to a virtual function call table, holding
 *           the appropriate methods for the associated hash table.
 */
// clang-format off
typedef struct {
    void              *htable;
    hatrack_vtable_t  *vtable;
} tophat_algo_info_t;

/* implementation -- The tophat_algo_info_t that we swap out atomically
 *                   when the migration is ready.
 *
 * flags          -- Really, only one flag. This controls whether we
 *                   migrate to a table that gives fully consistent
 *                   views, and linearizes the whole table (if
 *                   TOPHAT_F_CONSISTENT_VIEWS is set), or whether we
 *                   get a faster hash table.
 *
 *                   At compile time you can select whether those two
 *                   options are implemented with locks, or with
 *                   wait-free hash tables (the default).  If you do
 *                   want the locking variant for some reason, use 
 *                   TOPHAT_USE_LOCKING_ALGORITHMS.  See config.h.
 *                   
 */
typedef struct {
    alignas(16)
    _Atomic tophat_algo_info_t implementation;
    uint32_t                   flags;
} tophat_t;

enum : uint64_t {
    TOPHAT_F_CONSISTENT_VIEWS = 0x01,
};

void            tophat_init_cst  (tophat_t *);
void            tophat_init_fast (tophat_t *);
void           *tophat_get       (tophat_t *, hatrack_hash_t *, bool *);
void           *tophat_put       (tophat_t *, hatrack_hash_t *, void *, bool *);
void           *tophat_replace   (tophat_t *, hatrack_hash_t *, void *, bool *);
bool            tophat_add       (tophat_t *, hatrack_hash_t *, void *);
void           *tophat_remove    (tophat_t *, hatrack_hash_t *, bool *);
void            tophat_delete    (tophat_t *);
uint64_t        tophat_len       (tophat_t *);
hatrack_view_t *tophat_view      (tophat_t *, uint64_t *, bool);

#endif
