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
 *  Name:           hatvtable.h
 *  Description:    Support for virtual call tables, used both in the
 *                  tophat algorithm (to switch tables), and in our
 *                  test harness.
 *
 *  Author:         John Viega, john@zork.org
 *
 */

#ifndef __HAT_VTABLE_H__
#define __HAT_VTABLE_H__

#include "hatrack_common.h"

/* For testing, and for our tophat implementation (which switches the
 * backend hash table out when it notices multiple writers), we keep
 * vtables of the operations to make it easier to switch between
 * different algorithms for testing. These types are aliases for the
 * methods that we expect to see.
 *
 * We use void * in the first parameter to all of these methods to
 * stand in for an arbitrary pointer to a hash table.
 */

// clang-format off
typedef void            (*hatrack_init_func)   (void *);
typedef void *          (*hatrack_get_func)    (void *, hatrack_hash_t *,
						bool *);
typedef void *          (*hatrack_put_func)    (void *, hatrack_hash_t *,
						void *, bool *);
typedef void *          (*hatrack_replace_func)(void *, hatrack_hash_t *,
						void *, bool *);
typedef bool            (*hatrack_add_func)    (void *, hatrack_hash_t *,
						void *);
typedef void *          (*hatrack_remove_func) (void *, hatrack_hash_t *,
						bool *);
typedef void            (*hatrack_delete_func) (void *);
typedef uint64_t        (*hatrack_len_func)    (void *);
typedef hatrack_view_t *(*hatrack_view_func)   (void *, uint64_t *, bool);

typedef struct {
    hatrack_init_func    init;
    hatrack_get_func     get;
    hatrack_put_func     put;
    hatrack_replace_func replace;
    hatrack_add_func     add;
    hatrack_remove_func  remove;
    hatrack_delete_func  delete;
    hatrack_len_func     len;
    hatrack_view_func    view;
} hatrack_vtable_t;

#endif
