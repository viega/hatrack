/*
 * Copyright © 2022 John Viega
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License atn
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *  Name:           flexarray.h
 *  Description:    A fast, wait-free flex array.
 *
 *                  This ONLY allows indexing and resizing the array.
 *                  If you need append/pop operations in addition, see
 *                  the vector_t type.
 *
 *  Author:         John Viega, john@zork.org
 */

#ifndef __FLEXARRAY_H__
#define __FLEXARRAY_H__

#include <stdint.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <hatrack/hatrack_config.h>


#define FLEXARRAY_MIN_STORE_SZ_LOG 4

// clang-format off
typedef void (*flex_callback_t)(void *);

typedef struct {
    void     *item;
    uint64_t  state;
} flex_item_t;

typedef _Atomic flex_item_t flex_cell_t;

typedef struct flex_store_t flex_store_t;

typedef struct {
    uint64_t        next_ix;
    flex_store_t   *contents;
    flex_callback_t eject_callback;
} flex_view_t;
    
struct flex_store_t {
    alignas(8)
    uint64_t                store_size;
    _Atomic uint64_t        array_size;
    _Atomic (flex_store_t *)next;
    _Atomic bool            claimed;
    flex_cell_t             cells[];
};

typedef struct {
    flex_callback_t          ret_callback;
    flex_callback_t          eject_callback;
    _Atomic (flex_store_t  *)store;
} flexarray_t;

flexarray_t *flexarray_new                (uint64_t);
void         flexarray_init               (flexarray_t *, uint64_t);
void         flexarray_set_ret_callback   (flexarray_t *, flex_callback_t);
void         flexarray_set_eject_callback (flexarray_t *, flex_callback_t);
void         flexarray_cleanup            (flexarray_t *);
void         flexarray_delete             (flexarray_t *);
void        *flexarray_get                (flexarray_t *, uint64_t, int *);
bool         flexarray_set                (flexarray_t *, uint64_t, void *);
void         flexarray_grow               (flexarray_t *, uint64_t);
void         flexarray_shrink             (flexarray_t *, uint64_t);
uint32_t     flexarray_len                (flexarray_t *);
flex_view_t *flexarray_view               (flexarray_t *);
void        *flexarray_view_next          (flex_view_t *, bool *);
void         flexarray_view_delete        (flex_view_t *);
void        *flexarray_view_get           (flex_view_t *, uint64_t, int *);
uint64_t     flexarray_view_len           (flex_view_t *);
flexarray_t *flexarray_add                (flexarray_t *, flexarray_t *);

enum64(flex_enum_t,
       FLEX_ARRAY_SHRINK = 0x8000000000000000,
       FLEX_ARRAY_MOVING = 0x4000000000000000,
       FLEX_ARRAY_MOVED  = 0x2000000000000000,
       FLEX_ARRAY_USED   = 0x1000000000000000
       );

enum {
    FLEX_OK,
    FLEX_OOB,
    FLEX_UNINITIALIZED
};

#endif
