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
 *  Name:           vector.h
 *  Description:    A wait-free vector, complete w/ push/pop/peek.
 *
 *  Author:         John Viega, john@zork.org
 */

#ifndef __VECTOR_H__
#define __VECTOR_H__

#include <stdint.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <hatrack/hatrack_config.h>


#define VECTOR_MIN_STORE_SZ_LOG 4

// clang-format off
typedef void (*vector_callback_t)(void *);

typedef struct {
    void     *item;
    int64_t   state;
} vector_item_t;

typedef _Atomic vector_item_t vector_cell_t;

typedef struct vector_store_t vector_store_t;

typedef struct {
    int64_t             next_ix;
    int64_t             size;
    vector_store_t     *contents;
    vector_callback_t   eject_callback;
} vector_view_t;

typedef struct {
    int64_t array_size;
    int64_t  job_id;
} vec_size_info_t;
    
struct vector_store_t {
    alignas(8)
    int64_t                   store_size;
    _Atomic vec_size_info_t   array_size_info;
    _Atomic (vector_store_t *)next;
    _Atomic bool              claimed;
    vector_cell_t             cells[];
};

typedef struct {
    vector_callback_t          ret_callback;
    vector_callback_t          eject_callback;
    _Atomic (vector_store_t  *)store;
    help_manager_t             help_manager;
} vector_t;

vector_t      *vector_new               (int64_t);
void           vector_init              (vector_t *, int64_t, bool);
void           vector_set_ret_callback  (vector_t *, vector_callback_t);
void           vector_set_eject_callback(vector_t *, vector_callback_t);
void           vector_cleanup           (vector_t *);
void           vector_delete            (vector_t *);
void          *vector_get               (vector_t *, int64_t, int *);
bool           vector_set               (vector_t *, int64_t, void *);
void           vector_grow              (vector_t *, int64_t);
void           vector_shrink            (vector_t *, int64_t);
uint32_t       vector_len               (vector_t *);
void           vector_push              (vector_t *, void *);
void          *vector_pop               (vector_t *, bool *);
void          *vector_peek              (vector_t *, bool *);
vector_view_t *vector_view              (vector_t *);
void          *vector_view_next         (vector_view_t *, bool *);
void           vector_view_delete       (vector_view_t *);

enum {
       VECTOR_POPPED   = 0x8000000000000000,
       VECTOR_USED     = 0x4000000000000000,
       VECTOR_MOVING   = 0x2000000000000000,
       VECTOR_MOVED    = 0x1000000000000000,
       VECTOR_JOB_MASK = 0x0fffffffffffffff
};


enum {
    VECTOR_OK,
    VECTOR_OOB,
    VECTOR_UNINITIALIZED
};

enum {
    VECTOR_OP_PUSH = 0,
    VECTOR_OP_POP,
    VECTOR_OP_PEEK,
    VECTOR_OP_GROW,
    VECTOR_OP_SHRINK,
    VECTOR_OP_SLOW_SET,
    VECTOR_OP_VIEW
};
    
#endif
