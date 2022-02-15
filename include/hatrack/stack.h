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
 *  Name:           stack.h
 *  Description:    A faster stack implementation that avoids
 *                  using a linked list node for each item.
 * 
 *                  We could devise something that is never going to
 *                  copy state when it needs to expand the underlying
 *                  store, breaking the stack up into linked
 *                  segments. For now, I'm not doing that, just to
 *                  keep things as simple as possible.
 *                  
 *
 *  Author:         John Viega, john@zork.org
 */

#ifndef __STACK_H__
#define __STACK_H__

#include <stdint.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <stdalign.h>

typedef struct {
    void    *item;
    uint32_t state;  // 4 bits of flags, 28 bits of a compression ID.
    uint32_t offset; // During a move, which cell are we moving to?
} stack_item_t;

typedef _Atomic stack_item_t stack_cell_t;
typedef struct stack_store_t stack_store_t;

struct stack_store_t {
    uint64_t                 num_cells;
    _Atomic uint64_t         head_state;
    _Atomic (stack_store_t *)next_store;
    stack_cell_t             cells[];
};

typedef struct {
    _Atomic (stack_store_t *)store;
    uint64_t                 compress_threshold;
} hatstack_t;


hatstack_t *hatstack_new                   (uint64_t);
void        hatstack_init                  (hatstack_t *, uint64_t);
void        hatstack_push                  (hatstack_t *, void *);
void       *hatstack_pop                   (hatstack_t *, bool *);
void        hatstack_set_compress_threshold(hatstack_t *, uint64_t);

/* These flags live in the head state.  Pushes still FAA to the head
 * state to get an index, but will immediately decide to help with the
 * operation, and will know that they overshot.
 */
enum {
    HATSTACK_HEAD_F_COMPRESSING = 0x8000000000000000,
    HATSTACK_HEAD_F_MIGRATING   = 0x4000000000000000,
    HATSTACK_HEAD_CID_ADD       = 0x0000000100000000,
    HATSTACK_HEAD_ISOLATE_CID   = 0x3fffffff00000000
};

/* These flags are used in the state field of stack_item_t.
 */

enum {
    HATSTACK_POPPED      = 0x80000000,
    HATSTACK_BACKSTOP    = 0x40000000,
    HATSTACK_MIGRATING   = 0x20000000,
    HATSTACK_MOVED       = 0x10000000,
    COMPRESSION_MASK     = 0x3fffffff
};



#define HATSTACK_MIN_STORE_SZ_LOG           6
#define HATSTACK_DEFAULT_COMPRESS_THRESHOLD 1 << 4


#endif
