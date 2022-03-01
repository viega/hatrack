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

#undef HATSTACK_WAIT_FREE

#ifdef  HATSTACK_WAIT_FREE
#define HATSTACK_BACKOFF_INCREMENT 50
#define HATSTACK_MAX_BACKOFF_LOG   10
#define HATSTACK_RETRY_THRESHOLD   7

#include <time.h>
#endif

/* "Valid after" means that, in any epoch after the epoch stored in
 * this field, pushers that are assigned that slot are free to try
 * to write there.
 *
 * Slow pushers assigned this slot in or before the listed epoch
 * are not allowed to write here.
 *
 * Similarly, pushes add valid_after to tell (very) poppers whether
 * they're allowed to pop the item.  As a pusher, if the operation
 * happens in epoch n, we'll actually write epoch-1 into the field, so
 * that the name "valid after" holds true.
 */
typedef struct {
    void    *item;
    uint32_t state;  
    uint32_t valid_after;
} stack_item_t;

typedef _Atomic stack_item_t stack_cell_t;
typedef struct stack_store_t stack_store_t;

typedef struct {
    uint64_t       next_ix;
    stack_store_t *store;
} stack_view_t;

struct stack_store_t {
    alignas(8)
    uint64_t                 num_cells;
    _Atomic uint64_t         head_state;
    _Atomic (stack_store_t *)next_store;
    _Atomic bool             claimed;
    stack_cell_t             cells[];
};

typedef struct {
    alignas(8)
    _Atomic (stack_store_t *)store;
    uint64_t                 compress_threshold;
    
#ifdef HATSTACK_WAIT_FREE
    _Atomic int64_t          push_help_shift;
#endif
    
} hatstack_t;


hatstack_t   *hatstack_new        (uint64_t);
void          hatstack_init       (hatstack_t *, uint64_t);
void          hatstack_cleanup    (hatstack_t *);
void          hatstack_delete     (hatstack_t *);
void          hatstack_push       (hatstack_t *, void *);
void         *hatstack_pop        (hatstack_t *, bool *);
stack_view_t *hatstack_view       (hatstack_t *);
void         *hatstack_view_next  (stack_view_t *, bool *);
void          hatstack_view_delete(stack_view_t *);

enum {
    HATSTACK_HEAD_MOVE_MASK     = 0x80000000ffffffff,
    HATSTACK_HEAD_EPOCH_BUMP    = 0x0000000100000000,
    HATSTACK_HEAD_INDEX_MASK    = 0x00000000ffffffff,
    HATSTACK_HEAD_EPOCH_MASK    = 0x7fffffff00000000,
    HATSTACK_HEAD_INITIALIZING  = 0xffffffffffffffff,
};

static inline bool
head_is_moving(uint64_t n, uint64_t store_size)
{
    return (n & HATSTACK_HEAD_INDEX_MASK) >= store_size;
}

static inline uint32_t
head_get_epoch(uint64_t n)
{
    return (n >> 32);
}

static inline uint32_t
head_get_index(uint64_t n)
{
    return n & HATSTACK_HEAD_INDEX_MASK;
}

static inline uint64_t
head_candidate_new_epoch(uint64_t n, uint32_t ix)
{
    return ((n & HATSTACK_HEAD_EPOCH_MASK) | ix) + HATSTACK_HEAD_EPOCH_BUMP;
}
	       
// These flags / constants are used in stack_item_t's state.
enum {
    HATSTACK_PUSHED    = 0x00000001, // Cell is full.
    HATSTACK_POPPED    = 0x00000002, // Cell was full, is empty.
    HATSTACK_MOVING    = 0x00000004, 
    HATSTACK_MOVED     = 0x00000008  
};

static inline uint32_t
state_add_moved(uint32_t old)
{
    return old | HATSTACK_MOVING | HATSTACK_MOVED;
}

static inline uint32_t
state_add_moving(uint32_t old)
{
    return old | HATSTACK_MOVING;
}

static inline bool
state_is_pushed(uint32_t state)
{
    return (bool)(state & HATSTACK_PUSHED);
}

static inline bool
state_is_popped(uint32_t state)
{
    return (bool)(state & HATSTACK_POPPED);
}

static inline bool
state_is_moving(uint32_t state)
{
    return (bool)(state & HATSTACK_MOVING);
}

static inline bool
state_is_moved(uint32_t state)
{
    return (bool)(state & HATSTACK_MOVED);
}

static inline bool
cell_can_push(stack_item_t item, uint32_t epoch)
{
    if (item.valid_after >= epoch) {
	return false;
    }

    return true;
}

#endif
