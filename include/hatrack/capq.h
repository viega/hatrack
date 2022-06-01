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
 *  Name:           capq.c
 *  Description:    A queue whose primary dequeue operation only
 *                  dequeues if the top value is as expected.
 *
 *                  The naive pop() operation on top of cap() retries
 *                  until it succeeds, making that operation
 *                  lock-free.
 *
 *                  However, the whole purpose of this queue is to
 *                  support a wait-free help system, where threads
 *                  stick jobs into the queue, and then process items
 *                  until their item has been processed.
 *
 *                  The compare-and-pop operation makes sure that
 *                  threads can "help" the top() item, yet, if
 *                  multiple threads try to pop it, only one will
 *                  succeed. Threads in that situation do NOT retry
 *                  the cap, so as long as the enqueue and cap
 *                  operations are wait-free, we're in good shape.
 *
 *                  In this queue, the head acts much like hq, in that
 *                  it FAA's, in a ring buffer, and if it catches up
 *                  with the tail, then it resizes the queue.
 *
 *                  The tail only updates via CAS.  We use the 'epoch'
 *                  as the thing that we compare against, and the tail
 *                  epoch is bumped up by 1<<32 for each migration,
 *                  just to ensure there's never any reuse.
 *
 *  Author:         John Viega, john@zork.org
 */

#ifndef __CAPQ_H__
#define __CAPQ_H__

#include <stdint.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <hatrack/hatrack_config.h>


// clang-format off
typedef struct {
    void    *item;
    uint64_t state;
} capq_item_t;

typedef capq_item_t capq_top_t;

typedef _Atomic capq_item_t capq_cell_t;

typedef struct capq_store_t capq_store_t;

struct capq_store_t {
    alignas(8)
    _Atomic (capq_store_t *)next_store;
    uint64_t                size;
    _Atomic uint64_t        enqueue_index;
    _Atomic uint64_t        dequeue_index;
    capq_cell_t             cells[];
};

typedef struct {
    alignas(8)
    _Atomic (capq_store_t *)store;
    _Atomic int64_t         len;
} capq_t;

enum {
    CAPQ_EMPTY              = 0x0000000000000000,
    CAPQ_DEQUEUED           = 0x0800000000000000,
    CAPQ_TOOSLOW            = 0x1000000000000000,
    CAPQ_USED               = 0x2000000000000000,
    CAPQ_MOVED              = 0x4000000000000000,
    CAPQ_MOVING             = 0x8000000000000000,
    CAPQ_FLAG_MASK          = 0xf800000000000000,
    CAPQ_STORE_INITIALIZING = 0xffffffffffffffff
};

static inline int64_t
capq_len(hq_t *self)
{
    return atomic_read(&self->len);
}

capq_t    *capq_new        (void);
capq_t    *capq_new_size   (uint64_t);
void       capq_init       (capq_t *);
void       capq_init_size  (capq_t *, uint64_t);
void       capq_cleanup    (capq_t *);
void       capq_delete     (capq_t *);
uint64_t   capq_enqueue    (capq_t *, void *);
capq_top_t capq_top        (capq_t *, bool *);
bool       capq_cap        (capq_t *, uint64_t);
void      *capq_dequeue    (capq_t *, bool *);

static inline bool
capq_cell_too_slow(capq_item_t item)
{
    return (bool)(item.state & CAPQ_TOOSLOW);
}
		   
static inline uint64_t
capq_set_used(uint64_t ix)
{
    return CAPQ_USED | ix;
}

static inline bool
capq_is_moving(uint64_t state)
{
    return state & CAPQ_MOVING;
}

static inline bool
capq_is_moved(uint64_t state)
{
    return state & CAPQ_MOVED;
}

static inline bool
capq_is_queued(uint64_t state)
{
    return state & CAPQ_USED;
}

static inline bool
capq_is_dequeued(uint64_t state)
{
    return state & CAPQ_DEQUEUED;
}

static inline bool
capq_is_invalidated(uint64_t state)
{
    return state & CAPQ_TOOSLOW;
}

static inline uint64_t
capq_add_moving(uint64_t state)
{
    return state | CAPQ_MOVING;
}

static inline uint64_t
capq_add_moved(uint64_t state)
{
    return state | CAPQ_MOVED | CAPQ_MOVING;
}

static inline uint64_t
capq_extract_epoch(uint64_t state)
{
    return state & ~(CAPQ_FLAG_MASK);
}

static inline bool
capq_can_enqueue(uint64_t state)
{
    return !(state & CAPQ_FLAG_MASK);
}

static inline uint64_t
capq_ix(uint64_t seq, uint64_t sz)
{
    return seq & (sz-1);
}

// Precondition-- we are looking at the right epoch.
static inline bool
capq_should_return(uint64_t state, uint64_t retries)
{
    if (capq_is_queued(state)) {
	return true;
    }

    if (capq_is_dequeued(state) && retries >= CAPQ_TOP_CONTEND_THRESHOLD) {
	return true;
    }

    return false;
}

#endif
