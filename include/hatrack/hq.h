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
 *  Name:           hq.h
 *  Description:    A fast, wait-free queue implementation.
 *
 *  Author:         John Viega, john@zork.org
 *
 * I've decided to build a queue that doesn't use segments; instead,
 * it uses a single buffer in a ring, and resizes up if the head
 * pointer catches up to the tail pointer. 
 *
 * Once I realized it was possible, it seemed more likely to work well
 * than continually allocing and freeing segments. And that does
 * indeed seem to be the case. The difference is particularly stark
 * when there are few enqueuers, but lots of dequeuers; the "skipping"
 * of the wait mechanism slows down the segment queue, because it will
 * end up doing a lot more mallocing than this one, which only ever
 * doubles the entire queue in size when it needs to grow.
 */

#ifndef __HQ_H__
#define __HQ_H__

#include <stdint.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <hatrack/hatrack_config.h>


// clang-format off
typedef struct {
    void    *item;
    uint64_t state;
} hq_item_t;

typedef _Atomic hq_item_t hq_cell_t;

typedef struct hq_store_t hq_store_t;
struct hq_store_t {
    alignas(8)
    _Atomic (hq_store_t *)next_store;
    uint64_t              size;
    _Atomic uint64_t      enqueue_index;
    _Atomic uint64_t      dequeue_index;
    hq_cell_t             cells[];
};

typedef struct {
    alignas(8)
    _Atomic (hq_store_t *)store;
    _Atomic int64_t       len;
} hq_t;

enum {
    HQ_EMPTY              = 0x0000000000000000,
    HQ_TOOSLOW            = 0x1000000000000000,
    HQ_USED               = 0x2000000000000000,
    HQ_MOVING             = 0x4000000000000000,
    HQ_MOVED              = 0x8000000000000000,
    HQ_FLAG_MASK          = 0xf000000000000000,
    HQ_STORE_INITIALIZING = 0xffffffffffffffff
};

static inline int64_t
hq_len(hq_t *self)
{
    return atomic_read(&self->len);
}

hq_t *hq_new      (void);
hq_t *hq_new_size (uint64_t);
void  hq_init     (hq_t *);
void  hq_init_size(hq_t *, uint64_t);
void  hq_cleanup  (hq_t *);
void  hq_delete   (hq_t *);
void  hq_enqueue  (hq_t *, void *);
void *hq_dequeue  (hq_t *, bool *);

static inline bool
hq_cell_too_slow(hq_item_t item)
{
    return (bool)(item.state & HQ_TOOSLOW);
}

static inline void *
hq_found(bool *found, void *item)
{
    mmm_end_op();

    if (found) {
	*found = true;
    }

    return item;
}

static inline void *
hq_not_found(bool *found)
{
    mmm_end_op();

    if (found) {
	*found = false;
    }

    return NULL;
}

static inline uint64_t
hq_set_used(uint64_t ix)
{
    return HQ_USED | ix;
}

static inline bool
hq_is_moving(uint64_t state)
{
    return state & HQ_MOVING;
}

static inline bool
hq_is_moved(uint64_t state)
{
    return state & HQ_MOVED;
}

static inline bool
hq_is_queued(uint64_t state)
{
    return state & HQ_USED;
}

static inline uint64_t
hq_add_moving(uint64_t state)
{
    return state | HQ_MOVING;
}

static inline uint64_t
hq_add_moved(uint64_t state)
{
    return state | HQ_MOVED | HQ_MOVING;
}

static inline uint64_t
hq_extract_epoch(uint64_t state)
{
    return state & ~HQ_MOVED;
}

static inline bool
hq_can_enqueue(uint64_t state)
{
    return !(state & HQ_FLAG_MASK);
}

static inline uint64_t
hq_ix(uint64_t seq, uint64_t sz)
{
    return seq & (sz-1);
}

#endif
