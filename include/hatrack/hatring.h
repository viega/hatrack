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
 *  Name:           hatring.h
 *  Description:    A wait-free ring buffer.
 *
 *  Author:         John Viega, john@zork.org
 */

#ifndef __HATRING_H__
#define __HATRING_H__

#include <stdint.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <time.h>
#include <hatrack/hatrack_config.h>


typedef struct {
    void    *item;
    uint64_t state;
} hatring_item_t;

typedef _Atomic hatring_item_t hatring_cell_t;

typedef void (*hatring_drop_handler)(void *);

typedef struct {
    alignas(16)
    _Atomic uint64_t             epochs;
    hatring_drop_handler         drop_handler;
    uint64_t                     last_slot;
    uint64_t                     size;
    hatring_cell_t               cells[];
} hatring_t;

enum {
    HATRING_ENQUEUED = 0x8000000000000000,
    HATRING_DEQUEUED = 0x4000000000000000,
    HATRING_MASK     = 0xcfffffffffffffff
};


static inline void *
hatring_not_found(bool *found)
{
    if (found) {
	*found = false;
    }

    return NULL;
}

static inline void *
hatring_found(void *item, bool *found)
{
    if (found) {
	*found = true;
    }

    return item;
}

static inline bool
hatring_is_lagging(uint32_t read_epoch, uint32_t write_epoch, uint64_t size)
{
    if (read_epoch + size < write_epoch) {
	return true;
    }

    return false;
}

static inline uint32_t
hatring_enqueue_epoch(uint64_t ptrs)
{
    return (uint32_t)(ptrs >> 32);
}

static inline uint32_t
hatring_dequeue_epoch(uint64_t ptrs)
{
    return (uint32_t)(ptrs & 0x00000000ffffffff);
}

static inline uint32_t
hatring_dequeue_ix(uint64_t epochs, uint32_t last_slot)
{
    return (uint32_t)(epochs & last_slot);
}
    
static inline uint32_t
hatring_cell_epoch(uint64_t state)
{
    return (uint32_t)(state & 0x00000000ffffffff);
}

static inline bool
hatring_is_enqueued(uint64_t state) {
    return state & HATRING_ENQUEUED;
}

static inline uint64_t
hatring_fixed_epoch(uint32_t write_epoch, uint64_t store_size)
{
    return (((uint64_t)write_epoch) << 32) | (write_epoch - store_size);
}

	       

hatring_t *hatring_new             (uint64_t);
void       hatring_init            (hatring_t *, uint64_t);
void       hatring_cleanup         (hatring_t *);
void       hatring_delete          (hatring_t *);
void       hatring_enqueue         (hatring_t *, void *);
void      *hatring_dequeue         (hatring_t *, bool *);
void       hatring_set_drop_handler(hatring_t *, hatring_drop_handler);
			 
#endif
