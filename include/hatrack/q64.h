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
 *  Name:           q64.h
 *  Description:    A variant of our wait-free queue for x86 systems
 *                  lacking a 128-bit compare and swap.
 *
 *  Author:         John Viega, john@zork.org
 *
 * In the 128-bit version of the algorithm, fields get a full 64 bits
 * for data, and then a state field where most of the state is unused.
 *
 * In this version, we steal the two low bits for the state we
 * need. That means contents must either be pointers, or must fit in
 * 62 bits.
 *
 * In the case of pointers, they will be memory aligned, so stealing
 * the two bits is not going to impact anything. For non-pointer
 * values though, they should be shifted left a minimum of two bits.
 */

#ifndef __Q64_H__
#define __Q64_H__

#include <stdint.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <hatrack/hatrack_config.h>


#define QUEUE_HELP_VALUE 1 << QUEUE_HELP_STEPS

// clang-format off
typedef uint64_t q64_item_t;
typedef _Atomic q64_item_t q64_cell_t;

typedef struct q64_segment_st q64_segment_t;

/* If help_needed is non-zero, new segments get the default segement
 * size. Otherwise, we double the size of the queue, whatever it is.
 * 
 * Combine this with writers (eventually) exponentially increasing the
 * number of cells they jump the counter when their enqueue attempts
 * fail, and this is guaranteed to be wait-free.
 */

struct q64_segment_st {
    alignas(64)
    _Atomic (q64_segment_t *)next;
    uint64_t                 size;
    _Atomic uint64_t         enqueue_index;
    _Atomic uint64_t         dequeue_index;
    q64_cell_t               cells[];
};

typedef struct {
    q64_segment_t *enqueue_segment;
    q64_segment_t *dequeue_segment;
} q64_seg_ptrs_t;

typedef struct {
    alignas(16)
    _Atomic q64_seg_ptrs_t segments;
    uint64_t               default_segment_size;
    _Atomic uint64_t       help_needed;
    _Atomic uint64_t       len;
} q64_t;

enum64(q64_cell_state_t,
       Q64_EMPTY   = 0x00,
       Q64_TOOSLOW = 0x01,
       Q64_USED    = 0x02);

static inline uint64_t
q64_len(q64_t *self)
{
    return atomic_read(&self->len);
}

q64_t   *q64_new      (void);
q64_t   *q64_new_size (char);
void     q64_init     (q64_t *);
void     q64_init_size(q64_t *, char);
void     q64_cleanup  (q64_t *);
void     q64_delete   (q64_t *);
void     q64_enqueue  (q64_t *, void *);
void    *q64_dequeue  (q64_t *, bool *);

#endif
