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
 *  Name:           queue.h
 *  Description:    A fast, wait-free queue implementation.
 *
 *  Author:         John Viega, john@zork.org
 *
 *  Before building Hatrack, I'd implemented a version of the Michael
 *  Scott lock-free queue.
 *
 *  By the time I was done with Hatrack, I knew for sure that it'd be
 *  easy to make something faster, due to all of the unnecessary
 *  memory management in the algorithm, since every enqueue requires a
 *  cell to be seprately malloc'd.  
 *
 *  I designed something in my head using a linked list of arrays into
 *  which items get enqueued. But I looked around before building it,
 *  and found that my idea was already improved upon in the
 *  literature, particularly because my initial approach would have
 *  left enqueues contending with each other.
 *
 *  Morrison / Afek showed how to do a lock-free FIFO without
 *  contention in the typical case, and Yang / Mellow-Crummey improved
 *  the progress guarantee to wait-free.
 *
 *  However, I did not like the Yang / Mellow-Crummey approach to
 *  wait-freedom; the "helping" algorithm came across as overly
 *  complicated on a number of dimensions.
 *
 *  This queue uses my own mechanism to ensure wait-freedom.
 *  Basically, the only time there's contention that could lead to
 *  issues is when queues are nearly empty, such that dequeuers are 
 *  potentially interfering with enqueue operations.
 *
 *  My approach to wait freedom here is twofold:
 *
 *  1) When an enqueue operation starts failing, the enqueue operation
 *     attempts to create extra space bewtween the enqueuers and
 *     dequeuers.  It doubles the number it adds to the enqueue index
 *     every time there's a successive failure.  
 *
 *  2) Additionally, when there are a certain number of fails in a
 *     row, the enqueuer registers for "help", and deregisters once
 *     it enqueues successfully.
 *
 *     Whenever "help" is requested, and a new segment is needed, threads
 *     will double the size of the new segment.
 *
 *     Because we're essentially adding an exponential amount of space
 *     between the enqueues and the dequeues (who only ever advance
 *     their index one item at a time), it will require a low, bounded
 *     number of attempts before a successful enqueue.
 *
 *     Future segments go back to the original size, as long as no
 *     help is required at the time.
 *
 *  This approach I would expect to be more efficient when helping is
 *  actually required, and is certainly at least much simpler.
 *
 *  Additionally, we use MMM for memory management, as opposed to the
 *  custom hazard pointer hybrid.
 */

#ifndef __QUEUE_H__
#define __QUEUE_H__

#include <stdint.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <hatrack/hatrack_config.h>


#define QUEUE_HELP_VALUE 1 << QUEUE_HELP_STEPS

// clang-format off
typedef struct {
    void    *item;
    uint64_t state;
} queue_item_t;

typedef _Atomic queue_item_t queue_cell_t;

typedef struct queue_segment_st queue_segment_t;

/* If help_needed is non-zero, new segments get the default segement
 * size. Otherwise, we double the size of the queue, whatever it is.
 * 
 * Combine this with writers (eventually) exponentially increasing the
 * number of cells they jump the counter when their enqueue attempts
 * fail, and this is guaranteed to be wait-free.
 */

struct queue_segment_st {
    alignas(64)
    _Atomic (queue_segment_t *)next;
    uint64_t                   size;
    _Atomic uint64_t           enqueue_index;
    _Atomic uint64_t           dequeue_index;
    queue_cell_t               cells[];
};

typedef struct {
    queue_segment_t *enqueue_segment;
    queue_segment_t *dequeue_segment;
} queue_seg_ptrs_t;

typedef struct {
    alignas(16)
    _Atomic queue_seg_ptrs_t segments;
    uint64_t                 default_segment_size;
    _Atomic uint64_t         help_needed;
    _Atomic uint64_t         len;
} queue_t;

enum64(queue_cell_state_t,
       QUEUE_EMPTY   = 0x00,
       QUEUE_TOOSLOW = 0x01,
       QUEUE_USED    = 0x02);

static inline uint64_t
queue_len(queue_t *self)
{
    return atomic_read(&self->len);
}

queue_t *queue_new      (void);
queue_t *queue_new_size (char);
void     queue_init     (queue_t *);
void     queue_init_size(queue_t *, char);
void     queue_cleanup  (queue_t *);
void     queue_delete   (queue_t *);
void     queue_enqueue  (queue_t *, void *);
void    *queue_dequeue  (queue_t *, bool *);

#endif
