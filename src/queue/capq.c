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

#include <hatrack.h>


static const capq_item_t empty_cell      = { NULL, CAPQ_EMPTY };
static const capq_item_t too_slow_marker = { NULL, CAPQ_TOOSLOW };

typedef union {
    capq_item_t cell;
    __uint128_t num;
} unholy_u;

static const unholy_u moving_cell = { .cell = { NULL, CAPQ_MOVING } };
static const unholy_u moved_cell = { .cell = { NULL, CAPQ_MOVING | CAPQ_MOVED } };



static capq_store_t *capq_new_store(uint64_t);
static void          capq_migrate  (capq_store_t *, capq_t *);

#define CAPQ_DEFAULT_SIZE 1024
#define CAPQ_MINIMUM_SIZE 512

void
capq_init(capq_t *self)
{
    return capq_init_size(self, CAPQ_DEFAULT_SIZE);
}

void
capq_init_size(capq_t *self, uint64_t size)
{
    size = hatrack_round_up_to_power_of_2(size);
	
    if (size < CAPQ_MINIMUM_SIZE) {
	size = CAPQ_MINIMUM_SIZE;
    }
    
    self->store         = capq_new_store(size);
    self->len           = ATOMIC_VAR_INIT(0);
    
    self->store->dequeue_index = ATOMIC_VAR_INIT(1L<<32);
    self->store->enqueue_index = ATOMIC_VAR_INIT(1L<<32);    

    return;
}

capq_t *
capq_new(void)
{
    return capq_new_size(CAPQ_DEFAULT_SIZE);
}

capq_t *
capq_new_size(uint64_t size)
{
    capq_t *ret;

    ret = (capq_t *)malloc(sizeof(capq_t));
    capq_init_size(ret, size);

    return ret;
}

/* We assume here that this is only going to get called when there are
 * definitely no more enqueuers/dequeuers in the queue.  If you need
 * to decref or free any remaining contents, drain the queue before
 * calling cleanup.
 */
void
capq_cleanup(capq_t *self)
{
    mmm_retire(self->store);
    
    return;
}

void
capq_delete(capq_t *self)
{
    capq_cleanup(self);
    free(self);

    return;
}

/* capq_enqueue is pretty simple in the average case. It only gets
 * complicated when the head pointer catches up to the tail pointer.
 *
 * Otherwise, we're just using FAA modulo the size to get a new slot
 * to write into, and if it fails, it's because a dequeue or top
 * operation thinks we're too slow, so we start increasing the "step"
 * value exponentially (dequeue ops only ever scan one cell at a time).
 */
uint64_t
capq_enqueue(capq_t *self, void *item)
{
    capq_store_t *store;
    capq_item_t   expected;
    capq_item_t   candidate;
    uint64_t      cur_ix;
    uint64_t      end_ix;
    uint64_t      max;
    uint64_t      step;
    uint64_t      sz;
    capq_cell_t  *cell;    
    
    mmm_start_basic_op();
    
    candidate.item  = item;
    
    while (true) {
	store  = atomic_load(&self->store);
	sz     = store->size;
	step   = 1;
	
	while (true) {
	    // Note: it's important we read cur_ix before end_ix.
	    cur_ix = atomic_fetch_add(&store->enqueue_index, step);
	    end_ix = atomic_load(&store->dequeue_index);

	    max = end_ix + sz;
	    
	    if (cur_ix >= max) {
		break;
	    }

	    cell            = &store->cells[capq_ix(cur_ix, sz)];
	    expected        = empty_cell;
	    candidate.state = capq_set_used(cur_ix);

	    if (CAS(cell, &expected, candidate)) {
		atomic_fetch_add(&self->len, 1);
		mmm_end_op();

		return cur_ix;
	    }

	    if (capq_is_moving(expected.state)) {
		break;
	    }

	    assert(!(expected.state & CAPQ_USED));

	    step <<= 1;
	}
	
	capq_migrate(store, self);
    }
}

capq_top_t
capq_top(capq_t *self, bool *found)
{
    capq_store_t *store;
    uint64_t      cur_ix;
    uint64_t      end_ix;
    uint64_t      candidate_ix;
    uint64_t      sz;
    capq_cell_t  *cell;
    capq_item_t   item;
    
    mmm_start_basic_op();

    store = atomic_read(&self->store);

    while (true) {
	sz     = store->size;
	cur_ix = atomic_load(&store->dequeue_index);
	end_ix = atomic_load(&store->enqueue_index);

	if (cur_ix >= end_ix) {
	    if (found) {
		*found = false;
	    }

	    mmm_end_op();
	    return empty_cell;
	}

	cell = &store->cells[capq_ix(cur_ix, sz)];
	item = atomic_read(cell);

	// We're too slow.
	if (capq_extract_epoch(item.state) > cur_ix) {
	next_slot:
	    candidate_ix = cur_ix + 1;
	    CAS(&store->dequeue_index, &cur_ix, candidate_ix);
	    continue; 
	}

	/* Cell could have been skipped, or could be a slow writer.
	 * Attempt to CAS in TOO SLOW, and if that fails, try
	 * again w/o bumping the dequeue ix.
	 */
	if (!capq_is_queued(item.state)) {
	    if (!CAS(cell, &item, too_slow_marker)) {
		continue; // Might now be in this slot.
	    }
	    goto next_slot;
	}

	if (found) {
	    *found = true;
	}

	mmm_end_op();

	item.state = capq_extract_epoch(item.state);
	return item;
    }
}

bool
capq_cap(capq_t *self, uint64_t epoch)
{
    capq_store_t *store;
    uint64_t      cur_ix;
    uint64_t      end_ix;
    uint64_t      sz;    
    capq_cell_t  *cell;
    capq_item_t   expected;
    
    mmm_start_basic_op();

    store = atomic_read(&self->store);

    while (true) {
	sz     = store->size;
	cur_ix = atomic_read(&store->dequeue_index);
	end_ix = atomic_read(&store->enqueue_index);

	if (cur_ix >= end_ix) {
	    mmm_end_op();
	    return false;
	}

	cell     = &store->cells[capq_ix(cur_ix, sz)];
	expected = atomic_read(cell);

	if (capq_extract_epoch(expected.state) != epoch) {
	    mmm_end_op();	    
	    return false;
	}

	if (!capq_is_queued(expected.state)) {
	    mmm_end_op();	    
	    return false;
	}

	if (capq_is_moving(expected.state)) {
	    capq_migrate(store, self);
	    store = atomic_read(&self->store);
	    continue;
	}

	if (CAS(cell, &expected, empty_cell)) {
	    /* We don't need to bump the tail; the next call to
	     * capq_top() will do it for us.
	     */
	    mmm_end_op();	    
	    return true;
	}

	if (capq_is_moving(expected.state)) {
	    capq_migrate(store, self);
	    store = atomic_read(&self->store);
	    continue;
	}

	mmm_end_op();	
	return false;
    }
}

void *
capq_dequeue(capq_t *self, bool *found)
{
    capq_top_t top;
    bool       f;

    while (true) {
	top = capq_top(self, &f);
	if (!f) {
	    return hatrack_not_found(found);
	}
	if (capq_cap(self, capq_extract_epoch(top.state))) {
	    return hatrack_found(found, top.item);
	}
    }
}

static capq_store_t *
capq_new_store(uint64_t size)
{
    capq_store_t *ret;
    uint64_t    alloc_len;

    alloc_len = sizeof(capq_store_t) + sizeof(capq_cell_t) * size;
    ret       = (capq_store_t *)mmm_alloc_committed(alloc_len);
    
    ret->size = size;

    return ret;
}

static void
capq_migrate(capq_store_t *store, capq_t *top)
{
    capq_store_t *next_store;
    capq_store_t *expected_store;
    capq_item_t   expected_item;
    capq_item_t   candidate_item;
    capq_item_t   old_item;
    uint64_t      i, n;
    uint64_t      highest_ix;
    uint64_t      highest_epoch;
    uint64_t      lowest;    
    uint64_t      move_ctr;
    uint64_t      epoch;
    unholy_u      u;

    highest_ix    = 0;
    highest_epoch = 0;
    
    move_ctr = (store->dequeue_index & 0xffffffff00000000) + 0x0000000100000000;
    
    for (i = 0; i < store->size; i++) {
	
	u.num = atomic_fetch_or_explicit((_Atomic __uint128_t *)&store->cells[i],
					 moving_cell.num,
					 memory_order_relaxed);

	expected_item = u.cell;
	
	if (!capq_is_queued(expected_item.state)) {
	    atomic_fetch_or_explicit((_Atomic __uint128_t *)&store->cells[i],
				     moved_cell.num,
				     memory_order_relaxed);
	}
	epoch = capq_extract_epoch(expected_item.state);
	if (epoch > highest_epoch) {
	    highest_epoch = epoch;
	    highest_ix    = i;
	}
    }

    expected_store = NULL;
    next_store     = capq_new_store(store->size << 1);

    atomic_store(&next_store->enqueue_index, CAPQ_STORE_INITIALIZING);
    atomic_store(&next_store->dequeue_index, CAPQ_STORE_INITIALIZING);    

    if (!CAS(&store->next_store, &expected_store, next_store)) {
	mmm_retire_unused(next_store);
	next_store = expected_store;
    }

    n       = 0;
    lowest      = store->dequeue_index & store->size;
    highest_ix += store->size;
    
    if ((highest_ix > store->size) && (lowest <= (highest_ix - store->size))) {
	lowest = (highest_ix - (int64_t)store->size + 1);
    }

    for (i = lowest; i <= highest_ix; i++) {
	old_item = atomic_read(&store->cells[capq_ix(i, store->size)]);

	if (!capq_is_queued(old_item.state)) {
	    continue;
	}
	
	if (capq_is_moved(old_item.state)) {
	    n++;
	    continue;
	}

	expected_item        = empty_cell;
	candidate_item.item  = old_item.item;
	candidate_item.state = old_item.state & (~(CAPQ_MOVING|CAPQ_MOVED));
	
	CAS(&next_store->cells[n++], &expected_item, candidate_item);

	atomic_fetch_or((_Atomic __uint128_t *)
			&store->cells[capq_ix(i, store->size)],
			 moved_cell.num);
    }

    i = CAPQ_STORE_INITIALIZING;
    CAS(&next_store->dequeue_index, &i, move_ctr);
    i = CAPQ_STORE_INITIALIZING;
    CAS(&next_store->enqueue_index, &i, move_ctr | n);


    if (CAS(&top->store, &store, next_store)) {
	mmm_retire(store);
    }
    
    return;
}
