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
 *  Name:           hq.c
 *  Description:    A fast, wait-free queue implementation.
 *
 *  Author:         John Viega, john@zork.org
 */

#include <hatrack.h>

static const hq_item_t empty_cell    = { NULL, HQ_EMPTY };
static const hq_item_t too_slow_mark = { NULL, HQ_TOOSLOW };

static hq_store_t *hq_new_store(uint64_t);
static hq_store_t *hq_migrate  (hq_store_t *, hq_t *);

#define HQ_DEFAULT_SIZE 1024
#define HQ_MINIMUM_SIZE 128

void
hq_init(hq_t *self)
{
    return hq_init_size(self, HQ_DEFAULT_SIZE);
}

void
hq_init_size(hq_t *self, uint64_t size)
{
    size = hatrack_round_up_to_power_of_2(size);
	
    if (size < HQ_MINIMUM_SIZE) {
	size = HQ_MINIMUM_SIZE;
    }
    
    self->store = hq_new_store(size);
    self->len   = ATOMIC_VAR_INIT(0);

    return;
}

hq_t *
hq_new(void)
{
    return hq_new_size(HQ_DEFAULT_SIZE);
}

hq_t *
hq_new_size(uint64_t size)
{
    hq_t *ret;

    ret = (hq_t *)malloc(sizeof(hq_t));
    hq_init_size(ret, size);

    return ret;
}

/* We assume here that this is only going to get called when there are
 * definitely no more enqueuers/dequeuers in the queue.  If you need
 * to decref or free any remaining contents, drain the queue before
 * calling cleanup.
 */
void
hq_cleanup(hq_t *self)
{
    mmm_retire(self->store);
    
    return;
}

void
hq_delete(hq_t *self)
{
    hq_cleanup(self);
    free(self);

    return;
}

/* hq_enqueue is pretty simple in the average case. It only gets
 * complicated when the head pointer catches up to the tail pointer.
 *
 * Otherwise, we're just using FAA modulo the size to get a new slot
 * to write into, and if it fails, it's because a dequeue thinks we're
 * too slow, so we start increasing the "step" value exponentially
 * (dequeue ops only ever increase in steps of 1).
 */
void
hq_enqueue(hq_t *self, void *item)
{
    hq_store_t    *store;
    hq_item_t      expected;
    hq_item_t      candidate;
    uint64_t       cur_ix;
    uint64_t       end_ix;
    uint64_t       max;
    uint64_t       step;
    uint64_t       sz;
    
    mmm_start_basic_op();
    
    store           = atomic_read(&self->store);
    candidate.item  = item;
    step            = 1;
    
    while (true) {
	sz     = store->size;

	while (true) {
	    // Note: it's important we read cur_ix before end_ix.
	    cur_ix = atomic_fetch_add(&store->enqueue_index, step);
	    end_ix = atomic_read(&store->dequeue_index);
	    max    = end_ix + sz;
	    
	    if (cur_ix >= max) {
		break;
	    }

	    expected        = empty_cell;
	    candidate.state = hq_set_used(cur_ix);

	    if (CAS(&store->cells[hq_ix(cur_ix, sz)], &expected, candidate)) {
		atomic_fetch_add(&self->len, 1);
		mmm_end_op();
		
		return;
	    }

	    step <<= 1;
	}
	
	store  = hq_migrate(store, self);
    }
}

void *
hq_dequeue(hq_t *self, bool *found)
{
    hq_store_t *store;
    uint64_t    sz;
    uint64_t    cur_ix;
    uint64_t    end_ix;
    hq_item_t   expected;
    void       *ret;

    mmm_start_basic_op();

    store  = atomic_read(&self->store);

 retry_dequeue:
    sz     = store->size;
    cur_ix = atomic_read(&store->dequeue_index);
    end_ix = atomic_read(&store->enqueue_index);
	
    while (true) {
	if (cur_ix >= end_ix) {
	    return hq_not_found(found);
	}
	
	cur_ix   = atomic_fetch_add(&store->dequeue_index, 1);
	expected = empty_cell;
	
	if (CAS(&store->cells[hq_ix(cur_ix, sz)], &expected, too_slow_mark)) {
	    cur_ix++;
	    end_ix = atomic_read(&store->enqueue_index);	    
	    continue;
	}
	
	if (hq_is_moving(expected.state)) {
	    store = hq_migrate(store, self);
	    goto retry_dequeue;
	}

	if (hq_cell_too_slow(expected)) {
	    return hq_not_found(found); 
	}
	
	ret = expected.item;

	if (!CAS(&store->cells[hq_ix(cur_ix, sz)], &expected, empty_cell)) {
	    // Looped around and competed.
	    store = hq_migrate(store, self);
	    goto retry_dequeue;
	}
	atomic_fetch_sub(&self->len, 1);
	return hq_found(found, ret);
    }

    return hq_not_found(found);
}

hq_view_t *
hq_view(hq_t *self)
{
    hq_view_t  *ret;
    hq_store_t *store;
    bool        expected = false;

    mmm_start_basic_op();

    while (true) {
	store = atomic_read(&self->store);

	if (CAS(&store->claimed, &expected, true)) {
	    break;
	}
	hq_migrate(store, self);
    }

    hq_migrate(store, self);
    mmm_end_op();

    ret          = (hq_view_t *)malloc(sizeof(hq_view_t));
    ret->store   = store;
    ret->next_ix = 0;

    return ret;
}

void *
hq_view_next(hq_view_t *view, bool *found)
{
    hq_item_t item;

    while (true) {
	if (view->next_ix >= view->store->size) {
	    if (found) {
		*found = false;
	    }
	    return NULL;
	}

	item = atomic_read(&view->store->cells[view->next_ix++]);

	if (hq_is_queued(item.state)) {
	    if (found) {
		*found = true;
	    }
	    return item.item;
	}
    }
}

void
hq_view_delete(hq_view_t *view)
{
    mmm_retire(view->store);

    free(view);

    return;
}

static hq_store_t *
hq_new_store(uint64_t size)
{
    hq_store_t *ret;
    uint64_t    alloc_len;

    alloc_len = sizeof(hq_store_t) + sizeof(hq_cell_t) * size;
    ret       = (hq_store_t *)mmm_alloc_committed(alloc_len);
    
    ret->size = size;

    return ret;
}

static hq_store_t *
hq_migrate(hq_store_t *store, hq_t *top)
{
    hq_store_t *next_store;
    hq_store_t *expected_store;
    hq_item_t   expected_item;
    hq_item_t   candidate_item;
    hq_item_t   old_item;
    uint64_t    i, j, n;
    uint64_t    stored_epoch;
    uint64_t    lowest;
    uint64_t    lowest_ix;

    next_store = atomic_read(&top->store);

    if (next_store != store) {
	return next_store;
    }

    lowest    = 0;
    lowest_ix = 0;
    j         = 0;

    i = store->size;

    /* Note that it's possible for enqueues and dequeues to finish for
     * one slot faster than the operation on either side.  For
     * instance, we might try to dequeue at position 0 and 1, and slot
     * 1 manages to dequeue before we lock the queue, but slot 0 does not.
     *
     * That's conceptually an issue, however the thread that was
     * slated to dequeue 0 will still retry, and some thread that
     * retries will necessarily return 0.  Even if there are no future
     * dequeue requests, we're guaranteed to get the value returned.
     * So it can be simply be seen as a slow returner, that is still
     * linearized before the 1 return; the thread just helped migrate
     * where the other thread did not.
     *
     * If we care, we can address this issue, at some cost to
     * performance. But I don't think that's necessary.
     */
    while (i--) {
	expected_item = atomic_read(&store->cells[i]);

	while (true) {
	    stored_epoch = 0;
	    
	    if (hq_is_moving(expected_item.state)) {
		break;
	    }

	    if (!hq_is_queued(expected_item.state)) {
		candidate_item.item = NULL;
		candidate_item.state = hq_add_moved(expected_item.state);
	    }
	    else {
		candidate_item       = expected_item;
		candidate_item.state = hq_add_moving(expected_item.state);
		stored_epoch         = hq_extract_epoch(expected_item.state);
	    }

	    if (CAS(&store->cells[i], &expected_item, candidate_item)) {
		expected_item = candidate_item;
		break;
	    }
	}
	
	if (hq_is_queued(expected_item.state)) {
	    j++;
	}

	if (stored_epoch && stored_epoch < lowest) {
	    lowest    = stored_epoch;
	    lowest_ix = i;
	}
    }

    expected_store = NULL;
    next_store     = hq_new_store(store->size << 1);

    atomic_store(&next_store->enqueue_index, HQ_STORE_INITIALIZING);
    atomic_store(&next_store->dequeue_index, HQ_STORE_INITIALIZING);    

    if (!CAS(&store->next_store, &expected_store, next_store)) {
	mmm_retire_unused(next_store);
	next_store = expected_store;
    }

    i = lowest_ix;
    n = 0;

    while (true) {
	old_item = atomic_read(&store->cells[i]);
	
	if (hq_is_moved(old_item.state)) {
	    if (hq_is_queued(old_item.state)) {
		n++;
	    }
	    i = i + 1 & (store->size - 1);
	    if (i == lowest_ix) {
		break;
	    }
	    continue;
	}

	expected_item        = empty_cell;
	candidate_item       = old_item;
	candidate_item.state = HQ_USED;
	CAS(&next_store->cells[n++], &expected_item, candidate_item);

	candidate_item       = old_item;
	candidate_item.state = hq_add_moved(old_item.state);

	CAS(&store->cells[i], &old_item, candidate_item);
	
	i = i + 1 & (store->size - 1);
	if (i == lowest_ix) {
	    break;
	}
	
    }
    i = HQ_STORE_INITIALIZING;
    CAS(&next_store->dequeue_index, &i, 0);
    i = HQ_STORE_INITIALIZING;
    CAS(&next_store->enqueue_index, &i, n);

    if (CAS(&top->store, &store, next_store)) {
	if (!store->claimed) {
	    mmm_retire(store);
	}
    }
    
    return next_store;
}
