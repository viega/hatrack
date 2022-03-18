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
 * This is mainly an array we use in a ring, until the head grows to
 * meet the tail, at which point we double the size of the store,
 * copying data over.
 *
 * The hardest bit is keeping a linearized ordering intact during this
 * migration operation.
 *
 * If some active enqueues have to wait until after the migration,
 * that's no problem; the threads in the middle of their operations
 * can complete in any order without any of them being able to detect
 * it.
 *
 * We don't have the same luxury on the dequeue side.  For example,
 * imagine we're resizing a queue of size four, that contains: 
 *
 *    [0, 1, 2, 3]
 *
 * If the migration begins in parallel with three dequeuers of various
 * speeds, we might end up having 2 dequeue before 0 and 1.  That in
 * and of itself isn't a problem, as we can consider the linearization
 * point when we handed out the address from which to dequeue-- as
 * long as the three threads each dequeue their item, then all is
 * good; the operations can be linearized, and we just act like the
 * threads just took different speeds to return, conceptually.
 *
 * However, if we pause dequeuers in order to migrate, and then we
 * restart the operation, it would certainly be possible for the
 * thread that dequeued 2 to dequeue 0 or 1, which would violate our
 * linearization.
 *
 * Dequeuers can notice there's a dequeue in progress in two
 * ways. First, they might see it when they're trying to load an index
 * to store into. But, if they miss that signal, they might see it in
 * the cell when they try to read, or to mark the cell as read.
 *
 * If they don't notice the dequeue at all, the threads on either side
 * of them might be slower.  To ensure linearization, we would at
 * least need to make sure that any thread that theoretically dequeues
 * before us finishes its dequeue, even if it's out of the new store.
 *
 * Note also that various threads helping in the migration could end
 * up seeing different values of the various pointers, as late FAAs
 * process.
 *
 * Since different threads might have slightly different views on
 * where the start and end of the list are, we need to make sure we
 * don't get confused.  We don't want to miss copying items or
 * accidentally allow items to be dequeued twice, once in each store.
 *
 * To be clear, what we want to accomplish is to move a contiguous
 * block of cells, ending at what is the head at the time the
 * migration completes that have been enqueued (or skipped; writers
 * are allowed to skip cells, in which case the epoch in the cell will
 * be older than the tail), but have NOT been dequeued.  Once we find
 * ANY dequeued item, we must assume every item with an epoch lower
 * than it is in the process of being dequeued, and NOT migrate it.
 *
 * And, if *we* are a dequeuer who noticed the need to migrate, but
 * not until we were assigned a slot, then we will complete our
 * operation out of the OLD store if it turns out that our index ends
 * up LOWER than something that got dequeued (but only after the
 * migration is finished).  Otherwise, we will re-start the operation
 * in the new store.
 *
 * Once we mark a cell for migration, no other cell will change the
 * status of whether the cell is queued or enqueued, until after the
 * migration is complete.  So, we can simply rush to mark all the
 * cells, while keeping note of the highest epoch found in any cell.
 *
 * Once we have marked all cells, we go to the cell w/ the highest
 * epoch, and scan until we've either scanned all cells, or until we
 * find a cell that has been dequeued.  
 *
 * Remember again that cells can be skipped by the enqueue operation,
 * in which case the scan will show epochs that are lower than the
 * lowest possible epoch.  Those cells get marked as 'moved', since
 * there is no work to do to them.
 *
 * Once we find the starting point for the migration, we do all the
 * typical copying that we'd do in most of our other algorithms. Then,
 * any threads who had pending dequeue operations need to check the
 * epoch they were given; if it's below the starting point, we
 * complete the dequeue out of the old store. Otherwise, we pretend
 * like we never got a number assigned, and try again out of the new
 * store.
 *
 * As a result of the algorithm, our API for the migration is a little
 * bit different than most of our other migrations.  hq_migrate()
 * doesn't return the new store, it instead returns the smallest
 * migrated epoch, so that dequeuers know whether or not to resume.
 *
 * Threads needing to redo in the new store, just re-acquire the store
 * via the top-level object pointer.
 *
 *  Author:         John Viega, john@zork.org
 */

#include <hatrack.h>


static const hq_item_t empty_cell    = { NULL, HQ_EMPTY };

static const union {
    hq_item_t   cell;
    __uint128_t num;
}
    moving_cell = { .cell = { NULL, HQ_MOVING } },
    moved_cell = { .cell = { NULL, HQ_MOVING | HQ_MOVED } };



static hq_store_t *hq_new_store(uint64_t);
static uint64_t    hq_migrate  (hq_store_t *, hq_t *);

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
    
    self->store         = hq_new_store(size);
    self->len           = ATOMIC_VAR_INIT(0);
    
    self->store->dequeue_index = ATOMIC_VAR_INIT(size);
    self->store->enqueue_index = ATOMIC_VAR_INIT(size);    

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
    hq_store_t *store;
    hq_item_t   expected;
    hq_item_t   candidate;
    uint64_t    cur_ix;
    uint64_t    end_ix;
    uint64_t    max;
    uint64_t    step;
    uint64_t    sz;
    uint64_t    epoch;
    hq_cell_t  *cell;    
    
    mmm_start_basic_op();
    
    candidate.item  = item;
    
    while (true) {
	store  = atomic_read(&self->store);
	sz     = store->size;
	step   = 1;
	

	while (true) {
	    // Note: it's important we read cur_ix before end_ix.
	    cur_ix = atomic_fetch_add(&store->enqueue_index, step);
	    end_ix = atomic_read(&store->dequeue_index);

	    if (end_ix & HQ_MOVING) {
		break;
	    }
	    
	    max = end_ix + sz;
	    
	    if (cur_ix >= max) {
		break;
	    }

	    cell     = &store->cells[hq_ix(cur_ix, sz)];
	    expected = atomic_read(cell);
	    epoch    = hq_extract_epoch(expected.state);

	    if (epoch > cur_ix) {
		break;
	    }

	    if (hq_is_moving(expected.state)) {
		break;
	    }

	    if ((epoch < cur_ix) && hq_is_queued(expected.state)) {
		break;
	    }

	    if ((epoch == cur_ix) && hq_cell_too_slow(expected)) {
		step <<= 1;		
		continue;
	    }
	    
	    candidate.state = hq_set_used(cur_ix);

	    if (CAS(cell, &expected, candidate)) {
		atomic_fetch_add(&self->len, 1);
		mmm_end_op();

		return;
	    }

	    if (hq_extract_epoch(expected.state) != cur_ix) {
		break;
	    }

	    if (hq_is_moving(expected.state)) {
		break;
	    }

	    step <<= 1;
	}
	
	hq_migrate(store, self);
    }
}

void *
hq_dequeue(hq_t *self, bool *found)
{
    hq_store_t *store;
    uint64_t    sz;
    uint64_t    cur_ix;
    uint64_t    end_ix;
    uint64_t    epoch;
    hq_item_t   expected;
    hq_item_t   candidate;
    void       *ret;
    hq_cell_t  *cell;

    mmm_start_basic_op();

    store          = atomic_read(&self->store);
    candidate.item = NULL;

 retry_dequeue:
    while (true) {
	sz     = store->size;

	/* First, we should check if it's seems futile to ask for a
	 * dequeue spot.  If it does, we don't want to bump the
	 * tail way past the head, that will only slow us down.
	 */

	cur_ix    = atomic_read(&store->dequeue_index);

	if (cur_ix & HQ_MOVING) {
	    hq_migrate(store, self);
	    store = atomic_read(&self->store);
	    continue;
	}
	
	end_ix    = atomic_read(&store->enqueue_index);
	
	if (cur_ix >= end_ix) {
	    return hatrack_not_found_w_mmm(found);
	}

	/* Unfortunately, that means when we think there's a pretty
	 * good shot of getting a slot, we FAA the current index
	 * and re-check wrt. migration.
	 */
	cur_ix    = atomic_fetch_add(&store->dequeue_index, 1);
	
	if (cur_ix & HQ_MOVING) {
	    cur_ix &= ~HQ_MOVING;
	    
	migrate_then_possibly_dequeue:
	    
	    epoch = hq_migrate(store, self);
	    
	    if (epoch <= cur_ix) {
		store = atomic_read(&self->store);		
		continue;
	    }

	    /* If our epoch is older than the last dequeue out of this
	     * store, we're supposed to finish our operation, but if
	     * there was nothing to read due to a skipped cell, we
	     * then get to go to the new store.
	     */
	    cell     = &store->cells[hq_ix(cur_ix, sz)];
	    expected = atomic_read(cell);
	    epoch    = hq_extract_epoch(expected.state);
	    
	    if (epoch != cur_ix) {
		store = atomic_read(&self->store);		
		continue;
	    }

	    atomic_fetch_sub(&self->len, 1);	    
	    return hatrack_found_w_mmm(found, expected.item);
	}
	
	cell      = &store->cells[hq_ix(cur_ix, sz)];
	expected  = atomic_read(cell);
	epoch     = hq_extract_epoch(expected.state);

	while (epoch < cur_ix) {
	    /* We'd like to write in TOOSLOW, but we're definitely
	     * past the head, and there's data that hasn't been
	     * dequeued, so declare not found.
	     */
	    if(hq_is_queued(expected.state)) {
		return hatrack_not_found_w_mmm(found);
	    }
	    
	    candidate.state = HQ_TOOSLOW | cur_ix;

	    /* If we were right next to the head pointer, assume a
	     * miss.
	     */
	    if (CAS(cell, &expected, candidate)) {
		if ((cur_ix + 1) == end_ix) {
		    return hatrack_not_found_w_mmm(found);			
		}
		goto retry_dequeue;
	    }
	    epoch = hq_extract_epoch(expected.state);
	}

	if (epoch > cur_ix) {
	    /* We failed to read out of the slot, and an enqueuer put
	     * something new here. That indicates the cell was
	     * previously skipped, otherwise the enqueuer would have
	     * triggered a resize.  There's probably a resize in
	     * progress, but let's make sure, and then restart the op.
	     */

	    hq_migrate(store, self);
	    store = atomic_read(&self->store);
	    continue;
	}
	
	if (hq_is_moving(expected.state)) {
	    goto migrate_then_possibly_dequeue;
	}
	
	ret             = expected.item;
	candidate.state = cur_ix;
	
	if (!CAS(cell, &expected, candidate)) {
	    // This should only occur if a migration is being triggered.
	    goto migrate_then_possibly_dequeue;
	}

	atomic_fetch_sub(&self->len, 1);
	return hatrack_found_w_mmm(found, ret);
    }
}

hq_view_t *
hq_view(hq_t *self)
{
    hq_view_t  *ret;
    hq_store_t *store;
    bool        expected;

    mmm_start_basic_op();

    ret = (hq_view_t *)malloc(sizeof(hq_view_t));

    while (true) {
	store    = atomic_read(&self->store);
	expected = false;
	
	if (CAS(&store->claimed, &expected, true)) {
	    break;
	}
	hq_migrate(store, self);
    }

    ret->start_epoch = hq_migrate(store, self);
    
    mmm_end_op();

    ret->store      = store;
    ret->next_ix    = ret->start_epoch;
    ret->last_epoch = ret->start_epoch + store->size;

    return ret;
}

void *
hq_view_next(hq_view_t *view, bool *found)
{
    hq_item_t item;
    uint64_t  ix;

    while (true) {
	if (view->next_ix >= view->last_epoch) {
	    return hatrack_not_found(found);
	}

	ix   = hq_ix(view->next_ix++, view->store->size);
	item = atomic_read(&view->store->cells[ix]);

	if (hq_is_queued(item.state)) {
	    
	    if (hq_extract_epoch(item.state) < view->start_epoch) {
		continue;
	    }
	    
	    return hatrack_found(found, item.item);
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

static uint64_t
hq_migrate(hq_store_t *store, hq_t *top)
{
    hq_store_t *next_store;
    hq_store_t *expected_store;
    hq_item_t   expected_item;
    hq_item_t   candidate_item;
    hq_item_t   old_item;
    uint64_t    i, n;
    uint64_t    highest;
    uint64_t    lowest;
    uint64_t    epoch;


    atomic_fetch_or_explicit(&store->dequeue_index,
    			     HQ_MOVING,
    			     memory_order_relaxed);
				     
    highest = 0;
    
    for (i = 0; i < store->size; i++) {
	expected_item = atomic_read(&store->cells[i]);

	if (hq_is_queued(expected_item.state)) {
	    epoch = hq_extract_epoch(expected_item.state);
	
	    if (epoch > highest) {
		highest = epoch;
	    }
	}

	if (hq_is_moving(expected_item.state)) {
	    continue;
	}
	
	if (!hq_is_queued(expected_item.state)) {
	    atomic_fetch_or_explicit((_Atomic __uint128_t *)&store->cells[i],
				     moved_cell.num,
				     memory_order_relaxed);
	}
	else {
	    atomic_fetch_or_explicit((_Atomic __uint128_t *)&store->cells[i],
				     moving_cell.num,
				     memory_order_relaxed);
	}
    }

    n      = highest;
    lowest = (highest - store->size); // Anything lower than this is a skip.
    

    // When starting at the highest epoch, the lowest non-skipped
    // but non-queued epoch becomes the new value for 'lowest'.
    for (i = 0; i < store->size; i++) {
	expected_item = atomic_read(&store->cells[hq_ix(--n, store->size)]);
	
	if (hq_is_queued(expected_item.state)) {
	    continue;
	}
	
	epoch = hq_extract_epoch(expected_item.state);

	if (epoch < lowest) {
	    continue;
	}

	lowest = epoch;
	break;
    }

    expected_store = NULL;
    next_store     = hq_new_store(store->size << 1);

    atomic_store(&next_store->enqueue_index, HQ_STORE_INITIALIZING);
    atomic_store(&next_store->dequeue_index, HQ_STORE_INITIALIZING);    

    if (!CAS(&store->next_store, &expected_store, next_store)) {
	mmm_retire_unused(next_store);
	next_store = expected_store;
    }

    i = lowest;
    n = 0;

    for (i = lowest; i <= highest; i++) {
	old_item = atomic_read(&store->cells[hq_ix(i, store->size)]);
	
	if (hq_is_moved(old_item.state)) {
	    if (hq_is_queued(old_item.state)) {
		n++;
	    }
	    continue;
	}

	/* We might have something in the range of lowest-highest
	 * That is actually enqueued because of a slow dequeuer.  Don't
	 * copy such things.
	 */
	
	if (hq_extract_epoch(old_item.state) < lowest) {
	    continue;
	}
	
	expected_item        = empty_cell;
	candidate_item.item  = old_item.item;
	candidate_item.state = hq_set_used(n + next_store->size);
	CAS(&next_store->cells[n++], &expected_item, candidate_item);

	atomic_fetch_or((_Atomic __uint128_t *)
			&store->cells[hq_ix(i, store->size)],
			 moved_cell.num);
    }

    i = HQ_STORE_INITIALIZING;
    CAS(&next_store->dequeue_index, &i, 0 + next_store->size);
    i = HQ_STORE_INITIALIZING;
    CAS(&next_store->enqueue_index, &i, n + next_store->size);

    if (CAS(&top->store, &store, next_store)) {
	if (!store->claimed) {
	    mmm_retire(store);
	}
    }
    
    return lowest;
}
