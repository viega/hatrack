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

typedef union {
    capq_item_t cell;
    __uint128_t num;
} unholy_u;

static const unholy_u moving_cell = { .cell = { NULL, CAPQ_MOVING } };
static const unholy_u moved_cell  = { .cell = { NULL, CAPQ_MOVING | CAPQ_MOVED } };



static capq_store_t *capq_new_store(uint64_t);
static void          capq_migrate  (capq_store_t *, capq_t *);

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
    self->len           = 0;
    
    self->store->dequeue_index = 1L<<32;
    self->store->enqueue_index = 1L<<32;

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
    uint64_t      epoch;
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

	    /* We are going to write in the buffer in a circular
	     * fashion until it seems to be full. However, the
	     * enqueue_index and dequeue_index fields are not absolute
	     * indicies into the underlying array, they are
	     * essentially epoch values, and we will effectively take
	     * the epoch modulo the backing store size to get the
	     * index (via the macro capq_ix()).
	     *
	     * Given that, we test whether we're approximately full by
	     * adding the size to the dequeue epoch. If this value is
	     * larger than (or equal to) the enqueue epoch, the write
	     * is nominally safe.  Otherwise, we need to resize.
	     *
	     * Do note that, if enqueues and dequeues are both
	     * happening quickly, we might encounter one of these
	     * problems when we try to write:
	     *
	     * 1) If a dequeue or top operation gets to this slot
	     *    before we finish writing, it will invalidate this
	     *    cell; we might see our epoch set, but no item
	     *    enqueued, and no item dequeued.
	     *
             * 2) When we're REALLY slow, not only may our slot be
             *    invalidated, but another enqueuer may have since
             *    been assigned the slot, and written to it, in which
	     *    case we will see some future epoch.
	     *
	     * 3) Before we finish our write, some thread may have
	     * determined that the backing store needs to grow, and
	     * may have set CAPQ_MOVING.
	     *
	     * In all these cases, we will retry our enqueue
	     * operation.  In the first two cases, we double the value
	     * of 'step' to make sure our enqueue stays wait free.
	     *
	     * In the third case, we first help with the migration
	     * before we retry.
	     */
	    
	    max = end_ix + sz;
	    
	    if (cur_ix >= max) {
		break;
	    }

	    cell            = &store->cells[capq_ix(cur_ix, sz)];
	    expected        = atomic_read(cell);
	    candidate.state = capq_set_enqueued(cur_ix);

	    if (capq_is_moving(expected.state)) {
		break;
	    }
	    
	    /* On a successful write, *we* should be writing our
	     * epoch into this cell. If it's already there, then
	     * a dequeuer invalidated us.
	     *
	     * If the epoch is above ours, then we were really slow.
	     *
	     * Either way, we need to try again.
	     */
	    
	    epoch = capq_extract_epoch(expected.state);

	    if (epoch >= cur_ix) {
		step <<= 1;
		continue;
	    }

	    if (CAS(cell, &expected, candidate)) {
		atomic_fetch_add(&self->len, 1);
		mmm_end_op();

		return cur_ix;
	    }

	    if (capq_is_moving(expected.state)) {
		break;
	    }

	    // Otherwise, we got invalidated.
	    step <<= 1;
	    continue;
	}
	
	capq_migrate(store, self);
    }
}

/*
 * The basic idea here is to get the current value of the dequeue
 * index, and return the value that's stored there. However, a lot of
 * things can go wrong for us, here:
 *
 * 1. The queue might be empty at the point where we attempt to look
 *    at the top.
 *
 * 2. The slot we look in might have been skipped, due to an enqueue
 *    operation needing to skip slots (part of making the queue wait
 *    free).
 *
 * 3. We might end up very slow to read, causing us to see a dequeued
 *    item.
 *
 * 4. We might end up very slow to read, causing us to see an item
 *    that was enqueued later, which probably isn't a valid,
 *    linearizable item to return, yet. 
 *
 * 5. The slot we look in might not yet be written to, due to a slow
 *    writer.
 *
 *
 * Since the primary purpose of the CAPQ is to support building other
 * wait-free data structures, we want to support a bounded number of
 * retries, ideally with a small bound. And, we want to be able to
 * maintain a proper linearization, so we can show correctness (so we
 * cannot return as if we're empty if there are actually items in the
 * queue, but we're just having a hard time getting the top).
 *
 * For #1, we can easily tell when the queue seems to be empty before
 * we go look at a cell, and just bail.
 *
 * For #2, we expect a bounded number of skips, so this is no problem.
 *
 * For #3, we count the number of times we fail because we were too
 * slow. If we hit CAPQ_TOP_THRESHOLD, then we return the first valid
 * DEQUEUED value we see, which has the effect of linearizing the
 * top() operation BEFORE the competing dequeue.
 *
 * The caller will never be able to tell it got stale data, but it
 * does guarantee that any subsequent cap() operation will fail.
 *
 * Case #4 is similar to case #3, except that we cannot know if there
 * was a valid dequeued value.
 *
 * Note that we could have such bad luck that we always find ourselves
 * waking up to new epochs.  As a result, if we're above our retry
 * threshold, we'll kick off a migration to make sure we stay wait
 * free.
 *
 * For case #5, We attempt to invalidate cells is they're not yet
 * written, and then try again. This starves writers, not readers; we
 * can return 'empty' if we happen to know that there was some point
 * where the queue was empty, otherwise we can keep scanning.  We'll
 * eventually find something written, or we'll eventually get down to
 * one contending writer, where we can know that the queue is empty on
 * contention. When we see written, yet dequeued items, we take the
 * same strategy as with #3, thus ensuring that we keep a small bound.
 *
 * Note that capq_top() being a read-only operation, it can ignore
 * migrations; even if we're slow, it's fine for us to linearize to
 * the moment before the migration completes.
 */
capq_top_t
capq_top(capq_t *self, bool *found)
{
    capq_store_t *store;
    uint64_t      cur_ix;
    uint64_t      end_ix;
    uint64_t      candidate_ix;
    uint64_t      sz;
    uint64_t      suspension_retries;
    uint64_t      epoch;
    capq_cell_t  *cell;
    capq_item_t   item;
    capq_item_t   marker;
    
    mmm_start_basic_op();

    suspension_retries = 0;
    store              = atomic_read(&self->store);
    sz                 = store->size;
    cur_ix             = atomic_load(&store->dequeue_index);
    end_ix             = atomic_load(&store->enqueue_index);
    
    while (cur_ix < end_ix) {
	cell  = &store->cells[capq_ix(cur_ix, sz)];
	item  = atomic_read(cell);
	epoch = capq_extract_epoch(item.state);

	/* First let's look at the case where the epoch is as we
	 * expect it to be.  If the item was queued at some point
	 * since we started our operation, we can return the item,
	 * linearizing to the read of the dequeue index.
	 *
	 * Otherwise, the epoch would only be correct if some other
	 * thread successfully invalidated this cell, so we move on,
	 * after trying to 'help' swing the dequeue index (just in
	 * case a thread got stalled after invalidating but before the
	 * bump occurred).
	 */
	if (epoch == cur_ix) {
	    if (capq_should_return(item.state)) {
	    found_item:
		// This zeros out all flags in the state field.
		item.state = capq_extract_epoch(item.state);

		if (found) {
		    *found = true;
		}

		/* If we chose to return a dequeued item, we should
		 * still attempt to swing forward the dequeue index.
		 */
		if (capq_is_dequeued(item.state)) {
		    CAS(&store->dequeue_index, &cur_ix, cur_ix + 1);
		}
		
		mmm_end_op();
		
		return item;
	    }

	    next_slot:
		candidate_ix = cur_ix + 1;
		if (CAS(&store->dequeue_index, &cur_ix, candidate_ix)) {
		    cur_ix = candidate_ix;
		}
		continue; 
	}

	/* If the epoch is smaller than our epoch (and we are not
	 * migrating), then there are three possibilities:
	 *
	 * 1. There was a migration, and the epoch read constitutes a
	 * REAL epoch.  Here, if the item is listed as ENQUEUED or
	 * DEQUEUED, then we know it's still the valid next item to
	 * return.
	 *
	 * 2. There was contention, and some enqueuer skipped the
	 * enqueue index past this cell.
	 *
	 * 3. There is a slow writer, who has not yet written to this
	 * cell.
	 *
	 * In cases 2 and 3, the cell will NOT have the dequeue flag
	 * set, but we won't necessarily be able to differentiate
	 * between these two cases.  Therefore, if the item isn't
	 * enqueued, we make an attempt to invalidate the cell. If we
	 * succeed, we can move on to the next slot (jump to the
	 * next_slot target above). If we fail to invalidate the cell,
	 * then we should try the loop again w/o trying to swing the
	 * pointer, because there's some chance that a slow writer
	 * still managed to beat us, and the correct value is in this
	 * cell.
	 */
	if (capq_extract_epoch(item.state) < cur_ix) {

	    if (capq_should_return(item.state)) {
		goto found_item;
	    }

	    if (capq_is_moving(item.state)) {
		/* If we're migrating and the cell we're looking at
		 * hasn't been written to yet, we don't need to
		 * invalidate the cell; it's implicitly
		 * invalid. However, since we're not modifying the
		 * queue, we get to keep searching, by going to the
		 * next slot.
		 */
		cur_ix++;
		continue;
	    }
		
	    marker.item  = NULL;
	    marker.state = cur_ix; // Neither enqueued nor dequeued flags set.

	    if (CAS(cell, &item, marker)) {
		goto next_slot;
	    }
	    else {
		/* 
		 * Someone beat our write. It could have been another
		 * thread invalidating this slot, or it could have
		 * been a successful write. Re-try the loop, without
		 * bumping the value cur_ix, so we can see what's up!
		 */
		continue;
	    }
	}

	/* If we're here, the read epoch was AHEAD of the epoch we
	 * were expecting, and so we were definitely too slow, and the
	 * tail will have already been moved. There's no valid data
	 * stored in the cell anymore, so we can only bump up
	 * suspension_retries, grow the array if the number of retries
	 * is too high, and then start over, re-reading cur_ix and end_ix;
	 */

	if (!(++suspension_retries % CAPQ_TOP_SUSPEND_THRESHOLD)) {
	    capq_migrate(store, self);
	    store = atomic_read(&self->store);
	}

	cur_ix = atomic_load(&store->dequeue_index);
	end_ix = atomic_load(&store->enqueue_index);
	
	continue;
	
    }

    // If we exit the loop, the store was empty at some point.
    if (found) {
	*found = false;
    }

    mmm_end_op();
    
    return empty_cell;
}

/*
 * Our compare-and-pop operator has to worry about far fewer issues
 * than the top() operation. First, we already have an epoch that is
 * expected to be the 'top'. If we look at the dequeue index, and it's
 * not the same, then the CAP couldn't possibly succeed. If it is the
 * same, we just need to:
 *
 * 1) Load the cell to recover the item and flags.
 * 2) Make sure the loaded copy is still considered enqueued at the time we 
 *    loaded it.
 * 3) Swap it out for a version that indicates 'dequeued'.
 *
 * If all three of these things succeed, then our CAP succeeds. If
 * anything goes wrong, it is either because of a migration (in which
 * case we retry after the migration), or it's because the item is
 * already dequeued, in which case it's a fail.
 */
bool
capq_cap(capq_t *self, uint64_t epoch)
{
    capq_store_t *store;
    uint64_t      cur_ix;
    uint64_t      candidate_ix;
    uint64_t      sz;    
    capq_cell_t  *cell;
    capq_item_t   expected;
    capq_item_t   candidate;
    
    mmm_start_basic_op();

    store = atomic_read(&self->store);

    while (true) {
	sz       = store->size;
	cur_ix   = atomic_read(&store->dequeue_index);
	cell     = &store->cells[capq_ix(cur_ix, sz)];
	expected = atomic_read(cell);

	/* We can't compare against cur_ix because in a migration, the
	 * migrated cells will have values that are different than
	 * cur_ix-- they don't get re-written, partially because a
	 * migration can happen while a cap() is in progress, and we
	 * don't want to have to deal w/ the input epoch having to be
	 * remapped.
	 */
	if (capq_extract_epoch(expected.state) != epoch) {
	    mmm_end_op();
	    return false;
	}
	
	// If we were really slow to load, the epoch changed.
	if (capq_extract_epoch(expected.state) != epoch) {
	    mmm_end_op();	    
	    return false;
	}

	/* Equally, we could check capq_is_dequeued(), as we know
	 * if top() returned an epoch, then that epoch was enqueued.
	 */
	if (!capq_is_enqueued(expected.state)) {
	    mmm_end_op();	    
	    return false;
	}

	if (capq_is_moving(expected.state)) {
	    capq_migrate(store, self);
	    store = atomic_read(&self->store);
	    continue;
	}

	candidate.item  = expected.item;
	candidate.state = capq_set_state_dequeued(expected.state);
	    
	if (CAS(cell, &expected, candidate)) {
	    /* We don't need to bump the tail; the next call to
	     * capq_top() will do it for us.  However, let's do it
	     * anyway, just to avoid unnecessary retries in top().
	     */
	    candidate_ix = cur_ix + 1;
	    CAS(&store->dequeue_index, &cur_ix, candidate_ix);
	    
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

/* 
 * Note that this is a lock-free implementation, built on top of
 * capq_top() and capq_cap().
 *
 * We could build a more efficient lock-free version of this easily
 * using pieces of both those functions. And, with some extra work, we
 * could make a wait-free version of this call.
 *
 * However, we don't expect capq to be used as a general-purpose
 * queue; the point of the thing is to use compare-and-pop to dequeue.
 * This is here primarily to hook into our test harness.
 */
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

/* 
 * We start marking at the beginning of the backing store, not at the
 * dequeue pointer, since that is potentially a moving target.  
 *
 * As we lock cells, we'll note the epochs we see of enqueued items,
 * as well as the index in which we found those items, and when we're
 * done, all threads should be able to agree on the starting index for
 * migration.
 *
 * Note that we have to compare total lifetime using the epoch, but
 * record the associated INDEX because it's possible to have multiple
 * migrations without any dequeuing, so the epoch won't necessarily
 * reduce to the slot in which it's stored.
 *
 * As we migrate, we compact items (particularly, where there were
 * skips). The new 'epoch' we give out (via the enqueue pointer) is a
 * value that's definitely higher than any old epoch, but is aligned
 * to the slot.
 *
 * The dequeue pointer, really only strictly needs to point to the
 * slot, and does not need to represent an epoch. However, our logic
 * for top() is more straightforward if we keep the dequeue index
 * aligned to the epoch whenever possible. Therefore, when we're
 * finishing up a migration, we set the dequeue pointer to whatever
 * value we *would* have given out for the current slot, had we always
 * lived in this store, without migrating.
 *
 * The top() operation will behave okay when this value is higher than
 * the extracted epoch, because CAPQ_ENQUEUED will still be set.  And
 * once the pointer starts pointing at items that were enqueued
 * directly into the new backing store, everything will be right with
 * the cosmos.
 */
static void
capq_migrate(capq_store_t *store, capq_t *top)
{
    capq_store_t *next_store;
    capq_store_t *expected_store;
    capq_item_t   expected_item;
    capq_item_t   candidate_item;
    capq_item_t   old_item;
    uint64_t      i, n;
    uint64_t      lowest_ix;
    uint64_t      lowest_epoch;
    uint64_t      new_dqi;  // The new store's start value for dequeue_index.
    uint64_t      epoch;
    uint64_t      num_items;
    unholy_u      u;

    num_items     = 0;
    lowest_ix     = 0;
    lowest_epoch  = 0xffffffffffffffff;

    // Phase 1: mark for move, and search for the first cell to migrate.
    for (i = 0; i < store->size; i++) {
	u.num = atomic_fetch_or_explicit((_Atomic __uint128_t *)&store->cells[i],
					 moving_cell.num,
					 memory_order_relaxed);

	expected_item = u.cell;

	if (!capq_is_enqueued(expected_item.state)) {
	    atomic_fetch_or_explicit((_Atomic __uint128_t *)&store->cells[i],
				     moved_cell.num,
				     memory_order_relaxed);
	    continue;
	}

	num_items++;
	
	epoch = capq_extract_epoch(expected_item.state);
	
	if (epoch < lowest_epoch) {
	    lowest_epoch = epoch;
	    lowest_ix    = i;
	}
    }

    // Phase 2: agree on the new store.
    expected_store = NULL;
    next_store     = capq_new_store(store->size << 1);

    atomic_store(&next_store->enqueue_index, CAPQ_STORE_INITIALIZING);
    atomic_store(&next_store->dequeue_index, CAPQ_STORE_INITIALIZING);

    if (!CAS(&store->next_store, &expected_store, next_store)) {
	mmm_retire_unused(next_store);
	next_store = expected_store;
    }

    /*
     * Phase 3: migrate enqueued cells, while compacting.
     *
     * Note that we want to loop from the index associated with the
     * lowest enqueued epoch (lowest_ix), until we have found all
     * enqueued items.  We do this by visiting every index in order
     * until n equals num_items, where n counts the number of items
     * we have migrated successfully.
     */
    n = 0;
    
    for (i = lowest_ix; n < num_items; i = capq_ix(i+1, store->size)) {
	old_item = atomic_read(&store->cells[i]);

	if (!capq_is_enqueued(old_item.state)) {
	    continue;
	}

	if (capq_is_moved(old_item.state)) {
	    n++;
	    continue;
	}

	expected_item        = empty_cell;
	candidate_item.item  = old_item.item;
	candidate_item.state = capq_clear_moving(old_item.state);

	CAS(&next_store->cells[n++], &expected_item, candidate_item);

	atomic_fetch_or((_Atomic __uint128_t *)
			&store->cells[capq_ix(i, store->size)],
			 moved_cell.num);
    }

    // Phase 4: Install the new store.
    i       = CAPQ_STORE_INITIALIZING;
    new_dqi = (store->dequeue_index + 0x0000000100000000) & 0xffffffff00000000;
	
    CAS(&next_store->dequeue_index, &i, new_dqi);

    i       = CAPQ_STORE_INITIALIZING;
    
    CAS(&next_store->enqueue_index, &i, new_dqi | n);

    if (CAS(&top->store, &store, next_store)) {
	mmm_retire(store);
    }

    return;
}

