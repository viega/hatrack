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
 *  Name:           stack.c
 *  Description:    A faster stack implementation that avoids
 *                  using a linked list node for each item.
 *
 *                  Note that, if pops start, and have a push start
 *                  before the pop completes, or an pop end before us,
 *                  the stack can temporarily end up with dead
 *                  space... popped items below the head of the stack.
 *                  Pops therefore need to take dead space into
 *                  account, and walk down the stack when they see it,
 *                  until they find something to pop.
 * 
 *                  Note that I originally had the ability to do
 *                  as-you-go compression if a reader ends up walking
 *                  too far down to pop, and new pushes come in ahead
 *                  of him.  I was surprised to find that, even with a
 *                  high pusher to popper ratio, this is sigificantly
 *                  faster if we just wait to compress out popped
 *                  cells until we run out of space in the array.
 *
 *                  What ended up happening was multiple "gaps" that
 *                  could be compressed would show up in parallel, and
 *                  I'd only compress the top one.  So I was making
 *                  more threads do more work because compression is
 *                  more complicated, while only modestly lowering the
 *                  number of migrations I'd ultimately have to do.
 *
 *                  So instead of only compressing the top of the
 *                  stack, and doing it relatively frequently, I now
 *                  just wait till you run out of space, and then
 *                  compress the whole thing.
 *
 *                  I could use the compression algorithm I was using
 *                  for this, but instead I do the following:
 *
 *                  1) If, after compression, the stack would be more
 *                     than 1/2 full, I double the size of the stack,
 *                     and compress into the new store, using the same
 *                     algorithm I use for migrating hash tables.
 *
 *                  2) If, after compression, the stack would be 1/2
 *                     full or less, instead of compressing in place,
 *                     I create a new store of the same size, and 
 *                     compress into that.
 *
 *                  When we don't NEED to grow, we could definitely
 *                  avoid another memory allocation, and possibly some
 *                  copying.  However, I am pretty sure the extra
 *                  effort to coordinate an in-place compression isn't
 *                  worth it; better just to copy.  I may test this at
 *                  some point.
 *
 *                  In the meantime, I have stashed away a version
 *                  that does in-place compression so I can re-visit
 *                  it later.
 *
 *                  Currently, this algorithm is lock-free; Pushes
 *                  might need to retry if a pop invalidates their
 *                  cell, and that could happen continually.
 *
 *                  We could easily address this with a "help"
 *                  facility that keeps pops from asking pushes to
 *                  retry (and thus from moving the head pointer when
 *                  done), which risks increasing compressions.  We'll
 *                  try that out soon.
 *
 *  Author:         John Viega, john@zork.org
 */

#include <hatrack.h>

static const stack_item_t proto_item_empty = {
    .item        = NULL,
    .state       = 0,
    .valid_after = 0
};

static const stack_item_t proto_item_pop = {
    .item        = NULL,
    .state       = HATSTACK_POPPED,
    .valid_after = 0
};

// For migrations.
static const stack_item_t proto_item_pushed = {
    .item        = NULL,
    .state       = HATSTACK_PUSHED,
    .valid_after = 0
};

// clang-format off
static stack_store_t *hatstack_new_store (uint64_t);
static stack_store_t *hatstack_grow_store(stack_store_t *, hatstack_t *);
// clang-format on

hatstack_t *
hatstack_new(uint64_t prealloc)
{
    hatstack_t *ret;

    ret = (hatstack_t *)malloc(sizeof(hatstack_t));

    hatstack_init(ret, prealloc);

    return ret;
}

void
hatstack_init(hatstack_t *self, uint64_t prealloc)
{
    prealloc = hatrack_round_up_to_power_of_2(prealloc);

    atomic_store(&self->store, hatstack_new_store(prealloc));
    
    return;
}

void
hatstack_cleanup(hatstack_t *self)
{
    mmm_retire_unused(atomic_read(&self->store));

    return;
}

void
hatstack_delete(hatstack_t *self)
{
    hatstack_cleanup(self);

    free(self);

    return;
}

void
hatstack_push(hatstack_t *self, void *item)
{
    stack_store_t *store;
    uint64_t       head_state;
    uint32_t       ix;
    uint32_t       epoch;
    stack_item_t   candidate;
    stack_item_t   expected;

#ifdef HATSTACK_WAIT_FREE
    uint32_t       retries = 0;
#endif    
    
    candidate      = proto_item_pushed;
    candidate.item = item;
    
    mmm_start_basic_op();

    store = atomic_read(&self->store);

    while (true) {
	head_state = atomic_fetch_add(&store->head_state, 1);

	if (head_is_moving(head_state, store->num_cells)) {
	    store = hatstack_grow_store(store, self);
	    continue;
	}
	
	epoch                 = head_get_epoch(head_state);
	ix                    = head_get_index(head_state);
	expected              = proto_item_empty;
	candidate.valid_after = epoch - 1;

	if (CAS(&store->cells[ix], &expected, candidate)) {
	    mmm_end_op();

#ifdef HATSTACK_WAIT_FREE
	    if (retries >= HATSTACK_RETRY_THRESHOLD) {
		atomic_fetch_sub(&self->push_help_shift,
				 retries / HATSTACK_RETRY_THRESHOLD);
	    }
#endif	    
	    
	    return;
	}

	if (state_is_moving(expected.state)) {
	    store = hatstack_grow_store(store, self);
	    continue;
	}
	
	if (!cell_can_push(expected, epoch)) {
	    /* Once we see that a cell's been invalidated, we want to
	     * remove the invalidated flag before we move on.  That
	     * way, if the array successfully shrinks, the bucket can
	     * be reused, by removing the invalidated flag.  But,
	     * remember, we can be slow, so we need to make sure the
	     * 'invalid' flag was meant for us, not for some future
	     * thread that's also assigned this bucket.
	     */
	    continue;
	}

	// Usually this will be uncontested, and if so, we are done.
	if (CAS(&store->cells[ix], &expected, candidate)) {
	    mmm_end_op();

#ifdef HATSTACK_WAIT_FREE
	    if (retries >= HATSTACK_RETRY_THRESHOLD) {
		atomic_fetch_sub(&self->push_help_shift,
				 retries / HATSTACK_RETRY_THRESHOLD);
	    }
#endif	    
	    return;
	}

	/* If we couldn't CAS our item in, we are competing with one
	 * of two fairly rare things:
	 *
	 * 1) A grow operation.
	 * 2) A faster pop operation that invalidated our bucket.
	 *
	 * Whatever the case, we head back up to the top for another
	 * go.
	 */
#ifdef HATSTACK_WAIT_FREE
	if (!(++retries % HATSTACK_RETRY_THRESHOLD)) {
	    atomic_fetch_add(&self->push_help_shift, 1);
	}
#endif	
	continue;
    }
}

void *
hatstack_pop(hatstack_t *self, bool *found)
{
    stack_store_t *store;
    stack_item_t   expected;
    stack_item_t   candidate;
    uint64_t       head_state;
    uint64_t       head_candidate;    
    uint64_t       ix;
    uint32_t       epoch;

#ifdef HATSTACK_WAIT_FREE
    uint64_t        incr;
    int64_t        wait_time;
    struct timespec sleep_time;

	/* If pushers need help pushing, we need to slow down our
	 * invalidation popping.
	 */
	wait_time = atomic_read(&self->push_help_shift);

	if (wait_time) {
	    sleep_time.tv_sec = 0;

	    incr = HATSTACK_BACKOFF_INCREMENT;
	    
	    if (wait_time > HATSTACK_MAX_BACKOFF_LOG) {
		sleep_time.tv_nsec = incr << HATSTACK_MAX_BACKOFF_LOG;
	    }
	    else {
		sleep_time.tv_nsec = incr << wait_time;
	    }

	    nanosleep(&sleep_time, NULL);
	}
#endif	
    
    mmm_start_basic_op();


    /* Iteration instead of recursion.  We'll only come back up to the top
     * loop in the case where we are forced into doing a grow operation,
     * which only happens in two cases:
     *
     * 1) There's already one in progress when we start our operation.
     *
     * 2) We cannot pop because a migration is in progress.
     */
 top_loop:
    while (true) {
	store      = atomic_read(&self->store);
	head_state = atomic_read(&store->head_state);
	candidate  = proto_item_pop;	

	if (head_is_moving(head_state, store->num_cells)) {
	    store = hatstack_grow_store(store, self);
	    continue;
	}
	
	ix                    = head_get_index(head_state);
	epoch                 = head_get_epoch(head_state);
	expected              = proto_item_empty;
	candidate.valid_after = epoch;

	/* ix points to the next push location, so if it's at 0 the
	 * stack is empty.  If we're not at 0, we substract 1.
	 */
	if (!ix) {
	    return hatrack_not_found_w_mmm(found);
	}

	ix = ix - 1;
		
	/* First, let's assume the top of the stack is clean, and that
	 * we're racing pushes.  We can use proto_item_empty for
	 * expected and blindly try to swap.  After that finally
	 * fails, when we move to new cells we should read from them
	 * before trying to swap into them, since we won't be in a
	 * great position to guess the state.
	 */
	while (CAS(&store->cells[ix], &expected, candidate)) {
	    if (ix--) {
		continue;
	    }
	    return hatrack_not_found_w_mmm(found);
	}
	
	/* Go down the stack trying to swap in pops (updating epochs
	 * where needed), until:
	 *
	 * 1) We manage to swap in a pop where there was an "old 
              enough" pushed item
	 * 2) We hit the bottom of the stack.
	 * 3) We see that the cell we're looking at is FULLY migrated 
	 *    (we can pop until the cell is fully moved).
	 *
	 * We'll start by expecting a "clean" cell that's never been
	 * written to.  As we load cells that have pops in them, we'll
	 * bet that the cell below has the same state.
	 */
	while (true) {
	    if (state_is_moving(expected.state)) {
		goto top_loop;
	    }

	    if (!state_is_pushed(expected.state)) {
		if (expected.valid_after >= epoch) {
		    // We are very slow!
		    if (ix--) {
			expected = atomic_read(&store->cells[ix]);
			continue;
		    }
		    return hatrack_not_found_w_mmm(found);
		}

		if (CAS(&store->cells[ix], &expected, candidate)) {
		    expected = atomic_read(&store->cells[ix]);
		    continue;
		}
		continue;
	    }

	    if (CAS(&store->cells[ix], &expected, candidate)) {
		// We're popping this item. Break out of the loop and
		// finish up.
		break; 
	    }
	    // Don't care much why we failed; we can move down the
	    // stack.
	    if (ix--) {
		expected = atomic_read(&store->cells[ix]);
		continue;
	    }
	    return hatrack_not_found_w_mmm(found);
	}
	
	// Now try to swing the head.
	head_candidate = head_candidate_new_epoch(head_state, ix);

	CAS(&store->head_state, &head_state, head_candidate);

	return hatrack_found_w_mmm(found, expected.item);
    }
}

/* Here we don't worry about invalidating pushers; we may end up
 * racing with poppers, but if we do, we linearize ourselves
 * conceptually to the instant immediately after the pop right in
 * front of us ended, before any push that succeeded further up the
 * stack.
 */
void *
hatstack_peek(hatstack_t *self, bool *found)
{
    stack_store_t *store;
    stack_item_t   expected;
    stack_item_t   candidate;
    uint64_t       head_state;
    uint64_t       ix;
    uint32_t       epoch;

    mmm_start_basic_op();


    store      = atomic_read(&self->store);
    head_state = atomic_read(&store->head_state);
    candidate  = proto_item_pop;	
    ix         = head_get_index(head_state);
    epoch      = head_get_epoch(head_state);
    
	expected              = proto_item_empty;
	candidate.valid_after = epoch;

	/* Go down the stack until we see any pushed cell, or we reach the
	 * bottom of the stack.
	 */
	while (ix--) {
	    if (state_is_pushed(expected.state)) {
		return hatrack_found_w_mmm(found, expected.item);
	    }
	}

	return hatrack_not_found_w_mmm(found);
}

stack_view_t *
hatstack_view(hatstack_t *self)
{
    stack_view_t  *ret;
    stack_store_t *store;
    bool           expected;

    mmm_start_basic_op();

    while (true) {
	store    = atomic_read(&self->store);
	expected = false;
	
	if (CAS(&store->claimed, &expected, true)) {
	    break;
	}
	hatstack_grow_store(store, self);
    }

    hatstack_grow_store(store, self);
    mmm_end_op();

    ret          = (stack_view_t *)malloc(sizeof(flex_view_t));
    ret->store   = store;
    ret->next_ix = 0;

    return ret;
}

void *
hatstack_view_next(stack_view_t *view, bool *found)
{
    stack_item_t item;

    while (true) {
	if (view->next_ix >= view->store->num_cells) {
	    return hatrack_not_found(found);
	}

	item = atomic_read(&view->store->cells[view->next_ix++]);

	if (state_is_pushed(item.state)) {
	    return hatrack_found(found, item.item);
	}
    }
}

void
hatstack_view_delete(stack_view_t *view)
{
    mmm_retire(view->store);

    free(view);

    return;
}

static stack_store_t *
hatstack_new_store(uint64_t num_cells)
{
    stack_store_t *ret;
    uint64_t       alloc_len;

    if (num_cells < (1 << HATSTACK_MIN_STORE_SZ_LOG)) {
	num_cells = 1 << HATSTACK_MIN_STORE_SZ_LOG;
    }
    
    alloc_len       = sizeof(stack_store_t) + num_cells * sizeof(stack_cell_t);
    ret             = (stack_store_t *)mmm_alloc_committed(alloc_len);
    ret->num_cells  = num_cells;
    ret->head_state = ATOMIC_VAR_INIT(head_candidate_new_epoch(0, 0));

    return ret;
}

/* Migration operates pretty similarly to how it's operated in our
 * other algorithms.
 *
 * 1) Mark all the buckets.
 * 2) Agree on a new store.
 * 3) Migrate the contents to the new store, marking the old
 *    buckets as fully moved as we do.
 * 4) Install the new store and clean up.
 */
static stack_store_t *
hatstack_grow_store(stack_store_t *store, hatstack_t *top)
{
    stack_store_t *next_store;
    stack_store_t *expected_store;
    stack_item_t   expected_item;
    stack_item_t   candidate_item;
    stack_item_t   old_item;
    uint64_t       head_state;
    uint64_t       target_state;
    uint64_t       i;
    uint64_t       j;

    next_store = atomic_read(&top->store);

    if (next_store != store) {
	return next_store;
    }

    next_store = atomic_read(&store->next_store);
    if (next_store) {
	goto help_move;
    }

    head_state = atomic_read(&store->head_state);
    j          = 0;

    for (i = 0; i < store->num_cells; i++) {
	expected_item = atomic_read(&store->cells[i]);
	
	while (true) {
	    if (state_is_moving(expected_item.state)) {
		break;
	    }
	    
	    if (!state_is_pushed(expected_item.state)) {
		candidate_item.item  = NULL;
		candidate_item.state = state_add_moved(expected_item.state);
	    }
	    else {
		candidate_item       = expected_item;
		candidate_item.state = state_add_moving(expected_item.state);
	    }

	    if (CAS(&store->cells[i], &expected_item, candidate_item)) {
		break;
	    }
	}
	if (state_is_pushed(expected_item.state)) {
	    j++;
	}
    }

    expected_store = NULL;

    if (j < (store->num_cells >> 1)) {
	next_store     = hatstack_new_store(store->num_cells);	
    }
    else {
	next_store     = hatstack_new_store(store->num_cells << 1);
    }


    /* This is just to make sure threads know for sure whether
     * num_cells has been initialized, since a stack could legitimately
     * have 0 items. 
     */
    atomic_store(&next_store->head_state, HATSTACK_HEAD_INITIALIZING);
    
    if (!CAS(&store->next_store, &expected_store, next_store)) {
	mmm_retire_unused(next_store);
	next_store = expected_store;
    }

 help_move:
    for (i = 0, j = 0; i < store->num_cells; i++) {
	old_item = atomic_read(&store->cells[i]);
	
	if (state_is_moved(old_item.state)) {
	    if (state_is_pushed(old_item.state)) {
		j++;
	    }
	    continue;
	}

	expected_item       = proto_item_empty;
	candidate_item      = proto_item_pushed;
	candidate_item.item = old_item.item;
	
	CAS(&next_store->cells[j++], &expected_item, candidate_item);

	candidate_item       = old_item;
	candidate_item.state = state_add_moved(old_item.state);
	
	CAS(&store->cells[i], &old_item, candidate_item);
    }

    /* Install head_state.  The new index will be j; nothing else should
     * be set.  0 is the right compression ID, and we don't want
     * either of the status bits set when we're done.
     */
    target_state = HATSTACK_HEAD_INITIALIZING;
    head_state   = j;
    

    CAS(&next_store->head_state, &target_state, head_state);

    /* Finally, install the new store, opening the world back up for
     * pushes and pops.  Any late ops to the old store will still see
     * our state as "migrating", but will either quickly figure out
     * that the store has moved, or will go through the motions and do
     * no work, because every local cell is marked as moved.
     */
    if (CAS(&top->store, &store, next_store)) {
	if (!store->claimed) {
	    mmm_retire(store);
	}
    }

    return next_store;
}
