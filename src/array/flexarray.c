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
 *  Name:           flexarray.c
 *  Description:    A fast, wait-free flex array.
 *
 *                  This ONLY allows indexing and resizing the array.
 *                  If you need append/pop operations in addition, see
 *                  the vector_t type.
 *
 *  Author:         John Viega, john@zork.org
 */

#include <hatrack.h>

static flex_store_t *flexarray_new_store(uint64_t, uint64_t);
static void          flexarray_migrate(flex_store_t *, flexarray_t *);
    
/* The size parameter is the one larger than the largest allowable index.
 * The underlying store may be bigger-- it will be sized up to the next
 * power of two.
 */
flexarray_t *
flexarray_new(uint64_t initial_size)
{
    flexarray_t *arr;

    arr = (flexarray_t *)malloc(sizeof(flexarray_t));
    
    flexarray_init(arr, initial_size);
    
    return arr;
}

void
flexarray_init(flexarray_t *arr, uint64_t initial_size)
{
    uint64_t store_size;
    
    arr->ret_callback   = NULL;
    arr->eject_callback = NULL;
    store_size          = hatrack_round_up_to_power_of_2(initial_size);

    if (store_size < (1 << FLEXARRAY_MIN_STORE_SZ_LOG)) {
	store_size = 1 << FLEXARRAY_MIN_STORE_SZ_LOG;
    }
    
    atomic_store(&arr->store, flexarray_new_store(initial_size, store_size));

    return;
}

void
flexarray_set_ret_callback(flexarray_t *self, flex_callback_t callback)
{
    self->ret_callback = callback;

    return;
}

void
flexarray_set_eject_callback(flexarray_t *self, flex_callback_t callback)
{
    self->eject_callback = callback;
}

void
flexarray_cleanup(flexarray_t *self)
{
    flex_store_t *store;
    uint64_t    i;
    flex_item_t item;

    store = atomic_load(&self->store);
    
    if (self->eject_callback) {
	for (i = 0; i < store->array_size; i++) {
	    item = atomic_read(&store->cells[i]);
	    if (item.state) {
		(*self->eject_callback)(item.item);
	    }
	}
    }

    mmm_retire_unused(self->store);

    return;
}

void
flexarray_delete(flexarray_t *self)
{
    flexarray_cleanup(self);
    free(self);

    return;
}

void *
flexarray_get(flexarray_t *self, uint64_t index, int *status)
{
    flex_item_t   current;
    flex_store_t *store;
    
    mmm_start_basic_op();

    store = atomic_read(&self->store);
    
    if (index >= atomic_read(&store->array_size)) {
	if (status) {
	    *status = FLEX_OOB;
	}
	return NULL;
    }
	
    current = atomic_read(&store->cells[index]);
    
    if (!(current.state & FLEX_ARRAY_USED)) {
	if (status) {
	    *status = FLEX_UNINITIALIZED;
	}
	return NULL;
    }
    
    if (self->ret_callback && current.item) {
	(*self->ret_callback)(current.item);
    }
    
    mmm_end_op();

    if (status) {
	*status = FLEX_OK;
    }
    
    return current.item;
}

// Returns true if successful, false if write would be out-of-bounds.
bool
flexarray_set(flexarray_t *self, uint64_t index, void *item)
{
    flex_store_t *store;
    flex_item_t   current;
    flex_item_t   candidate;
    flex_cell_t  *cellptr;
    
    mmm_start_basic_op();
    
    store = atomic_read(&self->store);

    if (index >= atomic_read(&store->array_size)) {
	return false;
    }

    cellptr = &store->cells[index];
    current = atomic_read(cellptr);

    if (current.state & FLEX_ARRAY_MOVING) {
	flexarray_migrate(store, self);
	mmm_end_op();
	return flexarray_set(self, index, item);
    }

    candidate.item  = item;
    candidate.state = FLEX_ARRAY_USED;

    if (CAS(cellptr, &current, candidate)) {
	if (self->eject_callback && current.state == FLEX_ARRAY_USED) {
	    (*self->eject_callback)(current.item);
	}
	mmm_end_op();
	return true;
    }

    if (current.state & FLEX_ARRAY_MOVING) {
	flexarray_migrate(store, self);
	mmm_end_op();	
	return flexarray_set(self, index, item);
    }

    /* Otherwise, someone beat us to the CAS, but we sequence ourselves
     * BEFORE the CAS operation (i.e., we got overwritten).
     */

    if (self->eject_callback) {
	(*self->eject_callback)(item);
    }
    
    mmm_end_op();
    return true;
}

void
flexarray_set_size(flexarray_t *self, uint64_t index)
{
    flex_store_t *store;
    flex_next_t   next_candidate;
    flex_next_t   next_expected;
    uint64_t      array_size;
    bool          must_retry;

    mmm_start_basic_op();

    store      = atomic_read(&self->store);
    array_size = atomic_read(&store->array_size);

    /* The 'easy' path is if our store is large enough to handle the
     * resize, but we're resizing UP.  In that case, we can just
     * bump up store->array_size and be done.
     *
     * It's possible that shrink ops could come in, in parallel.
     * That's okay; fast-path grows will order before any shrink.
     */
    if (index >= array_size && index <= store->store_size) {
	while (array_size < index) {
	    if(CAS(&store->array_size, &array_size, index)) {
		mmm_end_op();
		return;
	    }
	}
    }

    /* Any time we need more memory, we have to migrate to a new
     * store.  However, we do the same thing if we are SHRINKING a
     * store, partially to make it easy to convince ourselves of the
     * linearization point for each operation.
     *
     * Note that there could be multiple set-size operations in
     * parallel.  Competing GROWS can be folded when possible-- if
     * there's a grow in progress that's bigger than our desired size,
     * we can just grow to the larger size.
     *
     * Similarly, we can fold SHRINKS.  However, we CANNOT combine the
     * two, because shrinks conceptually delete cells.
     * 
     * However, if there's contention we can linearize any grows
     * *before* the shrinks.   Therefore, the only times we need to 
     * do a retry are:
     *
     * 1) When we have a late shrink, where the current migration is 
     *    setting to a higher size than us.
     *
     * 2) We have a late grow, where the current migration isn't
     *    growing the array ENOUGH.
     */

    next_expected.next_size   = 0;
    next_expected.next_store  = NULL;
    next_candidate.next_size  = index;
    next_candidate.next_store = NULL;
    must_retry                = false;
    
    
    while (!CAS(&store->next, &next_expected, next_candidate)) {
	if (next_expected.next_store) {
	    // We are too late to have a say.  Help migrate.
	    flexarray_migrate(store, self);
	    
	    if (index > store->store_size) {
		// If we're a grow, we're done if the migration was a shrink
		// or a larger grow.
		if ((next_candidate.next_size < store->store_size) ||
		    index < next_candidate.next_size) {
		    mmm_end_op();
		    return;
		}
	    } else {
		// If we're a shrink, we're done if the migration was a
		// larger shrink.
		if (next_candidate.next_size < index) {
		    mmm_end_op();
		    return;
		}
	    }
	    // Otherwise, we need to retry our operation.
	    mmm_end_op();
	    flexarray_set_size(self, index);
	    return;
	}
	/* No store is agreed upon yet, so: 
	 *
	 * 1) If we are a grow and we see a bigger grow or a shrink,
	 *    we are content that our request is served, and we can go
	 *    help migrate.
	 *
	 * 2) If we are a shrink and we see a bigger shrink, we are
	 *    content that our request is served, and we go help
	 *    migrate.
	 *
	 * Otherwise, we try to install our desired target size again.
	 */

	if (index > store->store_size) {
	    if ((next_expected.next_size < store->store_size) ||
		(next_expected.next_size >= index)) {
		flexarray_migrate(store, self);
		mmm_end_op();
		return;
	    }
	}
	else {
	    if (next_expected.next_size <= index) {
		flexarray_migrate(store, self);
		mmm_end_op();
		return;
	    }
	}
    }
    
    mmm_end_op();
    
    return;
}

flex_view_t *
flexarray_view(flexarray_t *self)
{
    flex_view_t  *ret;
    flex_store_t *store;
    bool          expected;
    uint64_t      i;
    flex_item_t   item;

    mmm_start_basic_op();
    
    while (true) {
	store    = atomic_read(&self->store);
	expected = false;
	
	if (CAS(&store->claimed, &expected, true)) {
	    break;
	}
	flexarray_migrate(store, self);
    }

    flexarray_migrate(store, self);

    if (self->ret_callback) {
	for (i = 0; i < store->array_size; i++) {
	    item = atomic_read(&store->cells[i]);
	    if (item.state & FLEX_ARRAY_USED) {
		(*self->ret_callback)(item.item);
	    }
	}
    }
    
    mmm_end_op();
    
    ret           = (flex_view_t *)malloc(sizeof(flex_view_t));
    ret->contents = store;
    ret->next_ix  = 0;
    
    return ret;
}

void *
flexarray_view_next(flex_view_t *view, bool *found)
{
    flex_item_t item;

    while (true) {
	if (view->next_ix >= view->contents->array_size) {
	    if (found) {
		*found = false;
	    }
	    return NULL;
	}
	
	item = atomic_read(&view->contents->cells[view->next_ix++]);

	if (item.state & FLEX_ARRAY_USED) {
	    if (found) {
		*found = true;
	    }
	    return item.item;
	}
    }
}

void
flexarray_view_delete(flex_view_t *view)
{
    void *item;
    bool  found;
    
    if (view->eject_callback) {
	while (true) {
	    item = flexarray_view_next(view, &found);
	    if (!found) {
		break;
	    }

	    (*view->eject_callback)(item);
	}
    }

    mmm_retire(view->contents);

    free(view);

    return;
}

static flex_store_t *
flexarray_new_store(uint64_t array_size, uint64_t store_size)
{
    flex_store_t *ret;
    uint32_t      alloc_len;

    alloc_len = sizeof(flex_store_t) + sizeof(flex_cell_t) * store_size;
    ret       = (flex_store_t *)mmm_alloc_committed(alloc_len);
    
    ret->store_size = store_size;

    atomic_store(&ret->array_size, array_size);

    return ret;
}

static void
flexarray_migrate(flex_store_t *store, flexarray_t *top)
{
    flex_next_t   expected_next;
    flex_next_t   candidate_next;
    flex_store_t *next_store;
    flex_item_t   expected_item;
    flex_item_t   candidate_item;
    uint64_t      i;
    uint64_t      num_buckets;
    uint64_t      new_size;
    
    if (atomic_read(&top->store) != store) {
	return;
    }

    expected_next = atomic_read(&store->next);
    if (expected_next.next_store) {
	next_store = expected_next.next_store;
	goto help_move;
    }

    // Set those migration bits!
    for (i = 0; i < store->store_size; i++) {
	expected_item = atomic_read(&store->cells[i]);

	while (true) {
	    if (expected_item.state & FLEX_ARRAY_MOVING) {
		break;
	    }
	    if (expected_item.state & FLEX_ARRAY_USED) {
		candidate_item.state = FLEX_ARRAY_MOVING | FLEX_ARRAY_USED;
		candidate_item.item  = expected_item.item;
	    }
	    else {
		candidate_item.state = FLEX_ARRAY_MOVING | FLEX_ARRAY_MOVED;
		candidate_item.item  = NULL;
	    }
	    if (CAS(&store->cells[i], &expected_item, candidate_item)) {
		break;
	    }
	}
    }

    // Now, fight to install the store.

    expected_next = atomic_read(&store->next);

    while (!expected_next.next_store) {
	
	num_buckets = hatrack_round_up_to_power_of_2(expected_next.next_size);
	
	if (num_buckets < (1 << FLEXARRAY_MIN_STORE_SZ_LOG)) {
	    num_buckets = 1 << FLEXARRAY_MIN_STORE_SZ_LOG;
	}
	
	next_store = flexarray_new_store(expected_next.next_size, num_buckets);
	
	candidate_next.next_store = next_store;
	candidate_next.next_size  = expected_next.next_size;

	if (CAS(&store->next, &expected_next, candidate_next)) {
	    goto help_move;
	}

	mmm_retire_unused(next_store);
    }

    next_store = expected_next.next_store;

    // Now, help move items that are moving.
 help_move:

    new_size = expected_next.next_size;
    
    for (i = 0; i < store->store_size; i++) {
	candidate_item = atomic_read(&store->cells[i]);
	if (candidate_item.state & FLEX_ARRAY_MOVED) {
	    continue;
	}
	
	if (i < new_size) {
	    expected_item.item   = NULL;
	    expected_item.state  = 0;
	    candidate_item.state = FLEX_ARRAY_USED;
	    CAS(&next_store->cells[i], &expected_item, candidate_item);
	    expected_item.item   = candidate_item.item;
	    expected_item.state  = FLEX_ARRAY_USED|FLEX_ARRAY_MOVING;
	    CAS(&store->cells[i], &expected_item, candidate_item);
	    continue;
	}

	// If there are any items left in the current array, we
	// eject them, if the callback is set, and we win the CAS.
	expected_item         = candidate_item;
	candidate_item.state |= FLEX_ARRAY_MOVED;

	if (CAS(&store->cells[i], &expected_item, candidate_item)) {
	    if (top->eject_callback) {
		(*top->eject_callback)(candidate_item.item);
	    }
	}
    }

    // Okay, now swing the store pointer; winner retires the old store!
    if (CAS(&top->store, &store, next_store)) {
	if (!store->claimed) {
	    mmm_retire(store);
	}
    }

    return;
}
