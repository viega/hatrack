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
	mmm_end_op();
	return NULL;
    }

    // A resize is in progress, item is not there yet.
    if (index >= store->store_size) {
	if (status) {
	    *status = FLEX_UNINITIALIZED;
	}
	mmm_end_op();	
	return NULL;
    }
    
	
    current = atomic_read(&store->cells[index]);
    
    if (!(current.state & FLEX_ARRAY_USED)) {
	if (status) {
	    *status = FLEX_UNINITIALIZED;
	}
	mmm_end_op();	
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
    uint64_t      read_index;
    
    mmm_start_basic_op();
    
    store      = atomic_read(&self->store);
    read_index = atomic_read(&store->array_size) & ~FLEX_ARRAY_SHRINK;
	
    if (index >= read_index) {
	mmm_end_op();	
	return false;
    }

    if (index >= store->store_size) {
	flexarray_migrate(store, self);
	mmm_end_op();
	return flexarray_set(self, index, item);
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
	if (self->eject_callback && (current.state == FLEX_ARRAY_USED)) {
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
flexarray_grow(flexarray_t *self, uint64_t index)
{
    flex_store_t *store;
    uint64_t      array_size;

    mmm_start_basic_op();

    /* Just change store->array_size, kick off a migration if
     * necessary, and be done.
     */
    do {
	store      = atomic_read(&self->store);
	array_size = atomic_read(&store->array_size);

	/* If we're shrinking, we don't want to re-expand until we
	 * know that truncated cells are zeroed out.
	 */
	if (array_size & FLEX_ARRAY_SHRINK) {
	    flexarray_migrate(store, self);
	    continue;
	}
	
	if (index < array_size) {
	    mmm_end_op();	    
	    return;
	}
    } while (!CAS(&store->array_size, &array_size, index));


    if (index > store->store_size) {
	flexarray_migrate(store, self);		
    }
    
    mmm_end_op();
    return;
}

void
flexarray_shrink(flexarray_t *self, uint64_t index)
{
    flex_store_t *store;
    uint64_t      array_size;

    index |= FLEX_ARRAY_SHRINK;
    
    mmm_start_basic_op();
    
    do {
	store      = atomic_read(&self->store);
	array_size = atomic_read(&store->array_size);
	
	if (index > array_size) {
	    mmm_end_op();	    
	    return;
	}
    } while (!CAS(&store->array_size, &array_size, index));

    flexarray_migrate(store, self);		
    
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
    flex_store_t *next_store;
    flex_store_t *expected_next;
    flex_item_t   expected_item;
    flex_item_t   candidate_item;
    uint64_t      i;
    uint64_t      new_array_len;
    uint64_t      new_store_len;
    
    if (atomic_read(&top->store) != store) {
	return;
    }

    next_store = atomic_read(&store->next);
    
    if (next_store) {
	goto help_move;
    }

    // Set those migration bits!
    for (i = 0; i < store->store_size; i++) {
	expected_item = atomic_read(&store->cells[i]);

	if (!(expected_item.state & FLEX_ARRAY_MOVING)) {
	    if (expected_item.state & FLEX_ARRAY_USED) {
		OR2X64L(&store->cells[i], FLEX_ARRAY_MOVING);
	    }
	    else {
		OR2X64L(&store->cells[i], FLEX_ARRAY_MOVING | FLEX_ARRAY_MOVED);
	    }
	}
    }

    // Now, fight to install the store.
    expected_next = 0;
    new_array_len = store->array_size;
    new_store_len = hatrack_round_up_to_power_of_2(new_array_len) << 1;
    next_store    = flexarray_new_store(new_store_len, new_store_len);
    
    if (!CAS(&store->next, &expected_next, next_store)) {
	mmm_retire_unused(next_store);
	next_store = expected_next;
    }
    
    // Now, help move items that are moving.
 help_move:
    for (i = 0; i < store->store_size; i++) {
	candidate_item = atomic_read(&store->cells[i]);
	if (candidate_item.state & FLEX_ARRAY_MOVED) {
	    continue;
	}
	
	if (i < new_array_len) {
	    expected_item.item   = NULL;
	    expected_item.state  = 0;
	    candidate_item.state = FLEX_ARRAY_USED;
	    
	    CAS(&next_store->cells[i], &expected_item, candidate_item);
	    OR2X64L(&store->cells[i], FLEX_ARRAY_MOVED);
	    continue;
	}

	/* If there are any items left in the current array, we eject
	 * them, which requires us CASing in FLEX_ARRAY_MOVED, so that
	 * we can decide upon a thread to call the ejection handler.
	 */
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
