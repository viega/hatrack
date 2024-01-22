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
 *  Name:           vector.c
 *  Description:    A wait free vector, complete w/ push/pop/peek
 *
 *  Author:         John Viega, john@zork.org
 */

#include <hatrack.h>

static vector_store_t *vector_new_store(int64_t, int64_t);
static void            vector_migrate  (vector_store_t *, vector_t *);


// Ops in the help vtable.
static void            help_push  (help_manager_t *, help_record_t *, int64_t);
static void            help_pop   (help_manager_t *, help_record_t *, int64_t);
static void            help_peek  (help_manager_t *, help_record_t *, int64_t);
static void            help_grow  (help_manager_t *, help_record_t *, int64_t);
static void            help_shrink(help_manager_t *, help_record_t *, int64_t);
static void            help_set   (help_manager_t *, help_record_t *, int64_t);
static void            help_view  (help_manager_t *, help_record_t *, int64_t);

static helper_func vtable[] = {
    (helper_func)help_push,
    (helper_func)help_pop,
    (helper_func)help_peek,
    (helper_func)help_grow,
    (helper_func)help_shrink,
    (helper_func)help_set,
    (helper_func)help_view
};

/* The size parameter is the one larger than the largest allowable index.
 * The underlying store may be bigger-- it will be sized up to the next
 * power of two.
 */
vector_t *
vector_new(int64_t initial_size)
{
    vector_t *arr;

    arr = (vector_t *)calloc(1, sizeof(vector_t));
    
    vector_init(arr, initial_size, false);
    
    return arr;
}

void
vector_init(vector_t *vec, int64_t store_size, bool zero)
{
    vec->ret_callback   = NULL;
    vec->eject_callback = NULL;
    store_size          = hatrack_round_up_to_power_of_2(store_size);

    if (store_size < (1 << VECTOR_MIN_STORE_SZ_LOG)) {
	store_size = 1 << VECTOR_MIN_STORE_SZ_LOG;
    }
    
    atomic_store(&vec->store, vector_new_store(0, store_size));
    hatrack_help_init(&vec->help_manager, vec, vtable, zero);

    return;
}

void
vector_set_ret_callback(vector_t *self, vector_callback_t callback)
{
    self->ret_callback = callback;

    return;
}

void
vector_set_eject_callback(vector_t *self, vector_callback_t callback)
{
    self->eject_callback = callback;
}

void
vector_cleanup(vector_t *self)
{
    vector_store_t *store;
    int64_t         i;
    vector_item_t   item;
    vec_size_info_t si;

    store = atomic_load(&self->store);
    si    = atomic_load(&store->array_size_info);
    
    if (self->eject_callback) {
	for (i = 0; i < si.array_size; i++) {
	    item = atomic_load(&store->cells[i]);
	    if (item.state) {
		(*self->eject_callback)(item.item);
	    }
	}
    }

    mmm_retire_unused(self->store);

    return;
}

void
vector_delete(vector_t *self)
{
    vector_cleanup(self);
    free(self);

    return;
}

// Always linearized based on the read time.
void *
vector_get(vector_t *self, int64_t index, int *status)
{
    vector_item_t   current;
    vector_store_t *store;
    vec_size_info_t si;    
    
    mmm_start_basic_op();

    store = atomic_load(&self->store);
    si    = atomic_load(&store->array_size_info);    

    if (index >= si.array_size) {
	if (status) {
	    *status = VECTOR_OOB;
	}
	return NULL;
    }

    if (index >= store->store_size) {
	if (status) {
	    *status = VECTOR_UNINITIALIZED;
	}
	return NULL;
    }
	
    current = atomic_load(&store->cells[index]);

    if (!(current.state & VECTOR_USED)) {
	if (status) {
	    *status = VECTOR_UNINITIALIZED;
	}
	return NULL;
    }
    
    if (self->ret_callback && current.item) {
	(*self->ret_callback)(current.item);
    }
    
    mmm_end_op();

    if (status) {
	*status = VECTOR_OK;
    }
    
    return current.item;
}

// Returns true if successful, false if write would be out-of-bounds.
bool
vector_set(vector_t *self, int64_t index, void *item)
{
    vector_store_t *store;
    vector_item_t   current;
    vector_item_t   candidate;
    vector_cell_t  *cellptr;
    vec_size_info_t si;        
    bool            found;
    
    mmm_start_basic_op();
    
    store      = atomic_load(&self->store);
    si         = atomic_load(&store->array_size_info);        
	
    if (index >= si.array_size) {
	mmm_end_op();
	return false;
    }

    // This is messed up.  Need to pass in item and ix.
    if ((index + 1) == si.array_size) {
	hatrack_perform_wf_op(&self->help_manager,
			      VECTOR_OP_SLOW_SET,
			      item,
			      (void *)index,
			      &found);
	mmm_end_op();
	
	return found;
    }

    if (index >= store->store_size) {
	vector_migrate(store, self);
	mmm_end_op();
	return vector_set(self, index, item);
    }

    cellptr = &store->cells[index];
    current = atomic_load(cellptr);

    if (current.state & VECTOR_MOVING) {
	vector_migrate(store, self);
	mmm_end_op();
	return vector_set(self, index, item);
    }

    if (current.state & VECTOR_POPPED) {
	mmm_end_op();
	return false;
    }
	
    candidate.item  = item;
    candidate.state = current.state | VECTOR_USED;

    if (CAS(cellptr, &current, candidate)) {
	if (self->eject_callback && current.state == VECTOR_USED) {
	    (*self->eject_callback)(current.item);
	}
	mmm_end_op();
	return true;
    }

    if (current.state & VECTOR_MOVING) {
	vector_migrate(store, self);
	mmm_end_op();	
	return vector_set(self, index, item);
    }

    /* Otherwise, someone beat us to the CAS. It could be another set,
     * but it could also be a push(), if some pops happened after we
     * checked against the array size and did the initial read of the
     * current location, but before we got around to the CAS.
     *
     * Since a push implies the overwritten value was undefined, we
     * CANNOT sequence ourselves before this operation.  We must
     * try again. We go ahead and use the slow path to do so.
     */
    hatrack_perform_wf_op(&self->help_manager,
			  VECTOR_OP_SLOW_SET,
			  item,
			  NULL,
			  &found);
    
    mmm_end_op();
    return found;
}

void
vector_grow(vector_t *self, int64_t size)
{
    mmm_start_basic_op();
    hatrack_perform_wf_op(&self->help_manager,
			  VECTOR_OP_GROW,
			  (void *)size,
			  NULL,
			  NULL);
    mmm_end_op();

    return;
}

void
vector_shrink(vector_t *self, int64_t size)
{
    mmm_start_basic_op();
    hatrack_perform_wf_op(&self->help_manager,
			  VECTOR_OP_SHRINK,
			  (void *)size,
			  NULL,
			  NULL);
    mmm_end_op();

    return;
}

void
vector_push(vector_t *self, void *item)
{
    mmm_start_basic_op();
    hatrack_perform_wf_op(&self->help_manager,
			  VECTOR_OP_PUSH,
			  item,
			  NULL,
			  NULL);
    mmm_end_op();

    return;
}

void *
vector_pop(vector_t *self, bool *found)
{
    void           *ret;
    vector_store_t *store;
    vec_size_info_t si;
    
    mmm_start_basic_op();
    /* Before we enqueue ourselves, if we can see the array is
     * definitely empty, just linearize ourselves to the read of
     * array_size_info.
     */
    store = atomic_load(&self->store);
    si    = atomic_load(&store->array_size_info);
    if (!si.array_size) {
	if (found) {
	    *found = false;
	}
	mmm_end_op();
	return NULL;
    }
    ret = hatrack_perform_wf_op(&self->help_manager,
				VECTOR_OP_POP,
				NULL,
				NULL,
				found);
    
    mmm_end_op();

    return ret;
}

void *
vector_peek(vector_t *self, bool *found)
{
    void *ret;
    
    mmm_start_basic_op();
    ret = hatrack_perform_wf_op(&self->help_manager,
				VECTOR_OP_PEEK,
				NULL,
				NULL,
				found);
    mmm_end_op();

    return ret;
}

vector_view_t *
vector_view(vector_t *self)
{
    vector_view_t  *ret;
    vector_store_t *store;
    int64_t         i;
    vector_item_t   item;
    vec_size_info_t si;            

    ret          = malloc(sizeof(vector_view_t));
    ret->next_ix = 0;
		 
    mmm_start_basic_op();
    
    store     = hatrack_perform_wf_op(&self->help_manager,
				      VECTOR_OP_VIEW,
				      NULL,
				      NULL,
				      NULL);
    si        = atomic_load(&store->array_size_info);
    ret->size = si.array_size;

    if (self->ret_callback) {
	for (i = 0; i < si.array_size; i++) {
	    item = atomic_load(&store->cells[i]);
	    if (item.state & FLEX_ARRAY_USED) {
		(*self->ret_callback)(item.item);
	    }
	}
    }

    ret->contents       = store;
    ret->eject_callback = self->eject_callback;

    mmm_end_op();
    
    return ret;
}

void *
vector_view_next(vector_view_t *view, bool *found)
{
    vector_item_t item;

    while (true) {
	if (view->next_ix >= view->size) {
	    if (found) {
		*found = false;
	    }
	    return NULL;
	}
	
	item = atomic_load(&view->contents->cells[view->next_ix++]);

	if (item.state & VECTOR_USED) {
	    if (found) {
		*found = true;
	    }
	    return item.item;
	}
    }
}

void
vector_view_delete(vector_view_t *view)
{
    void *item;
    bool  found;
    
    if (view->eject_callback) {
	while (true) {
	    item = vector_view_next(view, &found);
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

static vector_store_t *
vector_new_store(int64_t array_size, int64_t store_size)
{
    vector_store_t *ret;
    uint32_t        alloc_len;
    vec_size_info_t si;

    alloc_len = sizeof(vector_store_t) + sizeof(vector_cell_t) * store_size;
    ret       = (vector_store_t *)mmm_alloc_committed(alloc_len);
    
    si.array_size        = array_size;
    si.job_id            = 0;
    ret->store_size      = store_size;
    ret->array_size_info = si;

    return ret;
}

/* This only gets called while there is an active help job.
 * Therefore, as long as the store is the current store, we can be
 * sure that the current help jobid is appropriate.
 */
static void
vector_migrate(vector_store_t *store, vector_t *top)
{
    vector_store_t *next_store;
    vector_store_t *expected_next;
    vector_item_t   expected_item;
    vector_item_t   candidate_item;
    int64_t         i;
    int64_t         new_array_len;
    int64_t         new_store_len;
    vec_size_info_t si;
    
	
    if (atomic_load(&top->store) != store) {
	return;
    }

    next_store = atomic_load(&store->next);
    
    if (next_store) {
        si            = atomic_load(&next_store->array_size_info);
	new_array_len = si.array_size;
	goto help_move;
    }

    // Set those migration bits!
    for (i = 0; i < store->store_size; i++) {
	expected_item = atomic_load(&store->cells[i]);

	while (true) {
	    if (expected_item.state & VECTOR_MOVING) {
		break;
	    }
	    if (expected_item.state & VECTOR_USED) {
		candidate_item.state = VECTOR_MOVING | VECTOR_USED;
		candidate_item.item  = expected_item.item;
	    }
	    else {
		candidate_item.state = VECTOR_MOVING | VECTOR_MOVED;
		candidate_item.item  = NULL;
	    }
	    if (CAS(&store->cells[i], &expected_item, candidate_item)) {
		break;
	    }
	}
    }

    /* Now, fight to install the store.  The +1 for new_array_len
     * ensures that, if the migration is the result of a push, that we
     * actually always allocate enough room to hold that push.
     */
    expected_next    = 0;
    si               = atomic_load(&store->array_size_info);
    new_array_len    = si.array_size;
    new_store_len    = hatrack_round_up_to_power_of_2(new_array_len + 1);
    next_store       = vector_new_store(new_array_len, new_store_len);
    
    if (!CAS(&store->next, &expected_next, next_store)) {
	mmm_retire_unused(next_store);
	next_store = expected_next;
    }
    
    // Now, help move items that are moving.
 help_move:
    for (i = 0; i < store->store_size; i++) {
	candidate_item = atomic_load(&store->cells[i]);
	if (candidate_item.state & VECTOR_MOVED) {
	    continue;
	}
	
	if (i < new_array_len) {
	    expected_item.item   = NULL;
	    expected_item.state  = 0;
	    candidate_item.state = VECTOR_USED;
	    CAS(&next_store->cells[i], &expected_item, candidate_item);
	    expected_item.item   = candidate_item.item;
	    expected_item.state  = VECTOR_USED|VECTOR_MOVING;
	    CAS(&store->cells[i], &expected_item, candidate_item);
	    continue;
	}

	// If there are any items left in the current array, we
	// eject them, if the callback is set, and we win the CAS.
	expected_item         = candidate_item;
	candidate_item.state |= VECTOR_MOVED;

	if (CAS(&store->cells[i], &expected_item, candidate_item)) {
	    if (top->eject_callback) {
		(*top->eject_callback)(candidate_item.item);
	    }
	}
    }

    // Okay, now swing the store pointer, and free if needed.
    if (CAS(&top->store, &store, next_store)) {
	if (!store->claimed) {
	    mmm_retire(store);
	}
    }

    return;
}

/* For these help functions, we may always be competing against other
 * threads trying to perform the exact same operations at the same
 * time.  However, in some cases (e.g., with pop), we might be
 * competing with CAS operations from vector_set() as well, so we
 * won't always want to assume that our failure is someone else's
 * success.
 *
 * Also, we still need to assume that we could be indefinitely
 * suspended at any time. That means we cannot just change values
 * under the assumption that they're current; we need to make sure
 * they really are current.
 *
 * To that end, fields we might want to update will keep a record of
 * the last jobid associated with an update.  If the found jobid is
 * lower than the one we're servicing, we need to change.  If it's
 * ever higher, then we know the entire help request is done, and we
 * don't need to do any more work.
 *
 *
 * Push will generally want to write itself out to the cell it's going
 * into before bumping up the store size, to help make sure it doesn't
 * ever have to compete with set() calls. Before it can do that, it
 * will need to expand the underlying store, if necessary.
 */
static void
help_push(help_manager_t *manager, help_record_t *record, int64_t jobid)
{
    vector_t       *vec;
    vector_store_t *store;
    vec_size_info_t si;
    vec_size_info_t csi;
    vector_item_t   expected;
    vector_item_t   candidate;
    int64_t         found_job;
    int64_t         slot;

    vec   = (vector_t *)manager->parent;
    store = atomic_load(&vec->store);
    si    = atomic_load(&store->array_size_info);

    if (si.job_id > jobid) {
	return;
    }

    if (si.job_id < jobid) {
	csi.job_id     = jobid;
	slot           = si.array_size;
	csi.array_size = slot + 1;
	
	if (slot == store->store_size) {
	    vector_migrate(store, vec);
	    
	    store = atomic_load(&vec->store);
	    si    = atomic_load(&store->array_size_info);
	    if (si.job_id > jobid) {
		return;
	    }
	    if (si.job_id == jobid) {
		if (si.array_size == csi.array_size) {
		    hatrack_complete_help(manager, record, jobid, NULL, true);
		    return;
		}
	    }
	}
	
	expected  = atomic_load(&store->cells[slot]);
	found_job = expected.state & VECTOR_JOB_MASK;
	if (found_job > jobid) {
	    return;
	}
	if (found_job < jobid) {
	    candidate.item  = record->input;
	    candidate.state = VECTOR_USED | jobid;
	    
	    CAS(&store->cells[si.array_size], &expected, candidate);
	    DEBUG3(jobid, record->input, slot, "Job $1: PUSH $2 (index $3)");
	}
	if((si.array_size + 1) != (csi.array_size)) {
	    DEBUG3(si.array_size, csi.array_size, jobid, "WTF??");
	}
	while (!CAS(&store->array_size_info, &si, csi)) {
	    if (si.job_id > jobid) {
		return;
	    }
	    if (si.job_id == jobid) {
		break;
	    }
	}
    } 
    hatrack_complete_help(manager, record, jobid, NULL, true);

    return;
}

static void
help_pop(help_manager_t *manager, help_record_t *record, int64_t jobid)
{
    vector_t       *vec;
    vector_store_t *store;
    vec_size_info_t si;
    vec_size_info_t csi;
    int64_t        index;
    vector_item_t   expected;
    vector_item_t   candidate;
    void           *ret;

    vec   = (vector_t *)manager->parent;
    store = atomic_load(&vec->store);
    si    = atomic_load(&store->array_size_info);

    if (si.job_id > jobid) {
	return; // This request was definitely already serviced.
    }

    if (si.job_id == jobid) {
	index = si.array_size;
    }
    else {
	if (!(si.array_size)) {
	    DEBUG_PTR(jobid, "Pop of empty stack, JID = $1");
	    hatrack_complete_help(manager, record, jobid, NULL, false);
	    return;
	}
	index = si.array_size - 1;
    }

    
    expected  = atomic_load(&store->cells[index]);
    ret       = expected.item;
    candidate.item  = expected.item;
    candidate.state = VECTOR_POPPED | jobid;

    /* The CAS in this loop can compete with vector_set() calls.  To
     * make the vector wait-free, we want to make sure there's a hard
     * upper bound on the amount of contention we can see there.
     *
     * To make that happen, vector_set() will, any time it sees that
     * it's being asked to set the last item in an array, enqueue its
     * operation via the help manager, to make sure that it's
     * processed sequentially with potential contending operations.
     *
     * set operations could still compete with pops, if the stack
     * shrinks after the set operation checks the size. That's okay
     * though, because once the pop operation is in progress, there
     * can be only a finite number of threads attempting to write to
     * this slot that *wouldn't* recognize it as the last slot.
     */
    while ((expected.state & (VECTOR_POPPED | VECTOR_JOB_MASK))
	   < (uint64_t)jobid) {
	    
	if (CAS(&store->cells[index], &expected, candidate)) {
	    goto complete_op;
	}
	
	ret             = expected.item;
	candidate.item  = expected.item;	
    }

    if ((expected.state & VECTOR_JOB_MASK) > (uint64_t)jobid) {
	return;
    }

 complete_op:
    if (si.job_id < jobid) {
	csi.array_size = index;
	csi.job_id     = jobid;
	CAS(&store->array_size_info, &si, csi);
    }
	
    if (expected.state & VECTOR_USED) {
	DEBUG3(jobid, ret, index, "Job $1 POP $2 (index $3)");
    }

    hatrack_complete_help(manager, record, jobid, ret, true);
    return;
    
}

/* We really only need peek to use the help manager to make sure that
 * the index of the top item doesn't change while we're performing the
 * operation.
 *
 * The other subtlety is that we need to be clear about semantics if
 * there's an uninitialized element at the top of the object.
 *
 * We're going to treat that as 'not found', but without indicating
 * whether it's not found because of an undefined element, or not
 * found because the vector is empty.
 *
 * We could steal another bit for a flag in the help manager, if we
 * cared about distinguishing these cases.
 *
 * An alternative would be to scan backward through the vector, but
 * there we can easily end up having linearization problems that would
 * require a lot more work on the vector_set() operation than we would
 * like.
 */
static void
help_peek(help_manager_t *manager, help_record_t *record, int64_t jobid)
{
    vector_t       *vec;
    vector_store_t *store;
    vec_size_info_t si;
    vector_item_t   item;
    

    vec   = (vector_t *)manager->parent;
    store = atomic_load(&vec->store);
    si    = atomic_load(&store->array_size_info);

    if (si.job_id > jobid) {
	return; // This request was definitely already serviced.
    }

    if (!(si.array_size)) {
    bottom:
	hatrack_complete_help(manager, record, jobid, NULL, false);
	return;
    }

    item = atomic_load(&store->cells[si.array_size - 1]);

    if ((item.state & VECTOR_JOB_MASK) > (uint64_t)jobid) {
	return; // Already serviced.
    }

    if (!(item.state & VECTOR_USED)) {
	goto bottom;
    }
	
    hatrack_complete_help(manager, record, jobid, item.item, true);
    return;
}

static void
help_grow(help_manager_t *manager, help_record_t *record, int64_t jobid)
{
    vector_t       *vec;
    vec_size_info_t expected;
    vec_size_info_t candidate;
    vector_store_t *store;
    int64_t         size;
    int64_t         old_size;
    bool            already_grown;

    vec      = (vector_t *)manager->parent;
    store    = atomic_load(&vec->store);
    expected = atomic_load(&store->array_size_info);
    size     = (int64_t)record->input;
    old_size = expected.array_size;

    if (store != atomic_load(&vec->store)) {
	hatrack_complete_help(manager, record, jobid, NULL, true);
	return;
    }
    
    if (expected.job_id > jobid) {
	return;
    }

    if (expected.job_id < jobid) {
	candidate.job_id = jobid;
	if (old_size >= size) {
	    candidate.array_size = old_size;
	    already_grown        = true;
	}
	else {
	    candidate.array_size = size;
	    already_grown        = false;
	}
	
	if (!CAS(&store->array_size_info, &expected, candidate)) {
	    /* If we got here, some other thread succeeded, so we just
	     * need to make sure we weren't suspended for too long.
	     */
	    if (expected.job_id > jobid) {
		return;
	    }
	}
    } else {
	if (old_size >= size) {
	    already_grown = true;
	}
	else {
	    already_grown = false;
	}
    }

    if (already_grown) {
	hatrack_complete_help(manager, record, jobid, NULL, true);
	return;
    }

    /* At this point, if the new size is less than the store size,
     * then we can simply finish up (as we changed the array_size_info
     * field above).
     *
     * Otherwise, we need to kick off a migration.
     */
    if (size > store->store_size) {
	vector_migrate(store, vec);
    }

    hatrack_complete_help(manager, record, jobid, NULL, true);
    return;
}

static void
help_shrink(help_manager_t *manager, help_record_t *record, int64_t jobid)
{
    vector_t       *vec;
    vec_size_info_t expected;
    vec_size_info_t candidate;
    vector_store_t *store;
    vector_item_t   expected_item;
    vector_item_t   candidate_item;
    int64_t         size;
    int64_t         old_size;
    int64_t         i;
    int64_t        found_job;
    bool            already_shrunk;

    vec      = (vector_t *)manager->parent;
    store    = atomic_load(&vec->store);
    expected = atomic_load(&store->array_size_info);
    size     = (int64_t)record->input;
    old_size = expected.array_size;

    if (expected.job_id > jobid) {
	return;
    }

    if (expected.job_id < jobid) {
	candidate.job_id = jobid;
	if (old_size <= size) {
	    candidate.array_size = old_size;
	    already_shrunk       = true;
	}
	else {
	    candidate.array_size = size;
	    already_shrunk       = false;
	}
	
	if (!CAS(&store->array_size_info, &expected, candidate)) {
	    /* If we got here, some other thread succeeded, so we just
	     * need to make sure we weren't suspended for too long.
	     */
	    if (expected.job_id > jobid) {
		return;
	    }
	}
    } else {
	if (old_size <= size) {
	    already_shrunk = true;
	}
	else {
	    already_shrunk = false;
	}
    }

    if (already_shrunk) {
	hatrack_complete_help(manager, record, jobid, NULL, true);
	return;
    }

    /* Otherwise, instead of migrating, we can simply set the flags
     * for any cells we've shrunk past to VECTOR_POPPED (and the jobid
     * to our jobid).
     */
    candidate_item.item  = NULL;
    candidate_item.state = VECTOR_POPPED | jobid;
	
    for (i = size; i < old_size; i++) {
	expected_item  = atomic_load(&store->cells[i]);
	found_job = expected_item.state & VECTOR_JOB_MASK;
	if (found_job == jobid) {
	    continue;
	}
	if (found_job > jobid) {
	    return;
	}
	CAS(&store->cells[i], &expected_item, candidate_item);
    }

    hatrack_complete_help(manager, record, jobid, NULL, true);

    return;
}

/* This slow path for set only gets called in cases where we know we
 * might compete with a pop() operation, in which case we volunteer to
 * take the slow path, which ends up keeping the pop operation
 * wait-free (see above).
 */
static void
help_set(help_manager_t *manager, help_record_t *record, int64_t jobid)
{
    vector_t       *vec;
    vector_store_t *store;
    vector_item_t   expected;
    vector_item_t   candidate;
    vec_size_info_t si;
    int64_t         ix;
    int64_t         found_job;
    void           *item;
    

    item  = record->input;
    ix    = (int64_t)record->aux;
    vec   = (vector_t *)manager->parent;
    store = atomic_load(&vec->store); // CST ensures we won't migrate.
    si    = atomic_load(&store->array_size_info);

    if (si.job_id > jobid) {
	return;
    }

    if (si.array_size >= ix) {
	hatrack_complete_help(manager, record, jobid, NULL, false);
	return;
    }

    expected  = atomic_load(&store->cells[ix]);
    found_job = expected.state & VECTOR_JOB_MASK;
    
    if (found_job > jobid) {
	return;
    }

    if (found_job == jobid) {
	hatrack_complete_help(manager, record, jobid, NULL, true);
    }

    candidate.item  = item;
    candidate.state = VECTOR_USED | jobid;

    if (!CAS(&store->cells[ix], &expected, candidate)) {
	if ((expected.state & VECTOR_JOB_MASK) > (uint64_t)jobid) {
	    return;
	}
    }

    hatrack_complete_help(manager, record, jobid, NULL, true);
    return;
}

/* We could get away with not doing a migration as part of the view,
 * but we'd have to add extra status logic involving both a flag and
 * an epoch, plus we'd end up touching about the same number of cells.
 *
 * So it really is better to just go ahead and kick off a migration.
 *
 * The only challenge here is knowing whether our store ends up
 * current after we load it (a faster view might replace it before we
 * load it, yet we might be needed to help return it).
 *
 * Here's how we're currently dealing with the problem:
 *
 * 1) Load the current value of the store.
 * 2) For the store we've found, check the value of the creation_epoch
 *    field. 
 *      A) If it's less than our job ID, we have the correct store.
 *      B) If it's higher than our job ID, the view already finished.
 *      C) If it's equal to our job ID, then the new store is installed
 *         and open, but the operation hasn't completed.
 * 3) In case A, we follow the typical migration path.
 * 4) In case C, we read the "prev" field in the new store, so that we
 *    can try to help return the store handle to the thread needing help.
 */
static void
help_view(help_manager_t *manager, help_record_t *record, int64_t jobid)
{
    #if 0
    vector_t       *vec;
    vector_store_t *store;
    vector_store_t *possible_store;
    capq_item_t     item;

    vec   = (vector_t *)manager->parent;
    store = atomic_load(&vec->store);
    item  = atomic_load(&record->retval);
    
    if (item.jobid > jobid) {
	return;
    }

    if (item.jobid == jobid) {
	possible_store = (vector_store_t *)item.data;
	if (possible_store != store) {
	    hatrack_complete_help(manager, record, jobid, store, true);
	}
	return;
    }

    atomic_store(&store->claimed, true);
    vector_migrate(store, vec);
    hatrack_complete_help(manager, record, jobid, store, true);
#endif
    return;
}
