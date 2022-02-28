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
 *  Name:           logring.h
 *
 *  Description: A fast ring buffer intended for safe in-memory
 *               message passing and logging, using a contiguous piece
 *               of memory.
 *
 */

#include <hatrack.h>

static void logring_view_help_if_needed(logring_t *);

static const logring_entry_info_t empty_entry = {
    .write_epoch = 0,
    .state       = LOGRING_EMPTY
};

logring_t *
logring_new(uint64_t ring_size, uint64_t entry_size)
{
    logring_t *ret;

    ret = (logring_t *)calloc(1, sizeof(logring_t));

    logring_init(ret, ring_size, entry_size);

    return ret;
}

void
logring_init(logring_t *self, uint64_t ring_size, uint64_t entry_size)
{
    uint64_t   n;
    uint64_t   m;
    uint64_t   l;

    if (ring_size < LOGRING_MIN_SIZE) {
	ring_size = LOGRING_MIN_SIZE;
    }
    
    n = hatrack_round_up_to_power_of_2(ring_size);

    if (n > HATRACK_THREADS_MAX) {
	m = hatrack_round_up_to_power_of_2(HATRACK_THREADS_MAX << 1);
    }

    else {
	m = n << 1;
    }

    l                = sizeof(logring_entry_t) + entry_size;
    self->ring       = hatring_new(n);
    self->entries    = (logring_entry_t *)calloc(m, l);
    self->last_entry = m - 1;
    self->entry_ix   = 0;
    self->entry_len  = entry_size;
    
    return;
}

void
logring_cleanup(logring_t *self)
{
    hatring_delete(self->ring);
    free(self->entries);

    return;
}

void
logring_delete(logring_t *self)
{
    logring_cleanup(self);

    free(self);

    return;
}

void
logring_enqueue(logring_t *self, void *item, uint64_t len)
{
    uint64_t             ix;
    uint64_t             byte_ix;
    uint32_t             start_epoch;
    logring_entry_info_t expected;
    logring_entry_info_t candidate;
    logring_entry_t     *cur;

    if (len > self->entry_len) {
	len = self->entry_len;
    }

    logring_view_help_if_needed(self);
	
    while (true) {
	start_epoch = hatring_enqueue_epoch(atomic_read(&self->ring->epochs));
	ix          = atomic_fetch_add(&self->entry_ix, 1) & self->last_entry;
	expected    = empty_entry;
	byte_ix     = ix * (sizeof(logring_entry_t) + self->entry_len);
	cur         = (logring_entry_t *)&(((char *)self->entries)[byte_ix]);

	candidate.write_epoch = 0;
	candidate.state       = LOGRING_RESERVED;
    
	if (CAS(&cur->info, &expected, candidate)) {
	    break;
	}

	if (!logring_can_write_here(expected, start_epoch)) {
	    continue;
	}

	if (CAS(&cur->info, &expected, candidate)) {
	    break;
	}
    }

    memcpy(cur->data, item, len);
    
    candidate.write_epoch = hatring_enqueue(self->ring, (void *)ix);
    candidate.state       = LOGRING_ENQUEUE_DONE;
    cur->len              = len;

    atomic_store(&cur->info, candidate);

    return;
}

bool
logring_dequeue(logring_t *self, void *output, uint64_t *len)
{
    uint64_t             ix;
    uint64_t             byte_ix;
    uint32_t             epoch;
    bool                 found;
    logring_entry_info_t expected;
    logring_entry_info_t candidate;
    logring_entry_t     *cur;    

    logring_view_help_if_needed(self);
    
    while (true) {
	ix = (uint64_t)hatring_dequeue_w_epoch(self->ring, &found, &epoch);

	if (!found) {
	    return false;
	}

	byte_ix  = ix * (sizeof(logring_entry_t) + self->entry_len);
	cur      = (logring_entry_t *)&(((char *)self->entries)[byte_ix]);
	expected = atomic_read(&cur->info);

	while (logring_can_dequeue_here(expected, epoch)) {
	    candidate        = expected;
	    candidate.state |= LOGRING_DEQUEUE_RESERVE;

	    if (CAS(&cur->info, &expected, candidate)) {
		goto safely_dequeue;
	    }
	}
    }

 safely_dequeue:
    memcpy(output, cur->data, cur->len);

    *len                  = cur->len;
    expected              = candidate;
    candidate.write_epoch = expected.write_epoch;

    while (true) {
	candidate.state = logring_set_dequeue_done(candidate.state);
	
	if (CAS(&cur->info, &expected, candidate)) {
	    return true;
	}
    }
}

logring_view_t *
logring_view(logring_t *self)
{
    logring_view_t *ret;
    uint64_t        n;
    uint64_t        len;
    view_info_t     expected;
    view_info_t     candidate;

    n        = self->ring->size + HATRACK_THREADS_MAX;
    len      = sizeof(logring_view_t) + sizeof(logring_view_entry_t) * n;
    ret      = (logring_view_t *)mmm_alloc_committed(len);
    
    candidate.view = ret;

    while (true) {
	expected = atomic_read(&self->view_state);

	logring_view_help_if_needed(self);
	
	candidate.last_viewid = expected.last_viewid + 1;
	ret->start_epoch      = atomic_read(&self->ring->epochs);
	
	if (CAS(&self->view_state, &expected, candidate)) {
	    break;
	}
    } 

    logring_view_help_if_needed(self);

    return ret;
}

void *
logring_view_next(logring_view_t *view, uint64_t *len)
{
    logring_view_entry_t *cur;
    void                 *ret;
    
    while (view->next_ix < view->num_cells) {
	cur = &view->cells[view->next_ix];
	view->next_ix++;
	
	if (!cur->value) {
	    continue;
	}

	*len       = cur->len;
	ret        = cur->value;
	cur->value = NULL;  // Delete will go and free any unused items.
	
	return ret;
    }

    return NULL;
}

void
logring_view_delete(logring_view_t *view)
{
    logring_view_entry_t *cur;
    
    while (view->next_ix < view->num_cells) {
	cur = &view->cells[view->next_ix];
	view->next_ix++;

	if (cur->value) {
	    free(cur->value);
	}
    }
    
    mmm_start_basic_op();
    mmm_retire(view);
    mmm_end_op();

    return;
}

static void
logring_view_help_if_needed(logring_t *self)
{
    view_info_t           view_info;
    view_info_t           candidate_vi;
    logring_view_t       *view;
    uint64_t              vid;
    uint64_t              ring_epoch_info;
    uint64_t              vix;
    uint64_t              new_epoch;
    uint64_t              offset_entry_ix;
    uint64_t              entry_ix;
    uint64_t              exp_len;
    uint32_t              rix;
    uint32_t              end_ix;
    uint32_t              cell_epoch;
    hatring_item_t        ringcell;
    hatring_item_t        cand_cell;
    logring_view_entry_t *cur_view_entry;
    logring_entry_t      *data_entry;
    logring_entry_info_t  exp_de_info;
    logring_entry_info_t  cand_de_info;
    char                 *contents;
    char                 *exp_contents;
    
    mmm_start_basic_op();
    view_info = atomic_read(&self->view_state);

    if (!view_info.view) {
	return;
    }

    vid    = view_info.last_viewid;
    view   = view_info.view;
    vix    = 0;
    rix    = hatring_dequeue_epoch(view->start_epoch);
    end_ix = hatring_enqueue_epoch(view->start_epoch);

    while (rix < end_ix) {
	cur_view_entry = &view->cells[vix++];
	new_epoch      = atomic_read(&self->ring->epochs);
	end_ix         = hatring_enqueue_epoch(view->start_epoch);

	if (atomic_read(&cur_view_entry->value)) {
	    /* We could try to swing the end epoch forward some, but
	     * since we did it so recently, and are substantially behind,
	     * we won't bother here; we just move on to the next slot.
	     */
	    rix = rix + 1;
	    continue;
	}
	
	offset_entry_ix = atomic_read(&cur_view_entry->offset_entry_ix);

	if (offset_entry_ix) {
	    entry_ix = offset_entry_ix - 1;
	    goto got_entry_index;
	}

	ringcell   = atomic_read(logring_get_ringcell(self, rix));
	cell_epoch = hatring_cell_epoch(ringcell.state);
	
	/* If the cell epoch is lower than we expect, we should try to
	 * invalidate the slot, and try again if we fail.
	 *
	 * If the epoch is too high, we skip. Other threads might have
	 * serviced the spot, but if they didn't, or they don't manage to
	 * finish in time, then the cell will obviously be invalid in the
	 * view, and we will skip past it.
	 */
	
	while (cell_epoch < rix) {
	    cand_cell.state = HATRING_DEQUEUED | rix;
	    cand_cell.item  = NULL;
	    
	    if (CAS(logring_get_ringcell(self, rix), &ringcell, cand_cell)) {
		cell_epoch = rix;
		ringcell   = cand_cell;
		break;
	    }
	    
	    cell_epoch = hatring_cell_epoch(ringcell.state);
	}
	
	if ((cell_epoch > rix) || !(ringcell.state & HATRING_ENQUEUED)) {
	    /* The item we're looking for has been overwritten or
	     * removed.  But the contents might still be in the bigger
	     * array, if another thread was faster than us in getting
	     * to this point, so let's check again before we give up.
	     */

	    offset_entry_ix = atomic_read(&cur_view_entry->offset_entry_ix);

	    if (offset_entry_ix) {
		entry_ix = offset_entry_ix - 1;
		goto got_entry_index;
	    }

	    goto next_cell;
	}

	entry_ix = (uint64_t)ringcell.item;
	atomic_store(&cur_view_entry->offset_entry_ix, entry_ix + 1);
		     
    got_entry_index:
	/* Okay, phase 1 is complete; we have an index into the bigger
	 * array, but we still might not be able to read the entry.
	 * we need to acquire 'VIEW' access to it, and if we cannot,
	 * then that's the same to us as if we couldn't find anything
	 * in the ring.
	 *
	 * Note, if any thread manages to reserve for view access, all
	 * threads will be able to see that, as long as the current
	 * view is still active.
	 */
	
	data_entry  = logring_get_entry(self, entry_ix);
	exp_de_info = atomic_read(&data_entry->info);

	/* In order to be able to read, the write_epoch must be equal to
	 * the write epoch we're expecting to see for this entry,
	 * which will be the write epoch we expected to see in the ring.
	 *
	 * Also, the view_id will need to match with our view_id
	 * (otherwise, we are way behind and we should bail entirely, 
	 * because the view we were working on is done).
	 * 
	 * If the view ID is right but VIEW_RESERVE is off, then some
	 * thread successfully managed to copy this entry, and we can
	 * move on to the next one.
	 *
	 * The epoch should never be too low, because enquing writes
	 * to this array before inserting into the ring.  So any index
	 * we get into this array, should be of a fully written out
	 * item.
	 */

	while (true) {
	    if (logring_current_entry_epoch(exp_de_info, rix)) {
		if (exp_de_info.view_id > vid) {
		    mmm_end_op();
		    return;
		}
		
		if (exp_de_info.view_id < vid) {
		    cand_de_info.view_id     = vid;
		    cand_de_info.write_epoch = exp_de_info.write_epoch;
		    cand_de_info.state       = exp_de_info.state |
			LOGRING_VIEW_RESERVE;

		    if (!CAS(&data_entry->info, &exp_de_info, cand_de_info)) {
			continue;
		    }
		    exp_de_info = cand_de_info;
		} else {
		    if (!(exp_de_info.state & LOGRING_VIEW_RESERVE)) {
			goto next_cell;
		    }
		}
		break;
	    }
	    else {
		goto next_cell;
	    }
	}

	// Figure out how much to copy.
	exp_len = 0;
	if (CAS(&cur_view_entry->len, &exp_len, data_entry->len)) {
	    exp_len = data_entry->len;
	}
	
	/* Now we can read. If we are slow enough, we might read the
	 * wrong item (or even a corrupted item, due to a write in
	 * progress).  However, this wouldn't happen until AFTER a
	 * correct item gets installed, so it's not a problem.
	 */

	contents     = (char *)malloc(data_entry->len);
	exp_contents = NULL;
	
	memcpy(contents, data_entry->data, exp_len);

	
	if (!CAS(&cur_view_entry->value,
		 (void **)&exp_contents,
		 (void *)contents)) {
	    free(contents);
	}

	/* Now we have to flip VIEW_RESERVE off, if no other thread has,
	 * And then we can move to the next cell.
	 */
	cand_de_info = exp_de_info;
	cand_de_info.state &= ~LOGRING_VIEW_RESERVE;

	CAS(&data_entry->info, &exp_de_info, cand_de_info);
	    
    next_cell:
	ring_epoch_info = atomic_read(&self->ring->epochs);
	end_ix          = hatring_enqueue_epoch(view->start_epoch);
	rix             = rix + 1;
	continue;
    }

    // Late threads may see more pushed items, but too little, too
    // late.
    exp_len = 0;
    CAS(&view->num_cells, &exp_len, vix);
	
    // Now we 'finish' by swapping out the view_info w/ a null pointer.
    candidate_vi.view        = NULL;
    candidate_vi.last_viewid = view_info.last_viewid;
    
    CAS(&self->view_state, &view_info, candidate_vi);
    
    mmm_end_op();

    return;
}
