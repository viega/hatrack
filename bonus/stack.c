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
 *                  We could devise something that is never going to
 *                  copy state when it needs to expand the underlying
 *                  store, breaking the stack up into linked
 *                  segments. For now, I'm not doing that, just to
 *                  keep things as simple as possible.
 *
 *                  Currently this is only going to be lock-free;
 *                  Pushes might need to retry if a pop invalidates
 *                  their cell, and that could happen continually.
 *
 *                  We could easily address this with a "help"
 *                  facility to caravan operations, but for now
 *                  we're going for simplicity and correctness.
 *
 *  Author:         John Viega, john@zork.org
 */

#include <hatrack.h>

static stack_store_t *hatstack_new_store        (uint64_t);
static inline void    hatstack_start_compression(stack_store_t *, hatstack_t *,
						  uint64_t);
static inline void    hatstack_help_compress    (stack_store_t *, hatstack_t *);
static stack_store_t *hatstack_grow_store       (stack_store_t *, hatstack_t *);
    
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

    if (prealloc < (1 << HATSTACK_MIN_STORE_SZ_LOG)) {
	prealloc = 1 << HATSTACK_MIN_STORE_SZ_LOG;
    }

    atomic_store(&self->store, hatstack_new_store(prealloc));
    
    self->compress_threshold = HATSTACK_DEFAULT_COMPRESS_THRESHOLD;

    return;
}

/* Since the stack can grow and shrink, we can't assume that the cell
 * we're writing into is totally empty, unless there's a migration in
 * progress. It could also be a pop or a compress.
 */
void
hatstack_push(hatstack_t *self, void *item)
{
    stack_store_t *store;
    uint64_t       head_state;
    uint64_t       cid;
    stack_item_t   candidate;
    stack_item_t   expected;
    

    candidate.item   = item;
    candidate.offset = 0;
    
    mmm_start_basic_op();

    store = atomic_read(&self->store);

    while (true) {
	/* We remove the CID from head_state, and shift it down to the
	 * lower bits, so we can detect when a compression is happening.
	 */
	
	head_state = atomic_fetch_add(&store->head_state, 1);
	cid        = head_state & HATSTACK_HEAD_ISOLATE_CID;
	cid      >>= 32;
	head_state = head_state & ~HATSTACK_HEAD_ISOLATE_CID;
	
	if (head_state >= store->num_cells) {
	    if (head_state & HATSTACK_HEAD_F_COMPRESSING) {
		hatstack_help_compress(store, self);
		continue;
	    }
	    /* Else, either we're ALREADY migrating, or we need to
	     * migrate, just go off and migrate, already, then retry
	     * the operation.
	     */
	    store = hatstack_grow_store(store, self);
	    continue;
	}

	expected = atomic_read(&store->cells[head_state]);

	if (expected.state & HATSTACK_MIGRATING) {
	    store = hatstack_grow_store(store, self);
	    continue;
	}

	if ((expected.state & COMPRESSION_MASK) >= cid) {
	    /* A compression could have started after our fetch and
	     * add; if it has, we could be REALLY slow and be multiple
	     * compressions behind.
	     *
	     * Instead of worrying about it, just continue; we'll
	     * bounce back up to the top, and see if we're still
	     * compressing.
	     */
	    continue;
	}

	/* While this isn't a compression, we want to make sure old,
	 * laggy compressions know that they're behind.  So instead of
	 * taking whatever was there, use the one we know what most
	 * recent.
	 */
	candidate.state = cid;
	    
	// Usually this will be uncontested, and if so, we are done.
	if (CAS(&store->cells[head_state], &expected, candidate)) {
	    mmm_end_op();
	    return;
	}

	/* If we couldn't CAS our item in, then we are either growing or
	 * compressing, as pushes never compete with each other. In
	 * that case, we head back up to the top.
	 */
	continue;
    }
}

void *
hatstack_pop(hatstack_t *self, bool *found)
{
    stack_store_t *store;
    stack_item_t   expected;
    stack_item_t   candidate;
    uint64_t       consecutive_pops;
    uint64_t       head_state;
    uint64_t       ix;
    uint64_t       cid;

    mmm_start_basic_op();

    store            = atomic_read(&self->store);
    candidate.item   = NULL;
    candidate.offset = 0;
			     
    
    while (true) {
	consecutive_pops = 0;
	head_state       = atomic_read(&store->head_state);
	cid              = head_state & HATSTACK_HEAD_ISOLATE_CID;
	cid            >>= 32;
	ix               = head_state & ~HATSTACK_HEAD_ISOLATE_CID;
	candidate.state  = HATSTACK_POPPED | cid;
	

	if (ix >= store->num_cells) {
	    if (ix & HATSTACK_HEAD_F_COMPRESSING) {
		hatstack_help_compress(store, self);
		continue;
	    }
	    store = hatstack_grow_store(store, self);
	    continue;
	}

	/* We only iterate through the top parts when we're retrying 
	 * after a grow or a migration.  When we come jump up to here,
	 * it's because we saw a POP and kept going down the stack.
	 */ 
    organic_next:
	ix -= 1;

	expected = atomic_read(&store->cells[ix]);

	if (expected.state & HATSTACK_MIGRATING) {
	    store = hatstack_grow_store(store, self);
	    continue;
	}

	if ((expected.state & COMPRESSION_MASK) >= cid) {
	    continue;
	}

	if ((expected.state & HATSTACK_POPPED)) {
	    if (!ix) {
		if (CAS(&store->head_state, &head_state, cid << 32)) {
		    if (found) {
			*found = false;
		    }
		    return NULL;
		}
		
		consecutive_pops++;
		
		if (consecutive_pops >= self->compress_threshold) {
		    hatstack_start_compression(store, self, head_state);
		}
		
		if (found) {
		    *found = false;
		}
		return NULL;
	    }
	    else {
		goto organic_next;
	    }
	}

	// We think we have something to pop, but there could be contention.
	if (CAS(&store->cells[ix], &expected, candidate)) {
	    if (found) {
		*found = true;
	    }

	    mmm_end_op();
	    
	    return expected.item;
	}

	/* We failed. The question is why. If could be due to another
	 * pop, a migration, or a compression.  It's also possible
	 * we'll see both a pop and a compression, but testing for the
	 * compression is a bit more expensive than testing for the
	 * pop, and we'll find out about the compression soon enough...
	 *
	 * That is, if we see POPPED, we ignore everything else and go
	 * to organic_next (or, return false if we're at ix 0).
	 */


	if (expected.state & HATSTACK_POPPED) {
	    if (!ix) {
		if (found) {
		    *found = false;
		}
		return NULL;
	    }
	    goto organic_next;
	}

	/* At this point, the CAS failed either because of a migration
	 * or compression; we'll go back up to the top to figure out
	 * why, and go help, if still appropriate.
	 */
	continue;
    }
}

void
hatstack_set_compress_threshold(hatstack_t *self, uint64_t threshold)
{
    self->compress_threshold = threshold;

    return;
}

static stack_store_t *
hatstack_new_store(uint64_t num_cells)
{
    stack_store_t *ret;
    uint64_t       alloc_len;

    alloc_len      = sizeof(stack_store_t) + num_cells * sizeof(stack_cell_t);
    ret            = (stack_store_t *)mmm_alloc(alloc_len);
    ret->num_cells = num_cells;

    return ret;
}

/* This is called when a thread notices that compression is necessary,
 * yet no compression seemed to be in progress when we read the head
 * state (as determined by having HATSTACK_HEAD_F_COMPRESSING set
 * in the head pointer).
 *
 * Note that pushes do NOT kick off compression, only pops do, and
 * only if they did a whole lot of popping in the face of competing
 * pushes (if there are no competing pushes, they just swing the head
 * pointer).
 */
static inline void
hatstack_start_compression(stack_store_t *store,
			   hatstack_t *top,
			   uint64_t expected)
{
    uint64_t desired;

    /* Flags aren't set if we're here.  Since the compression ID is
     * shifted 32 bits up to make room for the actual index of the
     * head, we add HATSTACK_HEAD_CID_ADD, which is 1 << 32.
     */
    desired  = expected + (HATSTACK_HEAD_F_COMPRESSING | HATSTACK_HEAD_CID_ADD);

    // If we've used the maximum compression ID, then we force a migration.
    if (desired & HATSTACK_HEAD_F_MIGRATING) {
	store = hatstack_grow_store(store, top);
    }

    while (!CAS(&store->head_state, &expected, desired)) {
	if (expected & HATSTACK_HEAD_F_MIGRATING) {
	    // We can give up; migration in progress will also compress.
	    return;
	}
	
	if (expected & HATSTACK_HEAD_F_COMPRESSING) {
	    // Another thread started a compression first. Go help.
	    hatstack_help_compress(store, top);
	    return;
	}
	
	/* If the compression ID is still less than ours, we keep
	 * trying on the CAS, because we lost to a push operation,
	 * not a compression or migrate.
	 *
	 * Otherwise, we were very late to the party, and we're done.
	 */
	if ((expected & HATSTACK_HEAD_ISOLATE_CID) >=
	    (desired & HATSTACK_HEAD_ISOLATE_CID)) {
	    return;
	}
    }
    
    hatstack_help_compress(store, top);

    return;
}

    /* We need to coordinate in-place compression, knowing that
     * other threads may be trying to help at the same time. Each
     * thread needs to be clear as to what the state of any node is,
     * and we need to make sure that, if a thread gets suspended for a
     * long time, and wakes up after a compression is done, they don't
     * try to proceed as if the compression still needs to happen.
     * In fact, we might have multiple compressions in quick succession.
     *
     * To support this, we will want to:
     *
     * 1) Write a "compression ID" into the state field, for cells
     *    that we mark, which increments once per compression in a
     *    store, so that we can detect when we're looking at something
     *    stale.  We will not remove this ID until a later compression
     *    operation.
     *
     *    The compression ID is the least significant 16 bits of
     *    the state field, and if the compression ID wraps around,
     *    we migrate instead.
     *
     *    We write the compression ID into cells right-to-left, from
     *    the (now locked) head, down to the point where SOME thread
     *    finds at least compress_threshold consecutive cells marked
     *    as popped. 
     *
     *    Note that, because we will be migrating the contents of
     *    cells, not every thread may see the full number of pops.
     *    Therefore:
     *
     * 2) We set a "BACKSTOP" bit in the cell to the left of the last
     *    pop (along w/ the compression ID), signaling  to other threads 
     *    that they don't need to go down any farther.  If the entire
     *    stack is empty, no BACKSTOP bit will be set.
     *
     * 3) As we compress, threads iterate, doing the following:
     *    a) Find the leftmost pop that is to the right of the 
     *       backstop.
     *    b) Find the leftmost value to the right of the leftmost pop.
     *    c) Based on the indexes in a and b, write into the value's 
     *       state the offset by which we want to move the value.
     *    d) Copy the contents in the value, into the popped location
     *       (including the compression ID).
     *    e) Replace the value we just coppied with a pop (again,
     *       including the compression ID).
     *
     * 4) When were are no more values to move, the rest of the values
     *    are pops.  Replace them with HATSTACK_OK, but leaving in the 
     *    compression ID.
     *
     * 5) Swap in the new location of head_state, also removing the 
     *
     *    Essentially, if we were doing this in a single-threaded
     *    manner, we're just swapping the HATSTACK_COMPRESSING bit.
     *
     * Threads can stall out and wake up at any point during this
     * process, so we need to make sure there's no ability to get
     * confused, and write the wrong data.
     *
     * First, note that, if a thread gets suspended, and doesn't wake
     * up until after the migration gets done, either:
     *
     * 1) They see that some slot has a "future" compression ID, in
     *    which case they know they're way behind, and can abandon 
     *    their work all together.
     *
     * 2) They see state with the current compression ID, but other
     *    than what they're expecting, in which case they know they're
     *    behind, and forego the operation.
     *
     * Also note that, once a cell has been involved in a compression,
     * the compression ID field will ALWAYS be in a cell.
     *
     * Let's consider the cases when a thread lags in the compression,
     * but is not so tardy as to see a new compression.
     *
     * They might stall when writing initial compression IDs and
     * counting pops. There's some chance they mis-count pops, but the
     * fastest thread will have added a backstop bit, and the backstop
     * bit would only get removed if it's overwritten when writing the
     * compression ID of another compression, in which case the thread
     * will notice that it's way behind.
     *
     * They might stall when trying to move items down the array. And,
     * the move is a two-phase process, since we cannot directly swap
     * two cells atomically.  We therefore either have to have two
     * copies of the data in the array for a limited time, or we need
     * to delete the item from the array and re-insert it. We do the
     * former, but either way could work.  We simply write the index
     * with which we're paired into the other cell, before doing the
     * swap. This ensures that threads coming in when the stack is in
     * an inconsistent state have a way of knowing whether they're
     * behind.
     *
     * Specifically, in our case, an array item X could both have
     * successfully been moved, and still in its own location. A late
     * arriving thread might find an empty bucket at index I, and
     * still see X. But the index written into X's cell won't be I, so
     * they'll know the cell is in the process of being deleted, and
     * attempt to help with the deletion before moving on.
     */

static inline void
hatstack_help_compress(stack_store_t *store, hatstack_t *top)
{
    uint32_t     max_index;
    uint32_t     read_cid;
    uint32_t     consecutive_pops;
    uint32_t     ix;
    uint32_t     scan_ix;
    uint32_t     offset;
    uint64_t     compressid;
    uint64_t     candidate_headstate;
    uint64_t     headstate;    
    stack_item_t expected;
    stack_item_t candidate;
    stack_item_t scanned;

    headstate  = atomic_read(&store->head_state);

    if (!(headstate & HATSTACK_HEAD_F_COMPRESSING)) {
	// Already done by the time we got here.
	return;
    }
			     
    max_index  = headstate & 0xffffffff;
    compressid = (headstate >> 32) & COMPRESSION_MASK; // Clear the top 2 bits.

    if (max_index >= store->num_cells) {
	max_index = store->num_cells - 1;
    }
    
    consecutive_pops = 0;
    ix               = max_index;

    /* This loop marks all the cells involved with the compression,
     * by swapping in the current compression sequence ID into
     * the cell. 
     *
     * Once we reach the backstop we can stop; and if we see enough
     * consecutive pops, we write the backstop (if necessary) into
     * the first NON-pop item.
     *
     * We're going to want to start compressing into popped slots
     * though, so when we leave the loop, make sure ix is pointing
     * to the popped slot, not the cell with the backstop.
     */
    while (true) {
	expected = atomic_read(&store->cells[ix]);
	read_cid = expected.state & COMPRESSION_MASK;

	if ((read_cid) > compressid) {
	    // We're at least a full compression behind.  Yikes!
	    return;
	}

	if (expected.state & HATSTACK_POPPED) {
	    consecutive_pops++;
	} else {
	    if (consecutive_pops < top->compress_threshold) {
		consecutive_pops = 0;
	    }
	}
	
	if (read_cid == compressid) {
	    if ((expected.state & HATSTACK_BACKSTOP) || !ix) {
		/* We found the backstop at the current index, or
		 * we're at the bottom of the stack.
		 */
		break;
	    }
	    ix--;
	    continue;
	}

	candidate.item    = expected.item;
	candidate.state   = compressid;
	candidate.offset  = 0;

	if (expected.state & HATSTACK_POPPED) {
	    candidate.state |= HATSTACK_POPPED;
	}
	else {
	    if (consecutive_pops >= top->compress_threshold) {
		candidate.state |= HATSTACK_BACKSTOP;
	    }
	}

	CAS(&store->cells[ix], &expected, candidate);
	if (candidate.state & HATSTACK_BACKSTOP) {
	    ix++;
	    break;
	}
	
	// Stack is empty, so no backstop was set.
	if (!ix) {
	    break;
	}
	
	ix--;
	continue;
    }

    scan_ix = ix + top->compress_threshold;

    while (scan_ix <= max_index) {
	expected = atomic_read(&store->cells[ix]);
	read_cid = expected.state & COMPRESSION_MASK;

	if ((read_cid) > compressid) {
	    return;
	}

	if (!(expected.state & HATSTACK_POPPED)) {
	    // Another thread is ahead of us and migrated
	    // something here.
	    ix++;
	    continue;
	}
	
	/* This loop scans for the first item we could possibly move.
	 * Note that, if we are slow, the item we should have moved
	 * into the slot at ix might be gone.  
	 *
	 * That's okay; we'll line ourselves up to the next available
	 * item, and will fail to swap it in at the end.  We will look
	 * to see if we were trying to swap in the wrong item, and NOT
	 * skip our scan_ix past the wrong item, if that's the case.
	 */
	while (true) {
	    scanned = atomic_read(&store->cells[scan_ix]);

	    // Make sure we're not TOO far behind.
	    if ((scanned.state & COMPRESSION_MASK) != compressid) {
		return;
	    }

	    if (scanned.state & HATSTACK_POPPED) {
	    scan_next_cell:
		scan_ix++;
		if (scan_ix > max_index) {
		    goto finish_up;
		}
		continue;
	    }
	    break;
	}
	offset = scan_ix - ix;
	
	/* If this condition is true, then some thread
	 * successfully coppied this cell, but has not finished
	 * replacing it with a pop.  We try to help them out.
	 */
	if (scanned.offset && (scanned.offset != offset)) {		
	    candidate.item   = NULL;
	    candidate.state  = compressid & HATSTACK_POPPED;
	    candidate.offset = 0;
	    CAS(&store->cells[scan_ix], &scanned, candidate);
	    
	    goto scan_next_cell;
	}
	
	/* If we're here, the current cell needs to be moved.
	 * First, if the offset isn't set, try to set it. If
	 * we fail, we got beat.
	 */
	
	if (!scanned.offset) {
	    candidate        = scanned;
	    candidate.offset = offset;
	    if (CAS(&store->cells[scan_ix], &scanned, candidate)) {
		scanned = candidate;
	    } else {
		if ((scanned.state & COMPRESSION_MASK) != compressid) {
		    return;
		}
	    }
	}
	
	/* Now, try to write the value from the scanned cell
	 * into the cell that's at ix. 
	 *
	 * The offset field gets coppied into the new slot to show
	 * where we coppied it from, allowing late threads to make
	 * sure they were working on the right item.
	 *
	 * That is, they could have gotten stalled after reading the
	 * item at ix, someone could have finished the move, and so
	 * the item at scan_ix is actually further up the array than
	 * the item that got coppied into the slot at ix.
	 */
	candidate.item   = scanned.item;
	candidate.state  = compressid;
	candidate.offset = offset;
	if (CAS(&store->cells[ix], &expected, candidate)) {
	    expected = candidate;
	}
	
	/* Now, try to replace the scanned cell w/ a pop.  Someone
	 * else may have done it already, and in fact, it could
	 * already be replaced with a "new" item if we're maximally
	 * compressed up to this cell.
	 */
	candidate.item   = NULL;
	candidate.state  = HATSTACK_POPPED | compressid;
	candidate.offset = 0;
	CAS(&store->cells[scan_ix], &scanned, candidate);
	
	// That cell's done; advance ix.
	ix++;

	/* If we were working on the wrong item, we will still move
	 * the current item, so don't advance scan_ix.
	 */
	if (expected.offset == offset) {
	    scan_ix++;
	    continue;
	}
	
	continue;
    }

 finish_up:
    /* ix is definitely now pointing to an empty item, which is where
     * the head state should always point.
     */
    candidate_headstate = (compressid << 32) | ix;
    CAS(&store->head_state, &headstate, candidate_headstate);

    return;
}

/* Migration is easier than compression; in fact, it operates pretty
 * similarly to how it's operated in our other algorithms.
 *
 * The only complication is that we could end up having a compression
 * operation start in parallel with a grow operation, which we handle
 * by using the head state as a gatekeeper in front of the operation.
 *
 * The migration first tries to get HATSTACK_HEAD_F_MIGRATING set.  If
 * it sees HATSTACK_HEAD_F_COMPRESSING instead, it goes off and helps
 * do that, and abandons the migration (it may get re-triggered on a
 * future push, but ideally the compression created some space).
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

    /* Since the migration is that last thing that will happen in this
     * store, we don't have to worry about setting the value of any of
     * the other head state, beyond the MIGRATING flag. 
     * 
     * And we only bail if we see someone managed to trigger a compression
     * before we triggered our migration... which compels up to help,
     * WITHOUT bothering to retry after.
     */
    do {
	if (head_state & HATSTACK_HEAD_F_COMPRESSING) {
	    hatstack_help_compress(store, top);
	    return store;
	}
	
	target_state = head_state | HATSTACK_HEAD_F_MIGRATING;
	
    } while (!CAS(&store->head_state, &head_state, target_state));

    /* If we're here, HATSTACK_HEAD_F_MIGRATING is set, and
     * HATSTACK_HEAD_F_COMPRESSING is NOT set. No compression is going
     * to compete at this point.  We basically stick to our usual
     * approach:
     *
     * 1) Mark all the buckets.
     * 2) Agree on a new store.
     * 3) Migrate the contents to the new store, marking the old
     *    buckets as fully moved as we do.
     * 4) Install the new store and clean up.
     */
    for (i = 0; i < store->num_cells; i++) {
	expected_item = atomic_read(&store->cells[i]);
	while (true) {
	    if (expected_item.state & HATSTACK_MIGRATING) {
		break;
	    }
	    if (expected_item.state & HATSTACK_POPPED) {
		candidate_item.item  = NULL;
		candidate_item.state |= HATSTACK_MIGRATING | HATSTACK_MOVED;
	    } else {
		candidate_item        = expected_item;
		candidate_item.state |= HATSTACK_MIGRATING;
	    }
	    if (CAS(&store->cells[i], &expected_item, candidate_item)) {
		break;
	    }
	}
    }

    expected_store = NULL;
    next_store     = hatstack_new_store(store->num_cells << 1);

    /* This is just to make sure threads know for sure whether
     * num_cells has been initialized, since a stack could legitimately
     * have 0 items. 
     *
     * COMPRESSING | MIGRATING is otherwise an invalid state, so we
     * use it to mean we're migrating INTO the store.
     */
    atomic_store(&next_store->head_state,
		 HATSTACK_HEAD_F_COMPRESSING | HATSTACK_HEAD_F_MIGRATING);
    
    if (!CAS(&store->next_store, &expected_store, next_store)) {
	mmm_retire_unused(next_store);
	next_store = expected_store;
    }

 help_move:
    for (i = 0, j = 0; i < store->num_cells; i++) {
	old_item = atomic_read(&store->cells[i]);

	if (old_item.state & HATSTACK_MOVED) {
	    if (!(old_item.state & HATSTACK_POPPED)) {
		j++;
	    }
	    continue;
	}
	
	expected_item.item   = NULL;
	expected_item.state  = 0;
	expected_item.offset = 0;

	// Clear out all the fields other than item (compression ID resets).
	candidate_item.item   = old_item.item;
	candidate_item.state  = 0;
	candidate_item.offset = 0;

	CAS(&next_store->cells[j++], &expected_item, candidate_item);

	candidate_item        = old_item;
	candidate_item.state |= HATSTACK_MOVED;
	
	CAS(&store->cells[i], &old_item, candidate_item);
    }

    /* Install head_state.  The new index will be j; nothing else should
     * be set.  0 is the right compression ID, and we don't want
     * either of the status bits set when we're done.
     */
    target_state = HATSTACK_HEAD_F_COMPRESSING | HATSTACK_HEAD_F_MIGRATING;
    head_state   = j;
    

    CAS(&next_store->head_state, &target_state, head_state);

    /* Finally, install the new store, opening the world back up for
     * pushes and pops.  Any late ops to the old store will still see
     * our state as "migrating", but will either quickly figure out
     * that the store has moved, or will go through the motions and do
     * no work, because every local cell is marked as moved.
     */
    if (CAS(&top->store, &store, next_store)) {
	mmm_retire(store);
    }

    return next_store;
}

