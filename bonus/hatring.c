#include <stdio.h>
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
 *  Name:           hatring.c
 *  Description:    A wait-free ring buffer.
 *
 *  Author:         John Viega, john@zork.org
 */

#include <hatrack.h>

#define HATRING_MINIMUM_SIZE 16

/* The overhead for a call to nanosleep should be probably a couple
 * hundred nanoseconds, so this seems like a reasonable starting point. 
 */
#define HATRING_STARTING_SLEEP_TIME  100

/* Sleep time can double 23 times while staying under a second.
 * We'll use this as a cap, even though we never expect to see this
 * much of a delay in the real world.
 */
#define HATRING_MAX_SLEEP_TIME 999999999

hatring_t *
hatring_new(uint64_t num_buckets)
{
    hatring_t *ret;
    uint64_t   alloc_len;
    
    num_buckets = hatrack_round_up_to_power_of_2(num_buckets);

    if (num_buckets < HATRING_MINIMUM_SIZE) {
	num_buckets = HATRING_MINIMUM_SIZE;
    }

    alloc_len = sizeof(hatring_cell_t) * num_buckets;
    ret       = (hatring_t *)calloc(1,sizeof(hatring_t) + alloc_len);

    ret->epochs    = (num_buckets << 32) | num_buckets;
    ret->last_slot = num_buckets - 1;
    ret->size      = num_buckets;

    return ret;
}

void
hatring_init(hatring_t *self, uint64_t num_buckets)
{
    num_buckets = hatrack_round_up_to_power_of_2(num_buckets);

    if (num_buckets < HATRING_MINIMUM_SIZE) {
	num_buckets = HATRING_MINIMUM_SIZE;
    }
    
    bzero(self, sizeof(hatring_t) + sizeof(hatring_cell_t) * num_buckets);

    self->last_slot = num_buckets - 1;

    return;
}

void
hatring_cleanup(hatring_t *self)
{
    uint64_t       i;
    hatring_item_t item;

    // This should never be necessary, but JUST in case.
    if (self->drop_handler) {
	for (i = 0; i < self->size; i++) {
	    item = atomic_read(&self->cells[i]);
	    if (hatring_is_enqueued(item.state)) {
		(*self->drop_handler)(item.item);
	    }
	}
    }

    return;
}

void
hatring_delete(hatring_t *self)
{
    hatring_cleanup(self);
    free(self);

    return;
}

uint32_t
hatring_enqueue(hatring_t *self, void *item)
{
    uint64_t       epochs;
    uint32_t       read_epoch;
    uint32_t       write_epoch;
    uint32_t       cell_epoch;        
    uint64_t       candidate_epoch;
    uint64_t       ix;
    hatring_item_t expected;
    hatring_item_t candidate;
    
    struct timespec sleep_time = {
	.tv_sec  = 0,
	.tv_nsec = HATRING_STARTING_SLEEP_TIME
    };

    candidate.item = item;

    while (true) {
	epochs      = atomic_read(&self->epochs);
	read_epoch  = hatring_dequeue_epoch(epochs);
	write_epoch = hatring_enqueue_epoch(epochs);

	while (hatring_is_lagging(read_epoch, write_epoch, self->size)) {
	    candidate_epoch = hatring_fixed_epoch(write_epoch + 1,
						  self->size);
	    
	    if (CAS(&self->epochs, &epochs, candidate_epoch)) {
		goto try_once;
	    }
	    
	    nanosleep(&sleep_time, NULL);

	    sleep_time.tv_nsec <<= 1;
	    if (sleep_time.tv_nsec > HATRING_MAX_SLEEP_TIME) {
		sleep_time.tv_nsec = HATRING_MAX_SLEEP_TIME;
	    }
	    
	    epochs      = atomic_read(&self->epochs);	    
	    read_epoch  = hatring_dequeue_epoch(epochs);
	    write_epoch = hatring_enqueue_epoch(epochs);
	}

	/* If we're confident we're going to lose to a writer who got
	 * FAA'd past us, don't bother writing.
	 */
	do {
	epochs      = atomic_fetch_add(&self->epochs, 1L << 32);
	read_epoch  = hatring_dequeue_epoch(epochs);
	write_epoch = hatring_enqueue_epoch(epochs);
	} while (write_epoch < read_epoch);
	
    try_once:
	ix          = write_epoch & self->last_slot;
	expected    = atomic_read(&self->cells[ix]);
	cell_epoch  = hatring_cell_epoch(expected.state);

	while (cell_epoch < write_epoch) {
	    candidate.state = HATRING_ENQUEUED | write_epoch;

	    if (CAS(&self->cells[ix], &expected, candidate)) {
		if (hatring_is_enqueued(expected.state) &&
		    self->drop_handler) {
		    (*self->drop_handler)(expected.item);
		}
		
		return write_epoch;
	    }

	    cell_epoch = hatring_cell_epoch(expected.state);
	}

	continue; 	// We were too slow, so we start again.
    }
}

void *
hatring_dequeue(hatring_t *self, bool *found)
{
    uint64_t       epochs;
    uint32_t       read_epoch;
    uint32_t       write_epoch;
    uint32_t       cell_epoch;    
    uint64_t       ix;
    hatring_item_t expected;
    hatring_item_t candidate;

    candidate.item = NULL;

    while (true) {
    try_again:
	epochs      = atomic_read(&self->epochs);
	read_epoch  = hatring_dequeue_epoch(epochs);
	write_epoch = hatring_enqueue_epoch(epochs);

	if (read_epoch >= write_epoch) {
	    return hatring_not_found(found);
	}
	
	epochs      = atomic_fetch_add(&self->epochs, 1);
	ix          = hatring_dequeue_ix(epochs, self->last_slot);
	read_epoch  = hatring_dequeue_epoch(epochs);
	write_epoch = hatring_enqueue_epoch(epochs);
	expected    = atomic_read(&self->cells[ix]);
	cell_epoch  = hatring_cell_epoch(expected.state);
	
	while (cell_epoch <= read_epoch) {
	    candidate.state = HATRING_DEQUEUED | read_epoch;
	    
	    if (CAS(&self->cells[ix], &expected, candidate)) {
		if (cell_epoch == read_epoch) {
		    if (hatring_is_enqueued(expected.state)) {
			return hatring_found(expected.item, found);
		    }
		    goto try_again; // We beat an enqueuer.
		}

		if (read_epoch >= write_epoch) {
		    return hatring_not_found(found);
		}
		/* We might find an unread enqueued item if the
		 * dequeuer catches up to the writer while it's
		 * writing (e.g., if a thread is suspended).  It's
		 * also why we need to apply the drop handler during
		 * clean-up.
		 */
		if (hatring_is_enqueued(expected.state) &&
		    self->drop_handler) {
		    (*self->drop_handler)(expected.item);
		}
		continue;
	    }

	    cell_epoch = hatring_cell_epoch(expected.state);	    
	}
	continue;  	// we got lapped.
    }
}

void *
hatring_dequeue_w_epoch(hatring_t *self, bool *found, uint32_t *epoch)
{
    uint64_t       epochs;
    uint32_t       read_epoch;
    uint32_t       write_epoch;
    uint32_t       cell_epoch;    
    uint64_t       ix;
    hatring_item_t expected;
    hatring_item_t candidate;

    candidate.item = NULL;

    while (true) {
    try_again:
	epochs      = atomic_read(&self->epochs);
	read_epoch  = hatring_dequeue_epoch(epochs);
	write_epoch = hatring_enqueue_epoch(epochs);

	if (read_epoch >= write_epoch) {
	    return hatring_not_found(found);
	}
	
	epochs      = atomic_fetch_add(&self->epochs, 1);
	ix          = hatring_dequeue_ix(epochs, self->last_slot);
	read_epoch  = hatring_dequeue_epoch(epochs);
	write_epoch = hatring_enqueue_epoch(epochs);
	expected    = atomic_read(&self->cells[ix]);
	cell_epoch  = hatring_cell_epoch(expected.state);
	
	while (cell_epoch <= read_epoch) {
	    candidate.state = HATRING_DEQUEUED | read_epoch;
	    
	    if (CAS(&self->cells[ix], &expected, candidate)) {
		if (cell_epoch == read_epoch) {
		    if (hatring_is_enqueued(expected.state)) {
			*epoch = read_epoch;
			return hatring_found(expected.item, found);
		    }
		    goto try_again; // We beat an enqueuer.
		}

		if (read_epoch >= write_epoch) {
		    return hatring_not_found(found);
		}
		/* We might find an unread enqueued item if the
		 * dequeuer catches up to the writer while it's
		 * writing (e.g., if a thread is suspended).  It's
		 * also why we need to apply the drop handler during
		 * clean-up.
		 */
		if (hatring_is_enqueued(expected.state) &&
		    self->drop_handler) {
		    (*self->drop_handler)(expected.item);
		}
		continue;
	    }

	    cell_epoch = hatring_cell_epoch(expected.state);	    
	}
	continue;  	// we got lapped.
    }
}

void
hatring_set_drop_handler(hatring_t *self, hatring_drop_handler func)
{
    self->drop_handler = func;

    return;
}
