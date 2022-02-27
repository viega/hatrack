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
	candidate.state &= ~LOGRING_DEQUEUE_RESERVE;
	
	if (CAS(&cur->info, &expected, candidate)) {
	    return true;
	}
    }
}
