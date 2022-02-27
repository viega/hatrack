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
 *
 *  Very early on in building hatrack, I put together an in-memory
 *  logging mechanism that was based on a really simple, wait-free
 *  ring buffer.  In that, multiple threads can write in parallel to
 *  continuous fixed size buffers, by FAA'ing a value, and then
 *  computing the index by moding the value with the table size.
 *
 *  This has worked incredibly well for debugging; it's been very
 *  fast, and hasn't had enough of an impact on overhead to change the
 *  behavior of programs in a way to hide bugs, or anything like that.
 *
 *  However, there are some obvious limitations, for example:
 *
 *  1) We might find data is only partially written when we go to read
 *     it.
 *
 *  2) We might find data in a state of being partially
 *  overwritten... a slow writer might not even get their data fully
 *  written by the time overwriting starts.
 *
 *  Generally, this has not been a practical problem, given a very
 *  large ring buffer (the smallest one I've used is 19M, and have
 *  occasionally pushed it up to 1G when necessary).  I also generally
 *  dump and search the buffer durring debugging, when the buffer is
 *  necessarily static; or print it out after all my threads are done
 *  working.
 *
 *  Still, it would be nice to have a more robust option, where
 *  there are at least a couple of useful guarantees:
 *
 *  1) That entries can be get fully written out, and be at the head
 *     point when the write is done.
 *
 *  2) That, once a dequeue (or read) of an entry is started, we can
 *     guarantee that the operation will complete, without concerns of
 *     data corruption.
 *
 *  Logring solves those two problems, though certainly at the expense
 *  of speed.
 *
 *  First, we should note that we can't just use the hatring
 *  construction, because there, we must atomically remove items in
 *  their entirety, while maintaining some state, which basically
 *  limits us to items that are pointer-sized.
 *
 *  However, we can use it as a building block!
 *
 *  The basic idea is that we have two circular buffers, one buffer
 *  names R that is N items long that is an actual ring, and another
 *  named L that is not a full ring, but holds the actual log
 *  messages. The entries in R simply point to a spot in L.  Threads
 *  scan L like a ring to get a spot to write, but if they notice an
 *  operation in progress, they skip it until they find one that's
 *  safe to write, and guaranteed not to have R pointing into it.
 *
 *  To do that, L needs to have more entries than R-- it needs to have
 *  at least L + MAX_NUM_THREADS entries to guarantee that the queue
 *  could be full, and have room for every thread to be doing an
 *  operation at the same time.
 *
 *  In practice, we will prefer our powers of two... so if you ask for
 *  a ring buffer with L items, where L is a power of 2, we will
 *  actually reserve L * 2 items in R (assuming L is greater than
 *  MAX_NUM_THREADS).
 *
 *  Enqueuers do the following:
 *  
 *  1) Reserve a slot in L.
 *  2) Copy data into L.
 *  3) Enqueue a pointer to L into R.
 *  4) Write into L the epoch R used to enqueue.
 *  5) Update their slot in L to indicate they're done with their enqueue.
 *
 *  There will be no competition for the enqueuer's slot from other
 *  enqueuers.
 *
 *  However, a dequeuer can come in after step 3 completes and before
 *  step 4 completes.  The dequeuer could even finish the dequeue
 *  operation before the enqueuer finishes his operation.
 *
 *  That's not a problem for us-- the linearization point is the
 *  enqueue into R.  We just need to make sure that enqueuers can only
 *  claim a slot if BOTH enqueuers and dequeuers are done with their
 *  operation (and, if it's not in R, of course).
 *
 *  Dequeuers do the following:
 *
 *  1) Dequeue a value from R.
 *  2) Attempt to mark the cell in L for read.
 *  3) Perform the read.
 *  4) Mark the cell in L to indicate the read is done.
 *
 *  Note that a slow dequeuer might find that by the time they attempt
 *  to flag the cell in L for read, someone has already claimed that
 *  cell for writing a newer log message.  In such cases, dequeuers
 *  just need to try again.
 *
 *  We could add a help mechanism to ensure wait-freedom, but if our
 *  buffer is big enough, this is going to be a non-issue. So for the
 *  moment, this algorithm, despite being built on a wait-free ring,
 *  is not itself strictly wait-free.
 *
 *  The remaining question is how to ensure that we know when an
 *  enqueuer can safely take a spot that's been enqueued, but not
 *  dequeued.
 *
 *  Note that, above, we write an epoch from R into the slot from L.
 *  That means, when an enqueuer starts an operation, it can look at
 *  the current state of R, and calculate an epoch that it knows is
 *  safe to overwrite.  However, if that enqueuer is very slow, then
 *  they will refresh that state whenever they see a slot in L that's
 *  enqueued but with no started dequeue operation associated with it
 *  (and only if their current state indicates they CAN'T reclaim the
 *  spot).
 *
 *  We could consider adding a 'soft' reservation in so that other
 *  writers don't waste too much time in competing over the slot, but
 *  I don't think that's going to be impactful enough in practice,
 *  so I don't do it.
 *
 *  Additionally, we may want to be able to support threads "reading"
 *  from the thing without dequeuing from the ring, and we might want
 *  people to be able to scan either forward or backward through the
 *  ring (knowing there may be dequeues and enqueues that impact us).
 *
 *  Author:         John Viega, john@zork.org
 */

#ifndef __LOGRING_H__
#define __LOGRING_H__

#include <stdint.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <time.h>
#include <hatrack/hatrack_config.h>

#define LOGRING_MIN_SIZE 64

typedef struct {
    uint32_t        write_epoch;
    uint32_t        state;
} logring_entry_info_t;

typedef struct {
    alignas(8)
    _Atomic logring_entry_info_t info;
    uint64_t                     len;
    char                         data[];
} logring_entry_t;

enum {
    LOGRING_EMPTY           = 0x00,
    LOGRING_RESERVED        = 0x01,
    LOGRING_ENQUEUE_DONE    = 0x02,
    LOGRING_DEQUEUE_RESERVE = 0x04
};

typedef struct {
    _Atomic uint64_t          entry_ix;
    uint64_t                  last_entry;
    uint64_t                  entry_len;
    hatring_t                *ring;
    logring_entry_t          *entries;
} logring_t;

static inline bool
logring_entry_is_being_read(logring_entry_info_t info)
{
    if (info.state & LOGRING_DEQUEUE_RESERVE) {
	return true;
    }
    
    return false;
}

static inline bool
logring_can_write_here(logring_entry_info_t info, uint32_t my_write_epoch)
{
    if (logring_entry_is_being_read(info)) {
	return false;
    }
    
    if (info.write_epoch > my_write_epoch) {
	return false;
    }

    return true;
}

static inline bool
logring_can_dequeue_here(logring_entry_info_t info, uint32_t expected_epoch)
{
    if (info.write_epoch > expected_epoch) {
	return false;
    }

    return true;
}

logring_t *logring_new    (uint64_t, uint64_t);
void       logring_init   (logring_t *, uint64_t, uint64_t);
void       logring_cleanup(logring_t *);
void       logring_delete (logring_t *);
void       logring_enqueue(logring_t *, void *, uint64_t);
bool       logring_dequeue(logring_t *, void *, uint64_t *);

#endif
