/*
 * Copyright © 2021-2022 John Viega
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *  Name:           mmm.h
 *  Description:    Miniature memory manager: a malloc wrapper to support
 *                  linearization and safe reclaimation for my hash tables.
 *
 *  Author:         John Viega, john@zork.org
 *
 */

#ifndef __MMM_H__
#define __MMM_H__

#include "hatrack_config.h"
#include "debug.h"
#include "counters.h"
#include "hatomic.h"
#include <stdlib.h>
#include <stdbool.h>
#include <pthread.h>
#include <stdalign.h>

/* This type represents a callback to de-allocate sub-objects before
 * the final free for an object allocated via MMM.
 *
 * We use it any time cleanup activity for objects not allocated via
 * mmm also need to defer cleanup. For instance, anything using locks
 * on a per-store basis will use this cleanup function, and have a
 * handler run through to destroy the locks.
 */
typedef void (*mmm_cleanup_func)(void *);

typedef struct mmm_header_st    mmm_header_t;
typedef struct mmm_free_tids_st mmm_free_tids_t;

/* We don't want to keep reservation space for threads that don't need
 * it, so we issue a threadid for each thread to keep locally, which
 * is an index into that array.
 *
 * If desired, threads can decide to "give back" their thread IDs, so
 * that they can be re-used, for instance, if the thread exits, or
 * decides to switch roles to something that won't require it.  If you
 * never run out of TID space, this will not get used; see
 * HATRACK_THREADS_MAX in config.h.
 */

// clang-format off
extern __thread int64_t        mmm_mytid;
extern __thread pthread_once_t mmm_inited;
extern _Atomic  uint64_t       mmm_epoch;
extern          uint64_t       mmm_reservations[HATRACK_THREADS_MAX];

/* The header data structure. Note that we keep a linked list of
 * "retired" records, which is the purpose of the field 'next'.  The
 * 'data' field is a mere convenience for returning the non-hidden
 * part of an allocation.
 *
 * Note that, when we perform an overwrite, since we want to preserve
 * ordering, we need to know the original create time. Since we want
 * to be able to free the old records from the original creation,
 * we need to cache that time in the create_epoch field.
 *
 */
// clang-format off
struct mmm_header_st {
    alignas(16)
    mmm_header_t    *next;
    _Atomic uint64_t create_epoch;
    _Atomic uint64_t write_epoch;
    uint64_t         retire_epoch;
    mmm_cleanup_func cleanup;
    alignas(16)
    uint8_t          data[];
};

struct mmm_free_tids_st {
    mmm_free_tids_t *next;
    uint64_t         tid;
};

void mmm_register_thread     (void);
void mmm_reset_tids          (void);
void mmm_retire              (void *);
void mmm_clean_up_before_exit(void);

#ifdef HATRACK_DEBUG
static inline void hatrack_debug_mmm(void *, char *);

#define DEBUG_MMM(x, y) hatrack_debug_mmm((void *)(x), y)
#ifdef HATRACK_MMM_DEBUG
#define DEBUG_MMM_INTERNAL(x, y) DEBUG_MMM(x, y)
#else
#define DEBUG_MMM_INTERNAL(x, y)
#endif
#else
#define DEBUG_MMM(x, y)
#define DEBUG_MMM_INTERNAL(x, y)
#endif

#ifdef HATRACK_MMMALLOC_CTRS
#define HATRACK_MALLOC_CTR()        HATRACK_CTR(HATRACK_CTR_MALLOCS)
#define HATRACK_FREE_CTR()          HATRACK_CTR(HATRACK_CTR_FREES)
#define HATRACK_RETIRE_UNUSED_CTR() HATRACK_CTR(HATRACK_CTR_RETIRE_UNUSED)
#else
#define HATRACK_MALLOC_CTR()
#define HATRACK_FREE_CTR()
#define HATRACK_RETIRE_UNUSED_CTR() HATRACK_CTR(HATRACK_CTR_RETIRE_UNUSED)
#endif

// clang-format on

/* This epoch system was inspired by my research into what was out
 * there that would be faster and easier to use than hazard
 * pointers. Particularly, IBR (Interval Based Reclaimation) seemed to
 * be about what I was looking for at the time, although since, I've
 * had to adapt it to my needs.
 *
 * In both algorithms, readers commit to an epoch at the time they
 * first enter an "operation". But with my algorithm, writers both
 * commit to a read epoch when they first start reading the data
 * structure, but they don't commit to a "write epoch" until the end
 * of their operation. This is important in our ability to have a
 * linear order for write operations, because writes may not complete
 * in the order they started.
 *
 * When I had the epiphany that I could fully linearize my hash table
 * if the write doesn't "commit" until a specific epoch, and those
 * commits are guaranteed to be ordered by epoch, the challenge was to
 * do this in a way that's atomic.
 *
 * To understand issues that arise, consider the typical (pseudo-)code
 * for setting an allocation epoch:
 *
 * write_epoch = atomic_fetch_and_add(epoch, 1) + 1
 *
 * The extra + 1 here is because a FAA operation returns the original
 * value, not the new epoch.  The write reall should happen on the
 * boundary of the old epoch and the new epoch, if there are are
 * readers looking at that epoch already (since they might have
 * already passsed by our node in their work).
 *
 * Actually, that's not strictly true, because we could force every
 * access, including reads, to create a new epoch.  If, we do this, we
 * are guaranteed that the write's committment epoch will always be
 * unique, and that any nearby readers will therefore see what they
 * should see.
 *
 * However, if we want to reduce memory barriers on the epoch counter
 * (which is desirable), we must consider writes to happen at the END
 * of their epoch, since readers that happened to grab the bumped
 * epoch before we commit our write could theoretically not see that
 * the record was active in their epoch.  That's the approach we take
 * here, especially considering that, typically, most hash table
 * access is done by readers.
 *
 * Another issue we have with the attempt to commit writes to an epic,
 * is that the store into the memory associated with the variable
 * write_epoch is NOT atomic (even without the +1). So the committing
 * thread could get suspended.
 *
 * And, while that thread is suspended, a reader that needs
 * linearization can come along, trying to read from a later epoch,
 * even though our commit hasn't happened yet.
 *
 * Our solution to this problem is to not allow readers to pass a
 * memory record that they see, where there is no epoch
 * committment. Instead, they must try to help, by also creating a
 * "new" epoch, and trying to write it to write_epoch via compare and
 * swap.  Since all readers will do this, and since any CAS failure
 * means some previous CAS succeded, it means this record will have an
 * epoch before any operation tries to process it.
 *
 * To clarify, for the writer inserting into the table, here's what the
 * process looks like:
 *
 * 1) Allocate the record, with no epoch data set.
 *
 * 2) Put it in the place where it belongs in a data structure,
 *    pointing to whatever record it replaces, if it exists.
 *
 * 3) Bump the epoch counter, which will also return the current
 *    epoch, which we again add 1 to, so that our number matches the
 *    epoch we created.
 *
 * 4) Attempt to write the value of the epoch we created into the
 *    write_epoch field, via compare-and-swap.
 *
 * 5) If the compare-and-swap fails, no big deal... it means the write
 *    has a later epoch than the one we were going to give it, but one
 *    more reflective of when the commit finally occured. And, there
 *    is guaranteed linearization before any other thread attempts to read
 *    the value in this record.
 *
 * Note that, as I was working this approach out, I realized there is
 * a fundamental race condition in the IBR approach as-described, that
 * makes it inappropriate for any kind of linearization.  In the
 * simplist version of their algorithm, they have a function
 * start_op() meant for every thread to use when starting a series of
 * reads OR writes to a data structure. Their pseudo-code for this
 * operation is:
 *
 * start_op(): reservations[tid] = epoch
 *
 * As with the above code, the entire operation cannot be done
 * atomically, so far as I'm aware. That means, the following scenario
 * is possible:
 *
 * 1) Our thread runs start_op(), and reads epoch N.
< * 2) Our thread gets SUSPENDED before recording its reservation.
 * 3) In epoch N+1, another thread retires a piece of the data structure
 *     that was alive in epoch N.
 * 4) Since no reservations are currently recorded for epoch N, the
 *    thread deletes the memory it just retired.
 * 5) Our thread wakes back up in epoch N+1, and attempts to read data
 *    that was available in epoch N.
 *
 * Certainly, in this case, we trust the other thread maintained the
 * data structure properly, so that we don't attempt to load a record
 * that's been freed. However, it's clear that we cannot even be
 * guaranteed that a reader who reserves epoch N will be able to
 * access every record that was theoretically alive in epoch N.
 *
 * For our epoch system, we address this problem by forcing the reader
 * to check to make sure the epoch hasn't changed after writing to the
 * array (if it hasn't changed, we are safe).
 *
 * Such a check on its own is not wait-free, because it's possible
 * that, every time we go to look at the epoch, a new write has since
 * occured, ad infinitum. We have two options for "enhancing" this
 * algorithm to make it wait-free:
 *
 * 1) We can recognize that this is probably never going to be a
 *    scenario that's likely to cause significant spinning for the
 *    thread, and specify that no writes are ever allowed under any
 *    circumstances, once the epoch counter gets to its maximum
 *    value. The fact that there's a bounded maximum wait time makes
 *    this algorithm wait-free, even though the bound is exceptionally
 *    high (2^64).
 *
 * 2) We can be conservative, and be willing to pay the performance
 *    penalty to avoid significant spinning in heavy-write
 *    situations. In such a case, we make a small number of attempts
 *    (say, 3), and then ask writers to "help" us, with the following
 *    algorithm:
 *
 *      a) In each item of the "reservations" array, we reserve one
 *         bit of each thread's reservation field as a "HELP" bit.
 *
 *         When a reader hits the maximum retries, he sets his own
 *         HELP bit, and atomically bumps a global "help" counter (with
 *         fetch and add.
 *
 *      b) Writers check the global help counter before every
 *         write. If it's non-zero, then they look at EVERY value in
 *         the reservation array before commencing any write operation
 *         (keeping them from creating a new epoch), looking for the
 *         help bit. If they see the help bit, they attempt to read
 *         the current epoch, and swap it into the reservation field.
 *         Then, the thread also checks against the current epoch,
 *         retrying if the epoch increases.  If the swap succeeds, and
 *         the epoch didn't increase in the process, then the writer
 *         removes 1 from the global help counter.
 *
 *         Once there's a successful epoch reservation, any writer
 *         attempting to help continues to proceed through the array,
 *         until they get to the end of the array, or the help count
 *         becomes zero (whichever occurs first).
 *
 *      c) The reader alternates between two operations:
 *
 *         i) Attempting to read an epoch from the gloval epoch counter.
 *         ii) Reading its reservation bucket (atomically).
 *
 *         If it manages to read an epoch itself, it still has to attempt
 *         to commit it, and test is as before. If it succeeds in helping
 *         itself, it also decrements the help counter.
 *
 *    Swaps in this algorithm should always set the "expected" value to
 *    the "No reservation" value (0), OR'd with the "HELP" bit, which
 *    would be the most significant bit.
 *
 *    It should be easy to see that this algorithm is also wait-free,
 *    bounded by the total number of writer threads. In the worst
 *    case, let's assume there are N writer threads that all start
 *    writing before thread T asks for help.  Any future thread that
 *    comes through with a second write operation, will NOT bump the
 *    epoch until it ensures that thread T got a safe epoch. We could
 *    end up in a situation where all N writer threads are helping T
 *    get a safe epoch-- in such a case, no thread can possibly be
 *    incrementing the epoch counter, bounding the work to O(N), thus
 *    making the algorithm wait-free.
 *
 * The first algorithm should be more performant in the expected case,
 * since checking a "help" counter will generally be a waste. If we
 * want to make the second algorithm even less likely to fire, we can
 * use a "deferred help" strategy, where writers only help if help is
 * still needed when a thread goes through the reclaimation process.
 *
 *
 * In the below enumeration:
 *
 * The first value will be stored in the reservations array when
 * threads do not have an epoch they've reserved for reading.
 *
 * The second is the first value of the epoch counter, which is 1.
 * There will never be any data in this epoch (any writer would bump
 * the epoch as part of their commit process).
 *
 * The third value is if we choose to implement the algorithm above to
 * help with epoch acquisitions -- it's the value that readers put
 * into their reservation to signal for help. This does reduce the
 * space for the epoch counter to 63 bits, which is still vastly more
 * epochs than any program could ever use in practice (using 1B epochs
 * per second, we'd have enough epochs for almost 300 years).
 *
 * The final value is used when we're looking to free old records. We
 * initialize the maximum value seen to this value, then go through
 * the reservations array, reducing the maximum value seen as we go
 * along.  If it's still this value by the time we get to the end,
 * then we can delete all our records.
 */

enum64(mmm_enum_t,
       HATRACK_EPOCH_UNRESERVED   = 0xffffffffffffffff,
       HATRACK_EPOCH_FIRST        = 0x0000000000000001,
       HATRACK_F_RESERVATION_HELP = 0x8000000000000000,
       HATRACK_EPOCH_MAX          = 0xffffffffffffffff);

/* This is a macro that allows us to access our hidden header.  Note
 * that, while this is straightforward, I did run into an issue where
 * the definition of mmm_header_t would end up getting a different
 * layout depending on the module.
 *
 * The solution was to force alignment of fields as needed in
 * mmm_header_t (above).
 */
static inline mmm_header_t *
mmm_get_header(void *ptr)
{
    return (mmm_header_t *)(((uint8_t *)ptr) - sizeof(mmm_header_t));
}

#ifdef HATRACK_DEBUG
/* Conceptually, this might belong in the debug subsystem. However,
 * this is a specific debug interface for outputting the epoch info
 * associated with an MMM allocation, and as such should live
 * independently from the more generic debug mechanism.
 *
 * As with the debug routines in the debug subsystem, we've sacrificed
 * readability for performance, since allocations can be VERY frequent
 * in test code.
 *
 * Specifically, I was originally just using sprintf() and it was
 * leading to a dramatic slowdown when debugging was on.
 */
static inline void
hatrack_debug_mmm(void *addr, char *msg)
{
    char buf[HATRACK_DEBUG_MSG_SIZE] = {
        '0',
        'x',
    };
    static const char rest[] = " :)  :r , :w , :c( :x0";
    const char       *r      = &rest[0];
    mmm_header_t     *hdr    = mmm_get_header(addr);
    char             *p;
    uint64_t          i;
    uintptr_t         n;

    p = buf + (HATRACK_PTR_CHRS + 3 * HATRACK_EPOCH_DEBUG_LEN + sizeof(rest));

    strncpy(p, msg, HATRACK_DEBUG_MSG_SIZE - (p - buf));

    *--p = *r++;
    *--p = *r++;
    *--p = *r++;
    n    = hdr->retire_epoch;

    for (i = 0; i < HATRACK_EPOCH_DEBUG_LEN; i++) {
        *--p = __hatrack_hex_conversion_table[n & 0xf];
        n >>= 4;
    }

    *--p = *r++;
    *--p = *r++;
    *--p = *r++;
    *--p = *r++;
    *--p = *r++;
    *--p = *r++;
    n    = hdr->write_epoch;

    for (i = 0; i < HATRACK_EPOCH_DEBUG_LEN; i++) {
        *--p = __hatrack_hex_conversion_table[n & 0xf];
        n >>= 4;
    }

    *--p = *r++;
    *--p = *r++;
    *--p = *r++;
    *--p = *r++;
    *--p = *r++;
    n    = hdr->create_epoch;

    for (i = 0; i < HATRACK_EPOCH_DEBUG_LEN; i++) {
        *--p = __hatrack_hex_conversion_table[n & 0xf];
        n >>= 4;
    }

    *--p = *r++;
    *--p = *r++;
    *--p = *r++;
    *--p = *r++;
    *--p = *r++;
    *--p = *r++;
    n    = (intptr_t)addr;

    for (i = 0; i < HATRACK_PTR_CHRS; i++) {
        *--p = __hatrack_hex_conversion_table[n & 0xf];
        n >>= 4;
    }

    *--p = *r++;
    *--p = *r++;

    hatrack_debug(p);

    return;
}
#endif

/* We stick our read reservation in mmm_reservations[mmm_mytid].  By
 * doing this, we are guaranteeing that we will only read data alive
 * during or after this epoch, until we remove our reservation.
 *
 * It does NOT guarantee that we won't read data written after the
 * reserved epoch, and does not ensure linearization (instead, use
 * mmm_start_linearized_op).
 *
 * The call to pthread_once would be better served if it moved to
 * thread initialization, but since we want to be as agnostic as
 * possible to the threading environment, we'll go ahead and pay the
 * (admittedly very small) cost of the implied test with each
 * operation.
 */
static inline void
mmm_start_basic_op(void)
{
    pthread_once(&mmm_inited, mmm_register_thread);
    mmm_reservations[mmm_mytid] = atomic_load(&mmm_epoch);

    return;
}

/* mmm_start_linearized_op() is used to help ensure we can safely recover
 * a consistent, full ordering of data objects in the lohat family.
 *
 * 1) When getting a read epoch that needs to be fully ordered, we
 *    make a reservation. But, we can't rely on that reservation alone
 *    for linearization. Particularly, if we're very slow to write our
 *    reservation to the reservations array, items could get retired
 *    in the same epoch, and freed, all before we complete our
 *    reservation (if the epoch advances while we're suspended, for
 *    instance).
 *
 *    That is, once the reservation is in place, it ensures nothing
 *    that's still in the table from that epoch or later is
 *    removed. However, this race condition means that it does not
 *    ensure that nothing EVER alive in that epoch won't be removed.
 *
 *    Checking the reservation again will give us a useful upper
 *    bound. We know that nothing retired from that epoch on will be
 *    freed before we're done with our linearization operation.  But,
 *    there might be a write operation in the current epoch, and that
 *    operation might actually end up delayed.
 *
 *    However! We have a restriction that there can be at most ONE
 *    write per epoch.  Therefore, we can go ahead and use this epoch
 *    to linearize. Either we will see the new write, in which case
 *    our linearization point is conceptionally the instant after the
 *    write (but before any retires), or we won't see the new write,
 *    in which case our linearization point is effectively the very
 *    end of the previous epoch, after any retires.
 *
 *    Even if there were retires in this new epoch, it won't matter;
 *    retires in the same epoch of creation never happen, since the
 *    writer reserves an epoch equal to or previous to that epoch, and
 *    creates a new epoch at the end.  And, by this point, we will
 *    have a reservation in the table, so writers in future epochs
 *    won't free from underneath us either.
 *
 *    When full ordering is necessary, readers should use the function
 *    mmm_start_linearized_op(), instead of mmm_start_op().  They do
 *    still use mmm_end_op() to end their operation and give up their
 *    reservation.
 *
 * 2) Whenever the reader finds a record w/ no write epoch, it must
 *    try to help get the record a definitive epoch before proceeding.
 *    If the stalled thread beats the linearized reader, we will see
 *    an epoch lower than ours, and will then read the
 *    record. Otherwise, the record write epoch will be higher than
 *    our read epoch, and we will not read. As part of our helping, we
 *    bump the write epoch, as all writes must occur on an epoch
 *    boundary, with only one write per epoch.
 */
static inline uint64_t
mmm_start_linearized_op(void)
{
    uint64_t read_epoch;

    pthread_once(&mmm_inited, mmm_register_thread);

    mmm_reservations[mmm_mytid] = atomic_load(&mmm_epoch);
    read_epoch                  = atomic_load(&mmm_epoch);

    HATRACK_YN_CTR_NORET(read_epoch == mmm_reservations[mmm_mytid],
                         HATRACK_CTR_LINEAR_EPOCH_EQ);

    return read_epoch;
}

/* This simply removes our reservation, indicating we are no longer
 * performing a data structure operation.
 */
static inline void
mmm_end_op(void)
{
    mmm_reservations[mmm_mytid] = HATRACK_EPOCH_UNRESERVED;

    return;
}

/* Note that the API for allocating via MMM is a little non-intuitive.
 * for malloc users, partially because it supports a couple of
 * different use cases:
 *
 * 1) Managing epochs for purposes of safe record management (and
 *    sorting) when linearization is NOT important. Here, we are
 *    good to set our write epoch at allocation time.
 *
 * 2) Managing epochs for both safe memory management, as well as full
 *    linearization. In such a case, we don't want to commit the write
 *    time until the end of our operation.
 *
 * mmm_alloc() is the interface for use case #2. It returns allocated
 * memory of the specific size, but without any committment. The
 * algorithm must call mmm_commit_write(), which is below.
 *
 * mmm_alloc_committed() is more like traditional malloc in that you
 * don't have to do any additional work, once you call it.
 *
 * I named the functions this way to help myself minimize programmer
 * errors. I felt if I swapped the functions (i.e., using an
 * mmm_alloc_uncommitted() function), then I was VERY likely to miss
 * errors where I forgot to commit, when building the fully linearized
 * algorithms, because the records would end up with epochs... just
 * ones that are wrong.
 *
 * The way it's done here, epochs that never get set tend to lead to
 * errors quickly, and we can easily add debugging to check the
 * condition if need be (though we need to be cognizent of possible
 * 'helpers').
 */
static inline void *
mmm_alloc(uint64_t size)
{
    uint64_t      actual_size = sizeof(mmm_header_t) + size;
    mmm_header_t *item        = (mmm_header_t *)calloc(1, actual_size);

    HATRACK_MALLOC_CTR();
    DEBUG_MMM_INTERNAL(item->data, "mmm_alloc");

    return (void *)item->data;
}

/*
 * Note here that atomic_fetch_add() returns the value before the add,
 * so we add one to it when storing our write epoch.
 */
static inline void *
mmm_alloc_committed(uint64_t size)
{
    uint64_t      actual_size = sizeof(mmm_header_t) + size;
    mmm_header_t *item        = (mmm_header_t *)calloc(1, actual_size);

    atomic_store(&item->write_epoch, atomic_fetch_add(&mmm_epoch, 1) + 1);

    HATRACK_MALLOC_CTR();
    DEBUG_MMM_INTERNAL(item->data, "mmm_alloc_committed");

    return (void *)item->data;
}

/* Cleanup handlers get called right before an allocation is freed.
 * They're used for sub-objects that aren't allocated via mmm, such as
 * mutex objects.
 */
static inline void
mmm_add_cleanup_handler(void *ptr, void (*handler)(void *))
{
    mmm_header_t *header = mmm_get_header(ptr);

    header->cleanup = handler;

    return;
}
/* When we called mmm_alloc(), we call this to set a write epoch.
 * Note that we do it via CAS, instead of a direct write, for the sake
 * of other threads that need to read the epoch information, in case
 * our thread stalls.
 *
 * Those other threads might need epoch information from our record
 * after we've placed a record, but before we've finished this call to
 * mmm_commit_write(). If a thread will need epoch information from
 * us, it will first call mmm_help_commit(), which is similar, but
 * first tests to make sure an epoch hasn't been written.
 */
static inline void
mmm_commit_write(void *ptr)
{
    uint64_t      cur_epoch;
    uint64_t      expected_value = 0;
    mmm_header_t *item           = mmm_get_header(ptr);

    cur_epoch = atomic_fetch_add(&mmm_epoch, 1) + 1;

    /* If this CAS operation fails, it can only be because:
     *
     *   1) We stalled in our write commit, some other thread noticed,
     *       and helped us out.
     *
     *   2) We were trying to help, but either the original thread, or
     *      some other helper got there first.
     *
     * Either way, we're safe to ignore the return value.
     */
    LCAS(&item->write_epoch, &expected_value, cur_epoch, HATRACK_CTR_COMMIT);
    DEBUG_MMM_INTERNAL(ptr, "committed");

    return;
}

/* Similar to mmm_commit_write(), but called by a different thread on
 * a pointer where it wants to ensure there's an epoch value. If it
 * doesn't see one, it attempts to install one. Whether it succeeds or
 * not, one is guaranteed to be there after the compare-and-swap is
 * done.
 */
static inline void
mmm_help_commit(void *ptr)
{
    mmm_header_t *item;
    uint64_t      found_epoch;
    uint64_t      cur_epoch;

    item        = mmm_get_header(ptr);
    found_epoch = item->write_epoch;

    if (!found_epoch) {
        cur_epoch = atomic_fetch_add(&mmm_epoch, 1) + 1;
        LCAS(&item->write_epoch,
             &found_epoch,
             cur_epoch,
             HATRACK_CTR_COMMIT_HELPS);
    }

    return;
}

// Call this when we know no other thread ever could have seen the
// data in question, as it can be freed immediately.
static inline void
mmm_retire_unused(void *ptr)
{
    DEBUG_MMM_INTERNAL(ptr, "mmm_retire_unused");
    HATRACK_RETIRE_UNUSED_CTR();

    free(mmm_get_header(ptr));

    return;
}

static inline uint64_t
mmm_get_write_epoch(void *ptr)
{
    return mmm_get_header(ptr)->write_epoch;
}

static inline void
mmm_set_create_epoch(void *ptr, uint64_t epoch)
{
    mmm_get_header(ptr)->create_epoch = epoch;
}

static inline uint64_t
mmm_get_create_epoch(void *ptr)
{
    mmm_header_t *header = mmm_get_header(ptr);

    return header->create_epoch ? header->create_epoch : header->write_epoch;
}

static inline void
mmm_copy_create_epoch(void *dst, void *src)
{
    mmm_set_create_epoch(dst, mmm_get_create_epoch(src));
}

#endif
