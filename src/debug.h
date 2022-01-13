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
 *  Name:           debug.h
 *  Description:    Provides support that I've found useful for dealing with
 *                  multi-threaded operations.  Right now, this consists
 *                  of some custom assertion macros, and an in-memory ring
 *                  buffer.
 *
 *                  None of this code is used unless HATRACK_DEBUG is on.
 *
 *                  This code is less concise and elegent than it
 *                  could be: for instance, if we were to use sprintf()
 *                  instead of manually formatting strings.
 *
 *                  However, given the frequency at which this code
 *                  can get called, using sprintf() has a HUGE
 *                  negative impact on performance.
 *
 *                  So yes, I've forgone clarity for performance, all
 *                  to try to make it easier to debug busy
 *                  multi-threaded apps, without having too
 *                  detrimental an impact to performance.
 *
 *  Author:         John Viega, john@zork.org
 */

#ifndef __HATRACK_DEBUG_H__
#define __HATRACK_DEBUG_H__

#include "config.h"

#ifdef HATRACK_DEBUG

#include <stdatomic.h>
#include <string.h>
#include <stdio.h>
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdalign.h>

/* hatrack_debug_record_t
 *
 * The type for records in our ring buffer.
 *
 * Note that the field named 'null' is intended to always be zero.
 * Records are strncpy()'d into the msg array, but just in case they
 * go right up to the end of the array, make sure we get a zero,
 * whatever the semantics of the strncpy() implementation.
 */
// clang-format off
typedef struct {
    alignas(8)
    char      msg[HATRACK_DEBUG_MSG_SIZE];
    char      null;
    alignas(8)
    uint64_t  sequence;
    int64_t   thread;    
} hatrack_debug_record_t;


/*
 * __hatrack_debug_sequence is a monotoincally increasing counter for
 * debug messages. Threads that use our debugging API will get a
 * unique number per-debug message, by using atomic_fetch_add() on the
 * variable __hatrack_debug_sequence. 
 *
 * The value returned from that atomic_fetch_add() will map to an
 * entry in the ring buffer to which they may write, by modding
 * against the number of entries in the table.
 *
 * As long as our ring buffer is big enough, this will ensure that
 * threads don't stomp on each other's insertions while a write is in
 * progress.
 */
extern hatrack_debug_record_t __hatrack_debug[];
extern _Atomic uint64_t       __hatrack_debug_sequence;
extern const char             __hatrack_hex_conversion_table[];
extern __thread int64_t       mmm_mytid;

/* The below functions are all defined in debug.c. Again, they're only
 * compiled in if HATRACK_DEBUG is on. They are meant for providing
 * access to the ring buffer, generally from within a debugger. 
 *
 * For instance, I have aliases to call each of these functions in my
 * debugger init file.
 *
 * debug_dump() will also get called when an assertion (via our
 * macros) fails.
 */
void debug_dump         (uint64_t);
void debug_thread       (void);
void debug_other_thread (int64_t);
void debug_grep         (char *);
void debug_pgrep        (uintptr_t);

// clang-format on

/* hatrack_debug()
 *
 * Writes a message into the ring buffer.
 */
static inline void
hatrack_debug(char *msg)
{
    uint64_t                mysequence;
    uint64_t                index;
    hatrack_debug_record_t *record_ptr;

    mysequence           = atomic_fetch_add(&__hatrack_debug_sequence, 1);
    index                = mysequence & HATRACK_DEBUG_RING_LAST_SLOT;
    record_ptr           = &__hatrack_debug[index];
    record_ptr->sequence = mysequence;
    record_ptr->thread   = mmm_mytid;

    strncpy(record_ptr->msg, msg, HATRACK_DEBUG_MSG_SIZE);

    return;
}

/* This is one of those things not we don't intend people to
 * configure; it's just there to avoid arcane numbers in the source
 * code.
 *
 * But the name really isn't much more descriptive, I admit!
 *
 * This represents the number of characters we need to allocate
 * statically on the stack for 'fixed' characters when constructing
 * the message to put in the record, in the function
 * hatrack_debug_ptr.  The four characters are the "0x" before the
 * pointer, and the ": " after.
 */
#undef HATRACK_PTR_FMT_CHRS
#define HATRACK_PTR_FMT_CHRS 4

/* hatrack_debug_ptr()
 *
 * Writes a message to the ring buffer, that basically starts with the
 * hex representation of a pointer, followed by the message passed in.
 *
 * Though, to be honest, I currently use this for ALL numbers I need
 * to write into the ring buffer, which is why you don't see a
 * hatrack_debug_int() yet.
 */
static inline void
hatrack_debug_ptr(void *addr, char *msg)
{
    char buf[HATRACK_PTR_CHRS + HATRACK_PTR_FMT_CHRS + 1] = {
        '0',
        'x',
    };
    char                   *p = buf + HATRACK_PTR_CHRS + HATRACK_PTR_FMT_CHRS;
    uint64_t                i;
    uintptr_t               n = (uintptr_t)addr;
    uint64_t                mysequence;
    hatrack_debug_record_t *record_ptr;

    mysequence = atomic_fetch_add(&__hatrack_debug_sequence, 1);
    record_ptr = &__hatrack_debug[mysequence & HATRACK_DEBUG_RING_LAST_SLOT];

    record_ptr->sequence = mysequence;
    record_ptr->thread   = mmm_mytid;

    *--p = ' ';
    *--p = ':';

    for (i = 0; i < HATRACK_PTR_CHRS; i++) {
        *--p = __hatrack_hex_conversion_table[n & 0xf];
        n >>= 4;
    }
    strcpy(record_ptr->msg, buf);
    strncpy(record_ptr->msg + HATRACK_PTR_CHRS + HATRACK_PTR_FMT_CHRS,
            msg,
            HATRACK_DEBUG_MSG_SIZE - HATRACK_PTR_CHRS - HATRACK_PTR_FMT_CHRS);

    return;
}

/* hatrack_debug_assert()
 *
 * This is meant to be called either through the ASSERT() macro, which
 * fills in the values of function, file and line with their
 * appropriate values.
 *
 * When an assertion made to this function fails, it prints history
 * from the ring buffer to stderr, and then goes into a busy-loop so
 * that you may attach a debugger. I even occasionally jump past the
 * loop to let the program keep running...
 */
static inline void
hatrack_debug_assert(bool        expression_result,
                     char       *assertion,
                     const char *function,
                     const char *file,
                     int         line)
{
    if (!expression_result) {
        fprintf(stderr,
                "%s:%d: Assertion \"%s\" failed (in function %s)\n",
                file,
                line,
                assertion,
                function);

        debug_dump(HATRACK_ASSERT_FAIL_RECORD_LEN);

        /* Loop instead of crashing, so that we can attach a
         * debugger.  The asm call forces clang not to optimize
         * this loop away, which it tends to do, even when the C
         * standard tells it not to do so.
         */
        while (true) {
            __asm("");
        }

        fprintf(stderr, "Use the debugger to jump here to keep going.\n");
    }

    return;
}

/* hatrack_debug_assert_w_params()
 *
 * Like hatrack_debug_assert() above, except lets you specify a
 * specific number of records to go back, when we dump them, and also
 * allows you to skip the busy wait loop, if you want to do something
 * else (like abort or keep going).
 *
 * This one is meant to be used via the XASSERT() macro.
 */
static inline void
hatrack_debug_assert_w_params(bool        expression_result,
                              char       *assertion,
                              const char *function,
                              const char *file,
                              int         line,
                              uint32_t    num_records,
                              bool        busy_wait)
{
    if (!expression_result) {
        fprintf(stderr,
                "%s:%d: Assertion \"%s\" failed (in function %s)\n",
                file,
                line,
                assertion,
                function);

        debug_dump(num_records);

        if (busy_wait) {
            /* Loop instead of crashing, so that we can attach a
             * debugger.  The asm call forces clang not to optimize
             * this loop away, which it tends to do, even when the C
             * standard tells it not to do so.
             */
            while (true) {
                __asm("");
            }
        }

        fprintf(stderr, "Use the debugger to jump here to keep going.\n");
    }

    return;
}

#define DEBUG(x)        hatrack_debug(x)
#define DEBUG_PTR(x, y) hatrack_debug_ptr((void *)(x), y)
#define ASSERT(x)       hatrack_debug_assert(x, #x, __FUNCTION__, __FILE__, __LINE__)
#define XASSERT(x, n, b)                                                       \
    hatrack_debug_assert_w_params(x, #x, __FUNCTION__, __FILE__, __LINE__, n, b)

#else

#define DEBUG(x)
#define DEBUG_PTR(x, y)
#define ASSERT(x)
#define XASSERT(x, n, b)
#endif

#endif
