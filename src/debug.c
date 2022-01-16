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
 *  Name:           debug.c
 *  Description:    Debugging via in-memory ring buffer, for use when
 *                  HATRACK_DEBUG is on.
 *
 *  Author:         John Viega, john@zork.org
 */

#include <hatrack.h>

#ifdef HATRACK_DEBUG

#include <stdio.h>

hatrack_debug_record_t __hatrack_debug[HATRACK_DEBUG_RING_SIZE] = {};

_Atomic uint64_t __hatrack_debug_sequence         = ATOMIC_VAR_INIT(0);
const char       __hatrack_hex_conversion_table[] = "0123456789abcdef";

/* debug_dump()
 *
 * Prints the most recent records in the ring buffer to stderr, up to
 * the specified amount.
 */
void
debug_dump(uint64_t max_msgs)
{
    int64_t oldest_sequence;
    int64_t cur_sequence;
    int64_t i;

    if (!max_msgs || max_msgs > HATRACK_DEBUG_RING_SIZE) {
        max_msgs = HATRACK_DEBUG_RING_SIZE;
    }

    cur_sequence    = atomic_load(&__hatrack_debug_sequence);
    oldest_sequence = cur_sequence - max_msgs;

    if (oldest_sequence < 0) {
        oldest_sequence = 0;
    }

    oldest_sequence &= HATRACK_DEBUG_RING_LAST_SLOT;
    cur_sequence &= HATRACK_DEBUG_RING_LAST_SLOT;

    if (oldest_sequence >= cur_sequence) {
        for (i = oldest_sequence; i < HATRACK_DEBUG_RING_SIZE; i++) {
            fprintf(stderr,
                    "%06llu: (tid %ld) %s\n",
                    (unsigned long long)__hatrack_debug[i].sequence,
                    (long)__hatrack_debug[i].thread,
                    __hatrack_debug[i].msg);
        }

        i = 0;
    }
    else {
        i = oldest_sequence;
    }

    for (; i < cur_sequence; i++) {
        fprintf(stderr,
                "%06llu: (tid %ld) %s\n",
                (unsigned long long)__hatrack_debug[i].sequence,
                (long)__hatrack_debug[i].thread,
                __hatrack_debug[i].msg);
    }

    return;
}

/* debug_thread()
 *
 * Prints (to stderr) all messages current in the ring buffer that
 * were written by the current thread.
 */
void
debug_thread(void)
{
    debug_other_thread(mmm_mytid);

    return;
}

/* debug_thread()
 *
 * Prints (to stderr) all messages current in the ring buffer that
 * were written by the thread with the given id. Note that we use the
 * thread IDs assigned by mmm for the purposes of thread identification.
 */
void
debug_other_thread(int64_t tid)
{
    int64_t start;
    int64_t i;

    start = atomic_load(&__hatrack_debug_sequence);
    start &= HATRACK_DEBUG_RING_LAST_SLOT;

    for (i = start; i < HATRACK_DEBUG_RING_SIZE; i++) {
        if (tid == __hatrack_debug[i].thread) {
            fprintf(stderr,
                    "%06llu: (tid %ld) %s\n",
                    (unsigned long long)__hatrack_debug[i].sequence,
                    (long)__hatrack_debug[i].thread,
                    __hatrack_debug[i].msg);
        }
    }

    for (i = 0; i < start; i++) {
        if (tid == __hatrack_debug[i].thread) {
            fprintf(stderr,
                    "%06llu: (tid %ld) %s\n",
                    (unsigned long long)__hatrack_debug[i].sequence,
                    (long)__hatrack_debug[i].thread,
                    __hatrack_debug[i].msg);
        }
    }

    return;
}

/* debug_grep()
 *
 * Okay, this doesn't really "grep", but it searches the message field
 * of all ring buffer entries (from oldest to newest), looking for the
 * given substring.
 *
 * The searching is implemented using strstr, so definitely no regexps
 * work.
 */
void
debug_grep(char *s)
{
    int64_t start;
    int64_t i;

    start = atomic_load(&__hatrack_debug_sequence);
    start &= HATRACK_DEBUG_RING_LAST_SLOT;

    for (i = start; i < HATRACK_DEBUG_RING_SIZE; i++) {
        if (strstr(__hatrack_debug[i].msg, s)) {
            fprintf(stderr,
                    "%06llu: (tid %ld) %s\n",
                    (unsigned long long)__hatrack_debug[i].sequence,
                    (long)__hatrack_debug[i].thread,
                    __hatrack_debug[i].msg);
        }
    }

    for (i = 0; i < start; i++) {
        if (strstr(__hatrack_debug[i].msg, s)) {
            fprintf(stderr,
                    "%06llu: (tid %ld) %s\n",
                    (unsigned long long)__hatrack_debug[i].sequence,
                    (long)__hatrack_debug[i].thread,
                    __hatrack_debug[i].msg);
        }
    }

    return;
}

/* debug_pgrep()
 *
 * If you pass in a pointer's value as an integer, constructs the
 * appropriate string for that pointer, and then calls debug_grep()
 * for you.
 */
void
debug_pgrep(uintptr_t n)
{
    char    s[HATRACK_PTR_CHRS + 1] = {};
    char *  p                       = s + HATRACK_PTR_CHRS;
    int64_t i;

    for (i = 0; i < HATRACK_PTR_CHRS; i++) {
        *--p = __hatrack_hex_conversion_table[n & 0xf];
        n >>= 4;
    }

    debug_grep(s);

    return;
}

#endif
