#include "debug.h"

#ifdef HATRACK_DEBUG

#include <stdio.h>

hatrack_debug_record_t __hatrack_debug[HATRACK_DEBUG_RING_SIZE] = {};

_Atomic uint64_t __hatrack_debug_sequence         = ATOMIC_VAR_INIT(0);
const char       __hatrack_hex_conversion_table[] = "0123456789abcdef";

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
            printf("%06llu: (tid %ld) %s\n",
                   __hatrack_debug[i].sequence,
                   (long)__hatrack_debug[i].thread,
                   __hatrack_debug[i].msg);
        }
        i = 0;
    }
    else {
        i = oldest_sequence;
    }
    for (; i < cur_sequence; i++) {
        printf("%06llu: (tid %ld) %s\n",
               __hatrack_debug[i].sequence,
               (long)__hatrack_debug[i].thread,
               __hatrack_debug[i].msg);
    }
}

void
debug_thread()
{
    debug_other_thread(mmm_mytid);
}

void
debug_other_thread(int64_t tid)
{
    int64_t start;
    int64_t i;

    start = atomic_load(&__hatrack_debug_sequence);
    start &= HATRACK_DEBUG_RING_LAST_SLOT;

    for (i = start; i < HATRACK_DEBUG_RING_SIZE; i++) {
        if (tid == __hatrack_debug[i].thread) {
            printf("%06llu: (tid %ld) %s\n",
                   __hatrack_debug[i].sequence,
                   (long)__hatrack_debug[i].thread,
                   __hatrack_debug[i].msg);
        }
    }
    for (i = 0; i < start; i++) {
        if (tid == __hatrack_debug[i].thread) {
            printf("%06llu: (tid %ld) %s\n",
                   __hatrack_debug[i].sequence,
                   (long)__hatrack_debug[i].thread,
                   __hatrack_debug[i].msg);
        }
    }
}

void
debug_grep(char *s)
{
    int64_t start;
    int64_t i;

    start = atomic_load(&__hatrack_debug_sequence);
    start &= ((1 << HATRACK_DEBUG_RING_LOG) - 1);

    for (i = start; i < ((1 << HATRACK_DEBUG_RING_LOG) - 1); i++) {
        if (strstr(__hatrack_debug[i].msg, s)) {
            printf("%06llu: (tid %ld) %s\n",
                   __hatrack_debug[i].sequence,
                   (long)__hatrack_debug[i].thread,
                   __hatrack_debug[i].msg);
        }
    }
    for (i = 0; i < start; i++) {
        if (strstr(__hatrack_debug[i].msg, s)) {
            printf("%06llu: (tid %ld) %s\n",
                   __hatrack_debug[i].sequence,
                   (long)__hatrack_debug[i].thread,
                   __hatrack_debug[i].msg);
        }
    }
}

void
debug_pgrep(uintptr_t n)
{
    char    s[HATRACK_PTR_CHRS + 1] = {};
    char   *p                       = s + HATRACK_PTR_CHRS;
    int64_t i;

    for (i = 0; i < HATRACK_PTR_CHRS; i++) {
        *--p = __hatrack_hex_conversion_table[n & 0xf];
        n >>= 4;
    }

    debug_grep(s);
    return;
}

#endif
