#include "debugbuffer.h"

#ifdef HATRACK_DEBUG

#include <stdio.h>

hatrack_debug_record_t __hatrack_debug[1 << HATRACK_DEBUG_RING_LOG] = {};

_Atomic uint64_t __hatrack_debug_sequence         = ATOMIC_VAR_INIT(0);
const char       __hatrack_hex_conversion_table[] = "0123456789abcdef";

void
debug_dump(uint64_t max_msgs)
{
    int64_t oldest_sequence;
    int64_t cur_sequence;
    int64_t i;

    if (!max_msgs || max_msgs > (1 << HATRACK_DEBUG_RING_LOG)) {
        max_msgs = 1 << HATRACK_DEBUG_RING_LOG;
    }
    cur_sequence    = atomic_load(&__hatrack_debug_sequence);
    oldest_sequence = cur_sequence - max_msgs;

    if (oldest_sequence < 0) {
        oldest_sequence = 0;
    }
    oldest_sequence &= ((1 << HATRACK_DEBUG_RING_LOG) - 1);
    cur_sequence &= ((1 << HATRACK_DEBUG_RING_LOG) - 1);

    if (oldest_sequence >= cur_sequence) {
        for (i = oldest_sequence; i < (1 << HATRACK_DEBUG_RING_LOG); i++) {
            printf("%06llu: (tid %lu) %s\n",
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
        printf("%06llu: (tid %lu) %s\n",
               __hatrack_debug[i].sequence,
               (long)__hatrack_debug[i].thread,
               __hatrack_debug[i].msg);
    }
}

#if 0
void
debug_dump_thread(uint64_t max_msgs)
{
    int64_t   oldest_sequence;
    int64_t   cur_sequence;
    int64_t   i;
    int64_t   self = mmm_mytid;

    if (!max_msgs || max_msgs > (1 << HATRACK_DEBUG_RING_LOG)) {
	max_msgs = 1 << HATRACK_DEBUG_RING_LOG;
    }
    cur_sequence = atomic_load(&__hatrack_debug_sequence);
    oldest_sequence = cur_sequence - max_msgs;

    if (oldest_sequence < 0) {
	oldest_sequence = 0;
    }
    oldest_sequence &= ((1 << HATRACK_DEBUG_RING_LOG) - 1);
    cur_sequence    &= ((1 << HATRACK_DEBUG_RING_LOG) - 1);

    if (oldest_sequence >= cur_sequence) {
	for (i = oldest_sequence; i < (1 << HATRACK_DEBUG_RING_LOG); i++) {
	    if (self != __hatrack_debug[i].thread) {
		continue;
	    }
	    printf("%06llu: (tid %lu) %s\n", __hatrack_debug[i].sequence,
		   (long) __hatrack_debug[i].thread,
		   __hatrack_debug[i].msg);
	}
	i = 0;
    }
    else {
	i = oldest_sequence;
    }
    for (; i < cur_sequence; i++) {
	if (self != __hatrack_debug[i].thread) {
	    continue;
	}
	printf("%06llu: (tid %lu) %s\n", __hatrack_debug[i].sequence,
	       (long) __hatrack_debug[i].thread,
	       __hatrack_debug[i].msg);
    }
}
#endif

void
debug_dump_thread()
{
    int64_t start = atomic_load(&__hatrack_debug_sequence);
    int64_t i;
    int64_t self = mmm_mytid;

    start &= ((1 << HATRACK_DEBUG_RING_LOG) - 1);

    for (i = start; i < ((1 << HATRACK_DEBUG_RING_LOG) - 1); i++) {
        if (self == __hatrack_debug[i].thread) {
            printf("%06llu: (tid %lu) %s\n",
                   __hatrack_debug[i].sequence,
                   (long)__hatrack_debug[i].thread,
                   __hatrack_debug[i].msg);
        }
    }
    for (i = 0; i < start; i++) {
        if (self == __hatrack_debug[i].thread) {
            printf("%06llu: (tid %lu) %s\n",
                   __hatrack_debug[i].sequence,
                   (long)__hatrack_debug[i].thread,
                   __hatrack_debug[i].msg);
        }
    }
}

void
debug_grep(char *s)
{
    int64_t start = atomic_load(&__hatrack_debug_sequence);
    int64_t i;
    int64_t len = strlen(s);

    start &= ((1 << HATRACK_DEBUG_RING_LOG) - 1);

    for (i = start; i < ((1 << HATRACK_DEBUG_RING_LOG) - 1); i++) {
        if (!strncmp(__hatrack_debug[i].msg, s, len)) {
            printf("%06llu: (tid %lu) %s\n",
                   __hatrack_debug[i].sequence,
                   (long)__hatrack_debug[i].thread,
                   __hatrack_debug[i].msg);
        }
    }
    for (i = 0; i < start; i++) {
        if (!strncmp(__hatrack_debug[i].msg, s, len)) {
            printf("%06llu: (tid %lu) %s\n",
                   __hatrack_debug[i].sequence,
                   (long)__hatrack_debug[i].thread,
                   __hatrack_debug[i].msg);
        }
    }
}

void
debug_pgrep(uintptr_t n)
{
    char s[HATRACK_PTR_CHRS + 5] = {
        '0',
        'x',
    };
    char   *p = s + HATRACK_PTR_CHRS + 4;
    int64_t i;
    int64_t start = atomic_load(&__hatrack_debug_sequence);
    int64_t len   = HATRACK_PTR_CHRS + 4;

    *--p = ' ';
    *--p = ':';
    for (i = 0; i < HATRACK_PTR_CHRS; i++) {
        *--p = __hatrack_hex_conversion_table[n & 0xf];
        n >>= 4;
    }

    for (i = start; i < ((1 << HATRACK_DEBUG_RING_LOG) - 1); i++) {
        if (!strncmp(__hatrack_debug[i].msg, s, len)) {
            printf("%06llu: (tid %lu) %s\n",
                   __hatrack_debug[i].sequence,
                   (long)__hatrack_debug[i].thread,
                   __hatrack_debug[i].msg);
        }
    }
    for (i = 0; i < start; i++) {
        if (!strncmp(__hatrack_debug[i].msg, s, len)) {
            printf("%06llu: (tid %lu) %s\n",
                   __hatrack_debug[i].sequence,
                   (long)__hatrack_debug[i].thread,
                   __hatrack_debug[i].msg);
        }
    }

    printf("Done.\n");
    return;
}

#endif
