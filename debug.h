#ifndef __HATRACK_DEBUG_H__
#define __HATRACK_DEBUG_H__

#ifdef HATRACK_DEBUG

#include <stdatomic.h>
#include <string.h>
#include <stdio.h>
#include <pthread.h>
#include <stdint.h>

#ifndef HATRACK_DEBUG_MSG_SIZE
#define HATRACK_DEBUG_MSG_SIZE 128
#endif

#if !defined(HATRACK_DEBUG_RING_LOG) || HATRACK_DEBUG_RING_LOG < 15
#undef HATRACK_DEBUG_RING_LONG
#define HATRACK_DEBUG_RING_LOG 15
#endif

#ifndef HATRACK_PTR_CHRS
#define HATRACK_PTR_CHRS 16
#endif

typedef struct {
    _Alignas(32) uint64_t sequence;
    char    msg[HATRACK_DEBUG_MSG_SIZE];
    int64_t thread;
    char    null;
} hatrack_debug_record_t;

extern hatrack_debug_record_t __hatrack_debug[];
extern _Atomic uint64_t       __hatrack_debug_sequence;
extern const char             __hatrack_hex_conversion_table[];
extern __thread int64_t       mmm_mytid;

static inline void
hatrack_debug(char *msg)
{
    uint64_t                mysequence;
    hatrack_debug_record_t *record_ptr;

    mysequence = atomic_fetch_add(&__hatrack_debug_sequence, 1);
    record_ptr
        = &__hatrack_debug[mysequence & ((1 << HATRACK_DEBUG_RING_LOG) - 1)];
    record_ptr->sequence = mysequence;
    record_ptr->thread   = mmm_mytid;
    strncpy(record_ptr->msg, msg, HATRACK_DEBUG_MSG_SIZE);
}

static inline void
hatrack_debug_ptr(void *addr, char *msg)
{
    char buf[HATRACK_PTR_CHRS + 5] = {
        '0',
        'x',
    };
    char                   *p = buf + HATRACK_PTR_CHRS + 4;
    uint64_t                i;
    uintptr_t               n = (uintptr_t)addr;
    uint64_t                mysequence;
    hatrack_debug_record_t *record_ptr;

    mysequence = atomic_fetch_add(&__hatrack_debug_sequence, 1);
    record_ptr
        = &__hatrack_debug[mysequence & ((1 << HATRACK_DEBUG_RING_LOG) - 1)];
    record_ptr->sequence = mysequence;
    record_ptr->thread   = mmm_mytid;

    *--p = ' ';
    *--p = ':';
    for (i = 0; i < HATRACK_PTR_CHRS; i++) {
        *--p = __hatrack_hex_conversion_table[n & 0xf];
        n >>= 4;
    }
    strcpy(record_ptr->msg, buf);
    strncpy(record_ptr->msg + HATRACK_PTR_CHRS + 4,
            msg,
            HATRACK_DEBUG_MSG_SIZE - HATRACK_PTR_CHRS - 4);
}

#define DEBUG(x)        hatrack_debug(x)
#define DEBUG_PTR(x, y) hatrack_debug_ptr((void *)(x), y)

#else

#define DEBUG(x)
#define DEBUG_PTR(x)

#endif

#endif
