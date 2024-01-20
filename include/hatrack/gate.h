/* Copyright © 2021-2022 John Viega
 *
 * See LICENSE.txt for licensing info.
 *
 *  Name:           gate.c
 *
 *  Description:    A spin-lock intented primarily to aid in timing
 *                  multi-threaded operations.
 *
 *                  The basic idea is that we want to open the
 *                  starting gate only when the worker threads we're
 *                  measuring are in position to start (e.g., they've
 *                  done all their pre-test initialization).
 *
 *                  Workers signal with gate_thread_ready(), which
 *                  then spins until the starting gun fires,
 *                  metaphorically speaking.
 *
 *                  Meanwhile, the thread management thread calls
 *                  gate_open(), which spins until the requested
 *                  number threads are ready (which should be all
 *                  threads we want to benchmark).
 *
 *                  When the worker threads are ready, the manager's
 *                  thread records a timestamp of the starting time,
 *                  and then fires the starting gun by writing the
 *                  value GATE_OPEN to the gate, which causes the
 *                  worker threads to go.
 *
 *                  The management thread can then join() on all the
 *                  thread ids in order to sleep until it's time to
 *                  look at the results.
 *
 *  Author:         John Viega, john@zork.org
 */

#ifndef __GATE_H__
#define __GATE_H__

#include <time.h>
#include <hatrack/mmm.h>
#include <strings.h>

typedef struct {
    _Atomic int64_t count;
    uint64_t        max_threads;
    double          elapsed_time;
    double          fastest_time;
    double          avg_time;
    struct timespec start_time;
    struct timespec end_times[];
} gate_t;

#define GATE_OPEN 0xffffffffffffffff

static inline double
gate_time_diff(struct timespec *end, struct timespec *start)
{
    return ((double)(end->tv_sec - start->tv_sec))
         + ((end->tv_nsec - start->tv_nsec) / 1000000000.0);
}

static inline void
gate_init(gate_t *gate, uint64_t max_threads)
{
    bzero(gate->end_times, sizeof(struct timespec) * max_threads);

    gate->max_threads  = max_threads;
    gate->count        = 0;
    gate->elapsed_time = 0;

    return;
}

static inline gate_t *
gate_new_size(uint64_t mt)
{
    gate_t *ret;

    ret = (gate_t *)malloc(sizeof(gate_t) + sizeof(struct timespec) * mt);

    gate_init(ret, mt);

    return ret;
}

static inline gate_t *
gate_new(void)
{
    return gate_new_size(HATRACK_THREADS_MAX);
}

static inline void
gate_delete(gate_t *gate)
{
    free(gate);

    return;
}

static inline void
gate_thread_ready(gate_t *gate)
{
    atomic_fetch_add(&gate->count, 1);

    while (atomic_read(&gate->count) != (int64_t)GATE_OPEN)
	;

    return;
}

static inline void
gate_thread_done(gate_t *gate)
{
    clock_gettime(CLOCK_MONOTONIC, &gate->end_times[mmm_mytid]);

    return;
}

static inline void
gate_open(gate_t *gate, int64_t num_threads)
{
    while (atomic_read(&gate->count) != num_threads)
	;

    atomic_signal_fence(memory_order_seq_cst);
    clock_gettime(CLOCK_MONOTONIC, &gate->start_time);
    atomic_signal_fence(memory_order_seq_cst);
    
    atomic_store(&gate->count, GATE_OPEN);

    return;
}

static inline double
gate_close(gate_t *gate)
{
    uint64_t i, n;
    double   cur, min, max, total;

    min   = 0;
    max   = 0;
    total = 0;
    n     = 0;
    
    for (i = 0; i < gate->max_threads; i++) {
	if (gate->end_times[i].tv_sec || gate->end_times[i].tv_nsec) {
	    cur    = gate_time_diff(&gate->end_times[i], &gate->start_time);
	    total += cur;
	    
	    n++;
	    
	    if (!min || cur < min) {
		min = cur;
	    }
	    if (!max || cur > max) {
		max = cur;
	    }
	}
    }

    gate->elapsed_time = max;
    gate->fastest_time = min;
    gate->avg_time     = total / n;

    return max;
}

static inline double
gate_get_avg(gate_t *gate)
{
    return gate->avg_time;
}

static inline double
gate_get_min(gate_t *gate)
{
    return gate->fastest_time;
}

// Basic gates can be used w/o timing, or can do the start time, and
// then you can handle the rest manually.
typedef _Atomic int64_t basic_gate_t;

static inline void
basic_gate_init(basic_gate_t *gate)
{
    atomic_store(gate, 0);

    return;
}

static inline void
basic_gate_open(basic_gate_t    *gate,
                int64_t          num_threads,
		struct timespec *ts)
{
    while (atomic_read(gate) != num_threads)
	;

    atomic_signal_fence(memory_order_seq_cst);
    if (ts) {
	clock_gettime(CLOCK_MONOTONIC, ts);
    }
    atomic_signal_fence(memory_order_seq_cst);
    atomic_store(gate, -1);

    return;
}

static inline void
basic_gate_thread_ready(basic_gate_t *gate) {
    atomic_fetch_add(gate, 1);

    while (atomic_read(gate) != -1)
	;

    return;
}

#endif






