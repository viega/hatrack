#include <testhat.h>
#include <hatrack.h>
#include <stdio.h>

/* This is not the best test of anything. The workload doesn't feel
 * particularly realistic, and if we're looking for best-case timing
 * of just the operations, there's a bit of extra cruft in here.
 *
 * But this is a reasonable proof-of-concept for now, until I put
 * together a better benchmark rig for it.
 */

#define TOTAL_ENQUEUES 10000000

static double
time_diff(struct timespec *end, struct timespec *start)
{
    return ((double)(end->tv_sec - start->tv_sec))
         + ((end->tv_nsec - start->tv_nsec) / 1000000000.0);
}

static struct timespec stop_times[HATRACK_THREADS_MAX];
static gate_t          starting_gate = ATOMIC_VAR_INIT(0);
static queue_t        *mt_queue;

static void
clear_timestamps(void)
{
    for (int i = 0; i < HATRACK_THREADS_MAX; i++) {
        stop_times[i].tv_sec  = 0;
        stop_times[i].tv_nsec = 0;
    }

    return;
}

void *
multi_threaded_enqueues(void *info)
{
    uint64_t max_counter = (uint64_t)info;
    uint64_t my_counter  = max_counter & 0xffffffff00000000;

    mmm_register_thread();

    starting_gate_thread_ready(&starting_gate);

    while (my_counter < max_counter) {
        my_counter++;
        queue_enqueue(mt_queue, (void *)my_counter);
    }

    clock_gettime(CLOCK_MONOTONIC, &stop_times[mmm_mytid]);
    mmm_clean_up_before_exit();

    return NULL;
}

static __thread uint64_t last_dequeue[HATRACK_THREADS_MAX];

void *
multi_threaded_dequeues(void *info)
{
    uint64_t num_iters = (uint64_t)info;
    uint64_t i;
    uint64_t res;
    uint32_t tid;

    for (i = 0; i < HATRACK_THREADS_MAX; i++) {
        last_dequeue[i] = 0;
    }

    mmm_register_thread();

    starting_gate_thread_ready(&starting_gate);

    for (i = 0; i < num_iters; i++) {
        res = (uint64_t)queue_dequeue(mt_queue, NULL);

        if (!res) {
            continue;
        }

        tid = res >> 32;

        if (last_dequeue[tid] >= res) {
            abort();
        }
        last_dequeue[tid] = res;
    }

    clock_gettime(CLOCK_MONOTONIC, &stop_times[mmm_mytid]);
    mmm_clean_up_before_exit();

    return NULL;
}

void
multi_threaded_v1(int num_threads)
{
    int             i;
    uint64_t        num_iters;
    uint64_t        next_threadid = 1;
    pthread_t       enqueue_threads[num_threads];
    pthread_t       dequeue_threads[num_threads];
    struct timespec start_time;
    double          cur, min, max;

    mt_queue = queue_new_size(25);
    clear_timestamps();
    starting_gate_init(&starting_gate);

    num_iters = TOTAL_ENQUEUES / num_threads;

    for (i = 0; i < num_threads; i++) {
        pthread_create(&enqueue_threads[i],
                       NULL,
                       multi_threaded_enqueues,
                       (void *)((next_threadid << 32) | num_iters));
        pthread_create(&dequeue_threads[i],
                       NULL,
                       multi_threaded_dequeues,
                       (void *)num_iters);
        next_threadid++;
    }

    starting_gate_open_when_ready(&starting_gate, num_threads * 2, &start_time);

    for (i = 0; i < num_threads; i++) {
        pthread_join(enqueue_threads[i], NULL);
        pthread_join(dequeue_threads[i], NULL);
    }

    min = 0;
    max = 0;

    for (i = 0; i < HATRACK_THREADS_MAX; i++) {
        if (stop_times[i].tv_sec || stop_times[i].tv_nsec) {
            cur = time_diff(&stop_times[i], &start_time);

            if (!min || cur < min) {
                min = cur;
            }
            if (!max || cur > max) {
                max = cur;
            }
        }
    }

    fprintf(stderr,
            "mt1(%d threads): %.4f sec; Ops/sec: %llu\n",
            num_threads,
            max,
            (unsigned long long)((2 * (double)TOTAL_ENQUEUES) / max));

    return;
}

void
single_threaded_v1(void)
{
    queue_t        *queue;
    uint64_t        i;
    double          diff;
    struct timespec start_time;
    struct timespec stop_time;

    queue = queue_new();

    clock_gettime(CLOCK_MONOTONIC, &start_time);

    for (i = 0; i < TOTAL_ENQUEUES; i++) {
        queue_enqueue(queue, (void *)(i + 1));
    }

    for (i = 0; i < TOTAL_ENQUEUES; i++) {
        if (((uint64_t)queue_dequeue(queue, NULL)) != (i + 1)) {
            abort();
        }
    }

    clock_gettime(CLOCK_MONOTONIC, &stop_time);

    diff = time_diff(&stop_time, &start_time);

    printf("Test 1: %.4f sec; Ops/sec: %llu\n",
           diff,
           (unsigned long long)(((double)(TOTAL_ENQUEUES * 2)) / diff));

    queue_delete(queue);

    return;
}

void
single_threaded_v2(void)
{
    queue_t        *queue;
    uint64_t        i;
    double          diff;
    struct timespec start_time;
    struct timespec stop_time;

    queue = queue_new();

    clock_gettime(CLOCK_MONOTONIC, &start_time);

    for (i = 0; i < TOTAL_ENQUEUES; i++) {
        queue_enqueue(queue, (void *)(i + 1));
        if (((uint64_t)queue_dequeue(queue, NULL)) != (i + 1)) {
            abort();
        }
    }

    clock_gettime(CLOCK_MONOTONIC, &stop_time);

    diff = time_diff(&stop_time, &start_time);

    printf("Test 2: %.4f sec; Ops/sec: %llu\n",
           diff,
           (unsigned long long)(((double)(TOTAL_ENQUEUES * 2)) / diff));

    queue_delete(queue);

    return;
}

int
main(void)
{
    single_threaded_v1();
    single_threaded_v2();
    multi_threaded_v1(2);
    multi_threaded_v1(3);
    multi_threaded_v1(4);
    multi_threaded_v1(8);
    multi_threaded_v1(20);
    multi_threaded_v1(100);

    return 0;
}
