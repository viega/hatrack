/* Copyright © 2021-2022 John Viega
 *
 * See LICENSE.txt for licensing info.
 *
 *  Name:           performance.c
 *
 *  Description:    Run one performance test. The default tests are
 *                  configured in default.c
 *
 *  Author:         John Viega, john@zork.org
 */

#include <testhat.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/utsname.h>
#include <sys/ioctl.h>
#include <stdio.h>
#include <string.h>

#define WIN_COL_IOCTL TIOCGWINSZ
typedef struct winsize wininfo_t;
#define get_cols(x) x.ws_col

/* For our rng option, if we select rand, we call rand() up to three
 * times per operation:
 *
 * 1) To figure out which operation to perform (scaled to the odds)
 * 2) To select a key to insert, when appropriate (scaled down to max keys)
 * 3) To select a value to insert, when appropriate.
 *
 * This has a bit more overhead than the 'shuffle' option, but is a
 * bit more 'real-world'.  In the shuffle option, we take the odds,
 * create a virtual 'deck of cards' with 100 items in it, with one
 * operation per-card (op_distribution).
 *
 * In both cases, each thread uses a random number to seed the RNG,
 * based on a starting seed given to it by the test manager.  In the
 * shuffle case, however, rand() is only used to shuffle the
 * thread-local copy of op_distribution (called thread_mix), and to
 * pick a value relatively prime to the number of keys in the range,
 * then use that to step through.
 *
 * When we use the shuffle approach, thread_full_cycles caches how
 * many times each thread will completely run through their thread_mix
 * array, and remaining_ops will be total_ops % (thread_full_cycles * 100)
 */
static char    op_distribution[100];
static int64_t thread_full_cycles;
static int     remaining_ops;

// key_mod_mask is one less than the (power-of-two) number of possible
// keys.  We use the mask to help us figure out which key to use.
static uint32_t key_mod_mask;

/* This is essentially a spin lock, where threads register their
 * readiness, and then wait for a signal from the controller thread
 * that all threads are ready, and that they should start!
 */
static gate_t starting_gate = ATOMIC_VAR_INIT(0);

/*
 * The table for the current test.
 */
static testhat_t *table;

/*
 * We try to minimize how much we time that is overhead. So instead of
 * having the controller join() on all threads and THEN look at the
 * clock, we have each thread write out the clock when it's done with
 * its work.
 *
 * That way, we can look at not only the total time taken, but the
 * fastest and average threads.
 */
static struct timespec stop_times[HATRACK_THREADS_MAX];

#define calculate_num_test_keys(n) hatrack_round_up_to_power_of_2(n)

// Internal enumeration used for the op distribution.
enum
{
    OP_READ,
    OP_PUT,
    OP_ADD,
    OP_REPLACE,
    OP_REMOVE,
    OP_VIEW,
    OP_ORDERED_VIEW
};

/* Instead of looking at each set of parameters and calculating the
 * column width (which is a better solution for sure), we just
 * ballpark the widest reasonable columns. If a column gets too
 * wide, we don't truncate, we just let things look bad :)
 */
#define COL_WIDTH    25
#define COL_PAD      1
#define FMT_READS    "Reads:         %u%%"
#define FMT_PUTS     "Puts:          %u%%"
#define FMT_ADDS     "Adds:          %u%%"
#define FMT_REPL     "Replaces:      %u%%"
#define FMT_RMS      "Removes:       %u%%"
#define FMT_FVIEWS   "Fast Views:    %u%%"
#define FMT_OVIEWS   "Ordered Views: %u%%"
#define FMT_START_SZ "Start buckets: 2^%u"
#define FMT_PREFILL  "Prefill:       %u%%"
#define FMT_RANGE    "Max keys:      %u"
#define FMT_OPS      "Total ops:     %llu"
#define FMT_THREADS  "# threads:     %u"
#define FMT_RNG      "RNG?:          %s"
#define FMT_COL_SEP  " %-25s"

#define output_cell(fmt, ...)                                                  \
    snprintf(buf, COL_WIDTH, fmt, __VA_ARGS__);                                \
    fprintf(stderr, FMT_COL_SEP, buf);                                         \
    if (!(++i % num_cols)) {                                                   \
        fputc('\n', stderr);                                                   \
    }

static void
output_test_information(benchmark_t *config)
{
    struct utsname sys_info;
    long           num_cores;
    char           buf[COL_WIDTH + 1];
    wininfo_t      terminfo;
    int            num_cols;
    int            i;

    uname(&sys_info);
    ioctl(0, WIN_COL_IOCTL, &terminfo);

    num_cores = sysconf(_SC_NPROCESSORS_ONLN);
    num_cols  = get_cols(terminfo) / (COL_WIDTH + COL_PAD);

    if (!num_cols) {
        num_cols = 1;
    }

    fprintf(stderr,
            "Test [%s]: (OS: %s %s) (HW: %ld core %s)\n",
            config->name,
            sys_info.sysname,
            sys_info.release,
            num_cores,
            sys_info.machine);

    i = 0;

    output_cell(FMT_READS, config->read_pct);
    output_cell(FMT_PUTS, config->put_pct);
    output_cell(FMT_ADDS, config->add_pct);
    output_cell(FMT_REPL, config->replace_pct);
    output_cell(FMT_RMS, config->remove_pct);
    output_cell(FMT_FVIEWS, config->view_pct);
    output_cell(FMT_OVIEWS, config->sort_pct);
    output_cell(FMT_START_SZ, config->start_sz);
    output_cell(FMT_PREFILL, config->prefill_pct);
    output_cell(FMT_RANGE, config->key_range);
    output_cell(FMT_OPS, (unsigned long long)config->total_ops);
    output_cell(FMT_THREADS, config->num_threads);
    output_cell(FMT_RNG, (config->shuffle ? "shuffle" : "rand"));
    fputc('\n', stderr);

    return;
}

/* Computes how many items to pre-fill the table with, based on
 * the asked-for starting capacity, and the prefill percentage.
 *
 * We don't allow this to go above the key range.  But we will let it
 * go past the starting table size-- anything over 74% will cause the
 * table size to grow, but BEFORE the timing turns on.
 */
static uint64_t
get_prefill_amount(benchmark_t *config)
{
    uint64_t n;

    n = 1 << config->start_sz;
    n = n * config->prefill_pct;
    n = n / 100;

    if (n > config->key_range) {
        return config->key_range;
    }

    return n;
}

/*
 * This approach is intended to avoid measuring too much use of the
 * RNG (even though our RNG is both very fast and threadsafe).  What
 * we do here is create an array of 100 items that tell us which
 * operation to do, shuffle it on a per-thread basis, and then have
 * each thread cycle through that array to determine what operation to
 * do.
 */
static void
prepare_operational_mix(benchmark_t *config)
{
    unsigned int i, j;

    j = 0;

    for (i = 0; i < config->read_pct; i++) {
        op_distribution[j++] = OP_READ;
    }

    for (i = 0; i < config->put_pct; i++) {
        op_distribution[j++] = OP_PUT;
    }

    for (i = 0; i < config->add_pct; i++) {
        op_distribution[j++] = OP_ADD;
    }

    for (i = 0; i < config->replace_pct; i++) {
        op_distribution[j++] = OP_REPLACE;
    }

    for (i = 0; i < config->remove_pct; i++) {
        op_distribution[j++] = OP_REMOVE;
    }

    for (i = 0; i < config->view_pct; i++) {
        op_distribution[j++] = OP_VIEW;
    }

    for (i = 0; i < config->sort_pct; i++) {
        op_distribution[j++] = OP_ORDERED_VIEW;
    }

    return;
}

extern __thread bool rand_inited;

static void *
shuffle_thread_run(void *v)
{
    int64_t  i, j;
    uint32_t next_key;
    uint32_t thread_step;
    uint32_t seed;
    uint64_t num_items;
    char     thread_mix[100];

    seed        = (uint32_t)(uintptr_t)v;
    next_key    = seed;
    thread_step = seed;

    memcpy(thread_mix, op_distribution, 100);
    test_shuffle_array(thread_mix, 100, sizeof(char));
    mmm_register_thread();
    starting_gate_thread_ready(&starting_gate);

    for (i = 0; i < thread_full_cycles; i++) {
        j = 0;

        for (j = 0; j < 100; j++) {
            switch (thread_mix[j]) {
            case OP_READ:
                test_get(table, next_key);
                break;
            case OP_PUT:
                test_put(table, next_key, 0);
                break;
            case OP_ADD:
                test_add(table, next_key, 0);
                break;
            case OP_REPLACE:
                test_replace(table, next_key, 0);
                break;
            case OP_REMOVE:
                test_remove(table, next_key);
                break;
            case OP_VIEW:
                free(test_view(table, &num_items, false));
                break;
            case OP_ORDERED_VIEW:
                free(test_view(table, &num_items, true));
                break;
            }
            next_key = (next_key + thread_step) & key_mod_mask;
        }
    }

    for (j = 0; j < remaining_ops; j++) {
        switch (thread_mix[j]) {
        case OP_READ:
            test_get(table, next_key);
            break;
        case OP_PUT:
            test_put(table, next_key, 0);
            break;
        case OP_ADD:
            test_add(table, next_key, 0);
            break;
        case OP_REPLACE:
            test_replace(table, next_key, 0);
            break;
        case OP_REMOVE:
            test_remove(table, next_key);
            break;
        case OP_VIEW:
            free(test_view(table, &num_items, false));
            break;
        case OP_ORDERED_VIEW:
            free(test_view(table, &num_items, true));
            break;
        }
        next_key = (next_key + thread_step) & key_mod_mask;
    }

    clock_gettime(CLOCK_MONOTONIC, &stop_times[mmm_mytid]);
    mmm_clean_up_before_exit();

    return NULL;
}

static void *
shuffle_thread_run64(void *v)
{
    int64_t  i, j;
    uint32_t next_key;
    uint32_t thread_step;
    uint32_t seed;
    uint64_t num_items;
    char     thread_mix[100];

    seed        = (uint32_t)(uintptr_t)v;
    next_key    = seed;
    thread_step = seed;

    memcpy(thread_mix, op_distribution, 100);
    test_shuffle_array(thread_mix, 100, sizeof(char));
    mmm_register_thread();
    starting_gate_thread_ready(&starting_gate);

    for (i = 0; i < thread_full_cycles; i++) {
        j = 0;

        for (j = 0; j < 100; j++) {
            switch (thread_mix[j]) {
            case OP_READ:
                test_get64(table, next_key);
                break;
            case OP_PUT:
                test_put64(table, next_key, 0xff);
                break;
            case OP_ADD:
                test_add64(table, next_key, 0xff);
                break;
            case OP_REPLACE:
                test_replace64(table, next_key, 0xff);
                break;
            case OP_REMOVE:
                test_remove64(table, next_key);
                break;
            case OP_VIEW:
                free(test_view64(table, &num_items, false));
                break;
            case OP_ORDERED_VIEW:
                free(test_view64(table, &num_items, true));
                break;
            }
            next_key = (next_key + thread_step) & key_mod_mask;
        }
    }

    for (j = 0; j < remaining_ops; j++) {
        switch (thread_mix[j]) {
        case OP_READ:
            test_get64(table, next_key);
            break;
        case OP_PUT:
            test_put64(table, next_key, 0xff);
            break;
        case OP_ADD:
            test_add64(table, next_key, 0xff);
            break;
        case OP_REPLACE:
            test_replace64(table, next_key, 0xff);
            break;
        case OP_REMOVE:
            test_remove64(table, next_key);
            break;
        case OP_VIEW:
            free(test_view64(table, &num_items, false));
            break;
        case OP_ORDERED_VIEW:
            free(test_view64(table, &num_items, true));
            break;
        }
        next_key = (next_key + thread_step) & key_mod_mask;
    }

    clock_gettime(CLOCK_MONOTONIC, &stop_times[mmm_mytid]);
    mmm_clean_up_before_exit();

    return NULL;
}

static void *
rand_thread_run(void *v)
{
    uint32_t n;
    int64_t  i;
    uint64_t num_items;
    intptr_t thread_total_ops;

    thread_total_ops = (uintptr_t)v;
    /* Grab the first number before the starting_gate to ensure that
     * RNG initialization happens outside the operation timing.
     */
    n                = test_rand() % 100;

    mmm_register_thread();
    starting_gate_thread_ready(&starting_gate);

    for (i = 0; i < thread_total_ops; i++) {
        switch (op_distribution[n]) {
        case OP_READ:
            test_get(table, test_rand() & key_mod_mask);
            break;
        case OP_PUT:
            test_put(table, test_rand() & key_mod_mask, test_rand());
            break;
        case OP_ADD:
            test_add(table, test_rand() & key_mod_mask, test_rand());
            break;
        case OP_REPLACE:
            test_replace(table, test_rand() & key_mod_mask, test_rand());
            break;
        case OP_REMOVE:
            test_remove(table, test_rand() & key_mod_mask);
            break;
        case OP_VIEW:
            free(test_view(table, &num_items, false));
            break;
        case OP_ORDERED_VIEW:
            free(test_view(table, &num_items, true));
            break;
        }
        n = test_rand() % 100;
    }

    clock_gettime(CLOCK_MONOTONIC, &stop_times[mmm_mytid]);
    mmm_clean_up_before_exit();

    return NULL;
}

static void *
rand_thread_run64(void *v)
{
    uint32_t n;
    int64_t  i;
    uint64_t num_items;
    intptr_t thread_total_ops;

    thread_total_ops = (uintptr_t)v;
    n                = test_rand() % 100;

    mmm_register_thread();
    starting_gate_thread_ready(&starting_gate);

    for (i = 0; i < thread_total_ops; i++) {
        switch (op_distribution[n]) {
        case OP_READ:
            test_get64(table, (test_rand() & key_mod_mask));
            break;
        case OP_PUT:
            test_put64(table, test_rand() & key_mod_mask, test_rand());
            break;
        case OP_ADD:
            test_add64(table, test_rand() & key_mod_mask, test_rand());
            break;
        case OP_REPLACE:
            test_replace64(table, test_rand() & key_mod_mask, test_rand());
            break;
        case OP_REMOVE:
            test_remove64(table, test_rand() & key_mod_mask);
            break;
        case OP_VIEW:
            free(test_view64(table, &num_items, false));
            break;
        case OP_ORDERED_VIEW:
            free(test_view64(table, &num_items, true));
            break;
        }
        n = test_rand() % 100;
    }

    clock_gettime(CLOCK_MONOTONIC, &stop_times[mmm_mytid]);
    mmm_clean_up_before_exit();

    return NULL;
}

static void
initialize_dictionary(benchmark_t *config, char *hat)
{
    int      i;
    uint32_t step;
    uint32_t n;
    int64_t  p;

    table = testhat_new_size(hat, config->start_sz);
    step  = test_rand() & key_mod_mask;
    n     = step;
    p     = get_prefill_amount(config);

    for (i = 0; i < p; i++) {
        test_add(table, n, i);
        n = (n + step) & key_mod_mask;
    }

    return;
}

static void
initialize_dictionary64(benchmark_t *config, char *hat)
{
    int      i;
    uint32_t step;
    uint32_t n;
    int64_t  p;

    table = testhat_new_size(hat, config->start_sz);
    step  = test_rand() & key_mod_mask;
    n     = step;
    p     = get_prefill_amount(config);

    for (i = 0; i < p; i++) {
        test_add64(table, n, i + 8);
        n = (n + step) & key_mod_mask;
    }

    return;
}

static void
clear_timestamps(void)
{
    for (int i = 0; i < HATRACK_THREADS_MAX; i++) {
        stop_times[i].tv_sec  = 0;
        stop_times[i].tv_nsec = 0;
    }

    return;
}

static double
time_diff(struct timespec *end, struct timespec *start)
{
    return ((double)(end->tv_sec - start->tv_sec))
         + ((end->tv_nsec - start->tv_nsec) / 1000000000.0);
}

static void
performance_report(char *hat, benchmark_t *config, struct timespec *start)
{
    double cur, min, max;

    min   = 0;
    max   = 0;

    for (int i = 0; i < HATRACK_THREADS_MAX; i++) {
        if (stop_times[i].tv_sec || stop_times[i].tv_nsec) {
            cur   = time_diff(&stop_times[i], start);

            if (!min || cur < min) {
                min = cur;
            }
            if (!max || cur > max) {
                max = cur;
            }
        }
    }

    fprintf(stderr,
            "%10s time: %.4f sec (fastest: %.4f, avg: %.4f); Ops/sec: %llu\n",
            hat,
            max,
            min,
            max / config->num_threads,
            (unsigned long long)(((double)config->total_ops) / max));

    return;
}

#define HB_DEFAULT 16

void
run_performance_test(benchmark_t *config)
{
    int             i = 0;
    unsigned int    j;
    uint64_t        ops_per_thread;
    uint32_t        tstep;
    pthread_t       threads[config->num_threads];
    struct timespec sspec;
    alg_info_t     *alg_info;

    key_mod_mask = calculate_num_test_keys(config->key_range) - 1;

    output_test_information(config);
    test_init_rand(config->seed);
    prepare_operational_mix(config);
    precompute_hashes(calculate_num_test_keys(config->key_range));
    atomic_store(&mmm_nexttid, 0); // Reset thread ids.

    ops_per_thread = config->total_ops / config->num_threads;

    if (config->shuffle) {
        thread_full_cycles = ops_per_thread / 100;
        remaining_ops      = ops_per_thread % 100;
    }

    while (config->hat_list[i]) {
        alg_info = algorithm_info(config->hat_list[i]);

        if (config->num_threads > 1 && !alg_info->threadsafe) {
            i++;
            continue;
        }

        if (alg_info->hashbytes == HB_DEFAULT) {
            initialize_dictionary(config, config->hat_list[i]);
        }
        else {
            initialize_dictionary64(config, config->hat_list[i]);
        }
        clear_timestamps();
        starting_gate_init(&starting_gate);

        for (j = 0; j < config->num_threads; j++) {
            if (config->shuffle) {
                tstep = test_rand() & key_mod_mask;
                if (alg_info->hashbytes == HB_DEFAULT) {
                    pthread_create(&threads[j],
                                   NULL,
                                   shuffle_thread_run,
                                   (void *)(uint64_t)tstep);
                }
                else {
                    pthread_create(&threads[j],
                                   NULL,
                                   shuffle_thread_run64,
                                   (void *)(uint64_t)tstep);
                }
            }
            else {
                if (alg_info->hashbytes == HB_DEFAULT) {
                    pthread_create(&threads[j],
                                   NULL,
                                   rand_thread_run,
                                   (void *)ops_per_thread);
                }
                else {
                    pthread_create(&threads[j],
                                   NULL,
                                   rand_thread_run64,
                                   (void *)ops_per_thread);
                }
            }
        }

        starting_gate_open_when_ready(&starting_gate,
                                      config->num_threads,
                                      &sspec);

        for (j = 0; j < config->num_threads; j++) {
            pthread_join(threads[j], NULL);
        }

        performance_report(config->hat_list[i], config, &sspec);
        testhat_delete(table);

        i++;
    }

    fputc('\n', stderr);

    return;
}
