#include "testhat.h"

#include <time.h>
#include <stdio.h>
#include <string.h>

static uint32_t
time_test(test_func_t      func,
          uint32_t         iters,
          char            *type,
          uint32_t         num_threads,
          uint32_t         range,
          uint32_t         extra,
          struct timespec *sspec,
          struct timespec *espec)
{
    clock_t     start;
    pthread_t   threads[num_threads];
    test_info_t info[num_threads];
    uint32_t    i;
    testhat_t  *dict;

    atomic_store(&test_func, NULL);
    atomic_store(&mmm_nexttid, 0); // Reset thread ids.

    dict = testhat_new(type);

    for (i = 0; i < num_threads; i++) {
        info[i].tid   = i;
        info[i].range = range;
        info[i].dict  = dict;
        info[i].type  = type;
        info[i].iters = iters / num_threads;
        info[i].extra = extra;

        pthread_create(&threads[i], NULL, start_one_thread, &info[i]);
    }

    start = clock();

    clock_gettime(CLOCK_MONOTONIC, sspec);
    atomic_store(&test_func, func); // Start the threads.

    for (i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
    }

    testhat_delete(dict);

    atomic_store(&test_func, NULL);
    clock_gettime(CLOCK_MONOTONIC, espec);

    return clock() - start;
}

static void
run_one_time_test(test_func_t func,
                  uint32_t    iters,
                  char       *type,
                  uint32_t    range,
                  uint32_t    thread_count,
                  uint32_t    extra)
{
    uint32_t        ticks;
    double          walltime;
    struct timespec sspec;
    struct timespec espec;

    fprintf(stderr, "%10s:\t", type);
    fflush(stderr);

    ticks    = time_test(func,
                      iters,
                      type,
                      thread_count,
                      range,
                      extra,
                      &sspec,
                      &espec);
    walltime = (espec.tv_sec - sspec.tv_sec)
             + ((espec.tv_nsec - sspec.tv_nsec) / 1000000000.0);

    fprintf(stderr,
            "%.4f sec, %d clocks, \t%0.4f c/i\n",
            walltime,
            ticks,
            (double)(((double)ticks) / (double)iters));

    return;
}

static void
run_time_test(char       *name,
              test_func_t func,
              uint32_t    iters,
              char       *types[],
              uint32_t   *ranges,
              uint32_t   *tcounts,
              uint32_t   *extra)
{
    uint32_t dict_ix;
    uint32_t range_ix;
    uint32_t tcount_ix;
    uint32_t extra_ix;

    fprintf(stderr, "[[ Test: %s ]]\n", name);

    extra_ix = 0;

    do {
        tcount_ix = 0;
        while (tcounts[tcount_ix]) {
            range_ix = 0;

            while (ranges[range_ix]) {
                if (extra) {
                    fprintf(stderr,
                            "[%10s] -- Parameters: threads=%4d, "
                            "iters=%7d, range=%6d, other=%4x\n",
                            name,
                            tcounts[tcount_ix],
                            iters,
                            ranges[range_ix],
                            extra[extra_ix]);
                    dict_ix = 0;

                    while (types[dict_ix]) {
			if (tcounts[tcount_ix] != 1 &&
			    !strcmp(types[dict_ix], "refhat")) {
			    dict_ix++;
			    continue;
			}
			run_one_time_test(func,
					  iters,
					  types[dict_ix],
					  ranges[range_ix],
					  tcounts[tcount_ix],
					  extra[extra_ix]);
                        dict_ix++;
                    }
                }
                else {
                    fprintf(stderr,
                            "[%10s] -- Parameters: threads=%4d, "
                            "iters=%7d, range=%6d\n",
                            name,
                            tcounts[tcount_ix],
                            iters,
                            ranges[range_ix]);
                    dict_ix = 0;

                    while (types[dict_ix]) {
			if (tcounts[tcount_ix] != 1 &&
			    !strcmp(types[dict_ix], "refhat")) {
			    dict_ix++;
			    continue;
			}
                        run_one_time_test(func,
                                          iters,
                                          types[dict_ix],
                                          ranges[range_ix],
                                          tcounts[tcount_ix],
                                          0);
                        dict_ix++;
                    }
                }
                range_ix++;
            }
            tcount_ix++;
        }
    } while (extra && extra[++extra_ix]);

    return;
}

// [ rand() ]
//
// Calculate a baseline for calls to rand().
bool
test_rand_speed(test_info_t *info)
{
    uint32_t i;

    for (i = 0; i < info->iters; i++) {
        test_rand();
    }

    return true;
}

bool
test_insert_speed(test_info_t *info)
{
    uint32_t i;

    for (i = 0; i < info->iters; i++) {
        test_put(info->dict, i % info->range, test_rand() % info->range);
    }

    return true;
}

bool
test_write_speed(test_info_t *info)
{
    uint32_t i;
    uint32_t key;

    for (i = 0; i < info->iters; i++) {
        key = test_rand() % info->range;

        if (!(test_rand() % info->extra)) {
            test_remove(info->dict, key);
        }
        else {
            test_put(info->dict, key, key);
        }
    }

    return true;
}

bool
test_rw_speed(test_info_t *info)
{
    uint32_t i;
    uint32_t key;
    uint32_t action;
    uint32_t delete_odds  = info->extra & 0xff;
    uint32_t write_odds   = info->extra >> 4;
    uint32_t nonread_odds = delete_odds + write_odds;

    for (i = 0; i < info->iters; i++) {
        key    = test_rand() % info->range;
        action = test_rand() % 100;

        if (action <= nonread_odds) {
            action = test_rand() % 100;

            if (action <= delete_odds) {
                test_remove(info->dict, key);
            }
            else {
                test_put(info->dict, key, key);
            }
        }
        else {
            key = test_get(info->dict, key);
        }
    }

    return true;
}

bool
test_sort_speed(test_info_t *info)
{
    uint32_t        i;
    uint32_t        key;
    uint32_t        action;
    uint32_t        delete_odds  = info->extra & 0xff;
    uint32_t        write_odds   = info->extra >> 4;
    uint32_t        nonread_odds = delete_odds + write_odds;
    uint64_t        n;
    hatrack_view_t *v;

    for (i = 0; i < info->iters; i++) {
        key    = test_rand() % info->range;
        action = test_rand() % 100;

        if (action <= nonread_odds) {
            action = test_rand() % 100;

            if (action <= delete_odds) {
                test_remove(info->dict, key);
            }
            else {
                test_put(info->dict, key, key);
            }
        }
        else {
            key = test_get(info->dict, key);
        }
    }

    for (i = 0; i < info->iters / 100; i++) {
        v = testhat_view(info->dict, &n, true);
        free(v);
    }

    return true;
}

bool
test_sort_contention(test_info_t *info)
{
    uint32_t        i;
    uint32_t        key;
    uint32_t        action;
    uint32_t        delete_odds  = info->extra & 0xff;
    uint32_t        write_odds   = info->extra >> 4;
    uint32_t        nonread_odds = delete_odds + write_odds;
    uint64_t        n;
    hatrack_view_t *v;

    for (i = 0; i < info->iters; i++) {
        key    = test_rand() % info->range;
        action = test_rand() % 100;

        if (action <= nonread_odds) {
            action = test_rand() % 100;

            if (action <= delete_odds) {
                test_remove(info->dict, key);
            }
            else {
                test_put(info->dict, key, key);
            }
        }
        else {
            key = test_get(info->dict, key);
        }

        if (!(i % 100)) {
            v = testhat_view(info->dict, &n, false);
            free(v);
        }
    }

    return true;
}

void
run_stress_tests(config_info_t *config)
{
    char **hat_list;

    hat_list = config->hat_list;
    
    run_time_test("rand()",
                  test_rand_speed,
                  HATRACK_DEFAULT_ITERS,
		  hat_list,
                  shrug_sizes,
                  basic_threads,
                  0);
    counters_output_delta();        
    run_time_test("insert",
                  test_insert_speed,
                  HATRACK_DEFAULT_ITERS,
		  hat_list,
                  basic_sizes,
                  basic_threads,
                  0);
    counters_output_delta();        
    run_time_test("writes",
                  test_write_speed,
                  HATRACK_DEFAULT_ITERS,
		  hat_list,
                  basic_sizes,
                  basic_threads,
                  del_rate);
    counters_output_delta();        
    run_time_test("rw speed",
                  test_rw_speed,
                  HATRACK_DEFAULT_ITERS,
		  hat_list,
                  basic_sizes,
                  basic_threads,
                  write_rates);
    counters_output_delta();
    run_time_test("sorts",
                  test_sort_speed,
                  HATRACK_DEFAULT_ITERS/10,
		  hat_list,
                  sort_sizes,
                  basic_threads,
                  write_rates);
    counters_output_delta();    
    run_time_test("contend",
                  test_sort_contention,
                  HATRACK_DEFAULT_ITERS/10,
		  hat_list,
                  sort_sizes,
		  mt_only_threads,
                  write_rates);
    counters_output_delta();    
    
}
