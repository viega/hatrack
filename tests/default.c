/* Copyright © 2021-2022 John Viega
 *
 * See LICENSE.txt for licensing info.
 *
 *  Name:           default.c
 *
 *  Description:    The test executable runs the below tests by default,
 *                  unless you specify other testing. It leverages the
 *                  performance.c to do the actual testing, this just
 *                  picks out the parameters.
 *
 *  Author:         John Viega, john@zork.org
 */

#include "testhat.h"

#define basictest(name, g, p, a, r, d, v, o, sz, pf, range, num, ops)          \
    {                                                                          \
        name, g, p, a, r, d, v, o, sz, pf, range, num, ops, true, 0, NULL      \
    }

#define threadset(name, g, p, a, r, d, v, o, sz, pf, range, ops)               \
    basictest(name, g, p, a, r, d, v, o, sz, pf, range, 1, ops),               \
        basictest(name, g, p, a, r, d, v, o, sz, pf, range, 2, ops),           \
        basictest(name, g, p, a, r, d, v, o, sz, pf, range, 3, ops),           \
        basictest(name, g, p, a, r, d, v, o, sz, pf, range, 4, ops),           \
        basictest(name, g, p, a, r, d, v, o, sz, pf, range, 8, ops),           \
        basictest(name, g, p, a, r, d, v, o, sz, pf, range, 20, ops),          \
        basictest(name, g, p, a, r, d, v, o, sz, pf, range, 100, ops)

#define viewset(name, g, p, a, r, d, v, o, pf, num)                            \
    basictest(name, g, p, a, r, d, v, o, 5, pf, 10, num, 5000000),             \
        basictest(name, g, p, a, r, d, v, o, 8, pf, 100, num, 1500000),        \
        basictest(name, g, p, a, r, d, v, o, 11, pf, 1000, num, 250000),       \
        basictest(name, g, p, a, r, d, v, o, 15, pf, 10000, num, 10000),       \
        basictest(name, g, p, a, r, d, v, o, 18, pf, 100000, num, 1000)

#define sortset(name, g, p, a, r, d, v, o, pf, num)                            \
    basictest(name, g, p, a, r, d, v, o, 5, pf, 10, num, 5000000),             \
        basictest(name, g, p, a, r, d, v, o, 8, pf, 100, num, 1000000),        \
        basictest(name, g, p, a, r, d, v, o, 11, pf, 1000, num, 50000),        \
        basictest(name, g, p, a, r, d, v, o, 15, pf, 10000, num, 3000),        \
        basictest(name, g, p, a, r, d, v, o, 18, pf, 100000, num, 300)

/* Right now, I've picked all op counts so that most tests take around
 * a second (plus or minus a bit) w/ 1 thread, when compiled
 * w/ optimization and w/o debug.
 *
 * Note that, duncecap's locking strategy often makes it a huge, huge
 * outlier, where it can take 10x the time of other algorithms as you
 * add threads.
 */
benchmark_t default_tests[] = {
    threadset("big read", 100, 0, 0, 0, 0, 0, 0, 17, 100, 100000, 10000000),
    threadset("big put", 0, 100, 0, 0, 0, 0, 0, 4, 0, 100000, 10000000),
    threadset("big add", 0, 0, 100, 0, 0, 0, 0, 4, 0, 100000, 10000000),
    threadset("big replace", 0, 0, 0, 100, 0, 0, 0, 17, 75, 100000, 10000000),
    threadset("big remove", 0, 0, 0, 0, 100, 0, 0, 17, 100, 100000, 10000000),
    threadset("small read", 100, 0, 0, 0, 0, 0, 0, 6, 100, 64, 25000000),
    threadset("small put", 0, 100, 0, 0, 0, 0, 0, 6, 0, 64, 15000000),
    threadset("med. read", 100, 0, 0, 0, 0, 0, 0, 12, 100, 2048, 50000000),
    threadset("med. put", 0, 100, 0, 0, 0, 0, 0, 12, 0, 2048, 25000000),
    viewset("view speed", 0, 0, 0, 0, 0, 100, 0, 100, 1),
    sortset("sort speed", 0, 0, 0, 0, 0, 0, 100, 100, 1),
    threadset("grow", 0, 0, 100, 10, 0, 0, 0, 4, 0, 2500000, 2500000),
    threadset("big cache", 98, 0, 1, 0, 1, 0, 0, 23, 75, 8388608, 5000000),
    threadset("data xch", 10, 0, 40, 10, 40, 0, 0, 17, 75, 100000, 15000000),
    threadset("contend", 0, 100, 0, 0, 0, 0, 0, 20, 0, 10, 25000000),
    threadset("|| sort", 60, 20, 0, 5, 5, 0, 10, 17, 50, 100000, 2000),
    {
        0,
    }};

void
run_default_tests(config_info_t *config)
{
    int i = 0;

    while (default_tests[i].total_ops) {
        default_tests[i].hat_list = (char **)config->hat_list;

        run_performance_test(&default_tests[i]);
        counters_output_delta();
        i++;
    }

    return;
}
