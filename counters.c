#include "counters.h"

#ifdef LOWHAT_COUNTERS

#include <stdio.h>

_Atomic uint64_t lowhat_counters[LOWHAT_COUNTERS_NUM]      = {};
uint64_t         lowhat_last_counters[LOWHAT_COUNTERS_NUM] = {};

_Atomic uint64_t lowhat_yn_counters[LOWHAT_YN_COUNTERS_NUM][2]      = {};
uint64_t         lowhat_last_yn_counters[LOWHAT_YN_COUNTERS_NUM][2] = {};

// clang-format off
char *lowhat_counter_names[LOWHAT_COUNTERS_NUM] = {
    "mmm alloc calls",
    "mmm used retires",
    "mmm unused retires"
};

char *lowhat_yn_counter_names[LOWHAT_YN_COUNTERS_NUM] = {
    "mmm linearize retries",    // 0
    "mmm write commits",        // 1
    "mmm commit helps",         // 2
    "lh0 bucket acquires",      // 3
    "lh0 record installs",      // 4
    "lh0 record delete",        // 5
    "lh0 store creates",        // 6
    "lh0 F_MOVING set",         // 7
    "lh0 F_MOVED (empty)",      // 8
    "lh0 F_MOVED (deleted)",    // 9
    "lh0 migrate hash",         // 10
    "lh0 migrate record",       // 11
    "lh0 F_MOVED (migrate)",    // 12
    "lh0 len installed",        // 13
    "lh0 store installs",       // 14
    "lh1 bucket acquires",      // 15
    "lh1 ptr installs",         // 16
    "lh1 hist hash installs",   // 17
    "lh1 record installs",      // 18
    "lh1 record delete",        // 19
    "lh1 store creates",        // 20
    "lh1 F_MOVING set",         // 21
    "lh1 F_MOVED (empty)",      // 22
    "lh1 F_MOVED (deleted)",    // 23
    "lh1 migrate hash",         // 24
    "lh1 migrate record",       // 25
    "lh1 move other hash",      // 26
    "lh1 install new ptr",      // 27
    "lh1 F_MOVED (migrate)",    // 28
    "lh1 hist ptr installed",   // 29
    "lh1 store installs",       // 30
    "lh2 bucket acquires",      // 31
    "lh2 ptr installs",         // 32
    "lh2 hist hash installs",   // 33
    "lh2 forward installed",    // 34
    "lh2 record installs",      // 35
    "lh2 record delete",        // 36
    "lh2 store creates",        // 37
    "lh2 F_MOVING set",         // 38
    "lh2 F_MOVED (empty)",      // 39
    "lh2 F_MOVED (deleted)",    // 40
    "lh2 migrate hash",         // 41
    "lh2 migrate record",       // 42
    "lh2 move other hash",      // 43
    "lh2 install new ptr",      // 44
    "lh2 F_MOVED (migrate)",    // 45
    "lh2 hist ptr installed",   // 46
    "lh2 store installs",       // 47
    "hi0 bucket acquires",      // 48
    "hi0 record installs",      // 49
    "hi0 record delete",        // 50
    "hi0 store creates",        // 51
    "hi0 F_MOVING set",         // 52
    "hi0 F_MOVED (empty)",      // 53
    "hi0 migrate hash",         // 54
    "hi0 migrate record",       // 55
    "hi0 F_MOVED (migrate)",    // 56
    "hi0 len installed",        // 57
    "hi0 store installs"        // 58
};
// clang-format on

void
counters_output_delta(void)
{
    uint64_t i;
    uint64_t total;
    uint64_t y_cur, n_cur, y_last, n_last;
    uint64_t ydelta, ndelta;
    double   percent;

    fprintf(stderr, "----------- Counter Deltas --------------\n");
    for (i = 0; i < LOWHAT_COUNTERS_NUM; i++) {
        if (!lowhat_counters[i]) {
            continue;
        }
        fprintf(stderr,
                "%s:\t %llu\n",
                lowhat_counter_names[i],
                lowhat_counters[i] - lowhat_last_counters[i]);

        lowhat_last_counters[i] = lowhat_counters[i];
    }

    for (i = 0; i < LOWHAT_YN_COUNTERS_NUM; i++) {
        y_cur   = lowhat_yn_counters[i][0];
        n_cur   = lowhat_yn_counters[i][1];
        y_last  = lowhat_last_yn_counters[i][0];
        n_last  = lowhat_last_yn_counters[i][1];
        ydelta  = y_cur - y_last;
        ndelta  = n_cur - n_last;
        total   = ydelta + ndelta;
        percent = (((double)ydelta) / (double)total) * 100.0;

        lowhat_last_yn_counters[i][0] = y_cur;
        lowhat_last_yn_counters[i][1] = n_cur;

        if (!total) {
            continue;
        }

        fprintf(stderr,
                "%s:\t %llu y, %llu n of %llu (%.2f%% y)\n",
                lowhat_yn_counter_names[i],
                ydelta,
                ndelta,
                total,
                percent);
    }
}

void
counters_output_alltime(void)
{
    uint64_t i;
    uint64_t total;

    fprintf(stderr, "----------- Counter TOTALS --------------\n");

    for (i = 0; i < LOWHAT_COUNTERS_NUM; i++) {
        if (!lowhat_counters[i]) {
            continue;
        }
        fprintf(stderr,
                "%s:\t %llu\n",
                lowhat_counter_names[i],
                lowhat_counters[i]);
    }

    for (i = 0; i < LOWHAT_YN_COUNTERS_NUM; i++) {
        total = lowhat_yn_counters[i][0] + lowhat_yn_counters[i][1];

        if (!total) {
            continue;
        }

        fprintf(stderr,
                "%s:\t %llu y, %llu n of %llu (%.2f%% y)\n",
                lowhat_yn_counter_names[i],
                lowhat_yn_counters[i][0],
                lowhat_yn_counters[i][1],
                total,
                (double)100.0
                    * (((double)lowhat_yn_counters[i][0]) / (double)total));
    }
}

#endif
