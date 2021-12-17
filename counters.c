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
    "mmm linearize retries",
    "mmm write commits",
    "mmm commit helps",
    "lh0 bucket acquires",
    "lh0 record installs",
    "lh0 record delete",
    "lh0 store creates",
    "lh0 F_MOVING set",
    "lh0 F_MOVED (empty)",
    "lh0 F_MOVED (deleted)",
    "lh0 migrate hash",
    "lh0 migrate record",
    "lh0 F_MOVED (migrate)",
    "lh0 len installed",
    "lh0 store installs",
    "lh1 bucket acquires",
    "lh1 ptr installs",
    "lh1 hist hash installs",
    "lh1 record installs",
    "lh1 record delete",
    "lh1 store creates",
    "lh1 F_MOVING set",
    "lh1 F_MOVED (empty)",
    "lh1 F_MOVED (deleted)",
    "lh1 migrate hash",
    "lh1 migrate record",
    "lh1 move other hash",
    "lh1 install new ptr",
    "lh1 F_MOVED (migrate)",
    "lh1 hist ptr installed",
    "lh1 store installs",
    "lh2 bucket acquires",
    "lh2 ptr installs",
    "lh2 hist hash installs",
    "lh2 forward installed",
    "lh2 record installs",
    "lh2 record delete",
    "lh2 store creates",
    "lh2 F_MOVING set",
    "lh2 F_MOVED (empty)",
    "lh2 F_MOVED (deleted)",
    "lh2 migrate hash",
    "lh2 migrate record",
    "lh2 move other hash",
    "lh2 install new ptr",
    "lh2 F_MOVED (migrate)",
    "lh2 hist ptr installed",
    "lh2 store installs"
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
