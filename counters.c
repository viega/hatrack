/*
 * Copyright © 2021 John Viega
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *  Name:           counters.c
 *  Description:    In-memory counters for performance monitoring,
 *                  when HATRACK_DEBUG is on.
 *
 *  Author:         John Viega, john@zork.org
 */

#include "counters.h"

#ifdef HATRACK_COUNTERS

#include <stdio.h>

// clang-format off
_Atomic uint64_t hatrack_counters[HATRACK_COUNTERS_NUM]            = {};
uint64_t hatrack_last_counters[HATRACK_COUNTERS_NUM]               = {};

_Atomic uint64_t hatrack_yn_counters[HATRACK_YN_COUNTERS_NUM][2]   = {};
uint64_t hatrack_last_yn_counters[HATRACK_YN_COUNTERS_NUM][2]      = {};

char *hatrack_counter_names[HATRACK_COUNTERS_NUM] = {
    "mmm alloc calls",
    "mmm used retires",
    "mmm unused retires",
    "stores shrunk",
    "hi1a msleep 1a success",
    "hi1a msleep 1a fail",
    "hi1a msleep 1b success",
    "hi1a msleep 1b fail",
    "hi1a msleep 2a success",
    "hi1a msleep 2a fail",
    "hi1a msleep 2b success",
    "hi1a msleep 2b fail",
    "wh help requests"
};

char *hatrack_yn_counter_names[HATRACK_YN_COUNTERS_NUM] = {
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
    "hi1 bucket acquires",      // 48
    "hi1 record installs",      // 49
    "hi1 record delete",        // 50
    "hi1 store creates",        // 51
    "hi1 F_MOVING set",         // 52
    "hi1 F_MOVED (empty)",      // 53
    "hi1 migrate hash",         // 54
    "hi1 migrate record",       // 55
    "hi1 F_MOVED (migrate)",    // 56
    "hi1 len installed",        // 57
    "hi1 store installs",       // 58
    "hi1 woke up to no job",    // 59
    "hi64 bucket acquires",     // 60
    "hi64 bucket acquires 2",   // 61
    "hi64 record installs",     // 62
    "hi64 record deletes",      // 63
    "hi64 store creates",       // 64
    "hi64 store installs",      // 65    
    "hi64 F_MOVING set",        // 66
    "hi64 F_MOVED (empty)",     // 67
    "hi64 migrate hash",        // 68
    "hi64 migrate hash 2",      // 69
    "hi64 migrate record",      // 70
    "hi64 F_MOVE (migrate)",    // 71
    "hi64 len installed",       // 72
    "wh bucket acquires",       // 73
    "wh record installs",       // 74
    "wh record delete",         // 75
    "wh store creates",         // 76
    "wh F_MOVING set",          // 77
    "wh F_MOVED (empty)",       // 78
    "wh migrate hash",          // 79
    "wh migrate record",        // 80
    "wh F_MOVED (migrate)",     // 81
    "wh len installed",         // 82
    "wh store installs",        // 83
    
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
    for (i = 0; i < HATRACK_COUNTERS_NUM; i++) {
        if (hatrack_counters[i] == hatrack_last_counters[i]) {
            continue;
        }
        fprintf(stderr,
                "%s:\t %llu\n",
                hatrack_counter_names[i],
                hatrack_counters[i] - hatrack_last_counters[i]);

        hatrack_last_counters[i] = hatrack_counters[i];
    }

    for (i = 0; i < HATRACK_YN_COUNTERS_NUM; i++) {
        y_cur   = hatrack_yn_counters[i][0];
        n_cur   = hatrack_yn_counters[i][1];
        y_last  = hatrack_last_yn_counters[i][0];
        n_last  = hatrack_last_yn_counters[i][1];
        ydelta  = y_cur - y_last;
        ndelta  = n_cur - n_last;
        total   = ydelta + ndelta;
        percent = (((double)ydelta) / (double)total) * 100.0;

        hatrack_last_yn_counters[i][0] = y_cur;
        hatrack_last_yn_counters[i][1] = n_cur;

        if (!total) {
            continue;
        }

        fprintf(stderr,
                "%s:\t %llu y, %llu n of %llu (%.2f%% y)\n",
                hatrack_yn_counter_names[i],
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

    for (i = 0; i < HATRACK_COUNTERS_NUM; i++) {
        if (!hatrack_counters[i]) {
            continue;
        }
        fprintf(stderr,
                "%s:\t %llu\n",
                hatrack_counter_names[i],
                hatrack_counters[i]);
    }

    for (i = 0; i < HATRACK_YN_COUNTERS_NUM; i++) {
        total = hatrack_yn_counters[i][0] + hatrack_yn_counters[i][1];

        if (!total) {
            continue;
        }

        fprintf(stderr,
                "%s:\t %llu y, %llu n of %llu (%.2f%% y)\n",
                hatrack_yn_counter_names[i],
                hatrack_yn_counters[i][0],
                hatrack_yn_counters[i][1],
                total,
                (double)100.0
                    * (((double)hatrack_yn_counters[i][0]) / (double)total));
    }
}

#endif
