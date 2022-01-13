/*
 * Copyright © 2021-2022 John Viega
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
#include <stdbool.h>

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
    "hi-a sleep 1 worked",
    "hi-a sleep 1 failed",    
    "hi-a sleep 2 worked",
    "hi-a sleep 2 failed",
    "wh help requests"
};

char *hatrack_yn_counter_names[HATRACK_YN_COUNTERS_NUM] = {
    "linearize epoch eq",       // 0
    "mmm write commits",        // 1
    "mmm commit helps",         // 2
    "lh bucket acquires",       // 3
    "lh record installs",       // 4
    "lh record delete",         // 5
    "lh store creates",         // 6
    "lh F_MOVING set",          // 7
    "lh F_MOVED (empty)",       // 8
    "lh F_MOVED (deleted)",     // 9
    "lh migrate hash",          // 10
    "lh migrate record",        // 11
    "lh F_MOVED (migrate)",     // 12
    "lh len installed",         // 13
    "lh store installs",        // 14
    "lh-a bucket acquires",     // 15
    "lh-a ptr installs",        // 16
    "lh-a hist hash installs",  // 17
    "lh-a record installs",     // 18
    "lh-a record delete",       // 19
    "lh-a store creates",       // 20
    "lh-a F_MOVING set",        // 21
    "lh-a F_MOVED (empty)",     // 22
    "lh-a F_MOVED (deleted)",   // 23
    "lh-a migrate hash",        // 24
    "lh-a migrate record",      // 25
    "lh-a move other hash",     // 26
    "lh-a install new ptr",     // 27
    "lh-a F_MOVED (migrate)",   // 28
    "lh-a hist ptr installed",  // 29
    "lh-a store installs",      // 30
    "lh-b bucket acquires",     // 31
    "lh-b ptr installs",        // 32
    "lh-b hist hash installs",  // 33
    "lh-b forward installed",   // 34
    "lh-b record installs",     // 35
    "lh-b record delete",       // 36
    "lh-b store creates",       // 37
    "lh-b F_MOVING set",        // 38
    "lh-b F_MOVED (empty)",     // 39
    "lh-b F_MOVED (deleted)",   // 40
    "lh-b migrate hash",        // 41
    "lh-b migrate record",      // 42
    "lh-b move other hash",     // 43
    "lh-b install new ptr",     // 44
    "lh-b F_MOVED (migrate)",   // 45
    "lh-b hist ptr installed",  // 46
    "lh-b store installs",      // 47
    "hih bucket acquires",      // 48
    "hih record installs",      // 49
    "hih record delete",        // 50
    "hih store creates",        // 51
    "hih F_MOVING set",         // 52
    "hih F_MOVED (empty)",      // 53
    "hih migrate hash",         // 54
    "hih migrate record",       // 55
    "hih F_MOVED (migrate)",    // 56
    "hih len installed",        // 57
    "hih store installs",       // 58
    "hiha woke up to no job",   // 59
    "wh bucket acquires",       // 60
    "wh record installs",       // 61
    "wh record delete",         // 62
    "wh store creates",         // 63
    "wh F_MOVING set",          // 64
    "wh F_MOVED (empty)",       // 65
    "wh migrate hash",          // 66
    "wh migrate record",        // 67
    "wh F_MOVED (migrate)",     // 68
    "wh len installed",         // 69
    "wh store installs",        // 70
    "wool bucket acquires",     // 71
    "wool record installs",     // 72
    "wool record delete",       // 73
    "wool store creates",       // 74
    "wool F_MOVING set",        // 75
    "wool F_MOVED (empty)",     // 76
    "wool F_MOVED (deleted)",   // 77
    "wool migrate hash",        // 78
    "wool migrate record",      // 79
    "wool F_MOVED (migrate)",   // 70
    "wool len installed",       // 81
    "wool store installs",      // 82
};

// clang-format on

/*
 * Used to output (to stderr) the difference between counters, from
 * the last time counters_output_delta() was called, until now.
 */
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

    return;
}

void
counters_output_alltime(void)
{
    uint64_t i;
    uint64_t total;
    bool     unused_counters = false;
    bool     print_comma     = false;

    fprintf(stderr, "----------- Counter TOTALS --------------\n");

    for (i = 0; i < HATRACK_COUNTERS_NUM; i++) {
        if (!hatrack_counters[i]) {
            unused_counters = true;
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
            unused_counters = true;
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

    if (unused_counters) {
        fprintf(stderr, "\nUnused counters: ");

        for (i = 0; i < HATRACK_COUNTERS_NUM; i++) {
            if (hatrack_counters[i]) {
                continue;
            }
	    
            if (print_comma) {
                fprintf(stderr, ", %s", hatrack_counter_names[i]);
            }
            else {
                fprintf(stderr, "%s", hatrack_counter_names[i]);
            }
        }

        for (i = 0; i < HATRACK_YN_COUNTERS_NUM; i++) {
            if (hatrack_yn_counters[i][0] || hatrack_yn_counters[i][1]) {
                continue;
            }
	    
            if (print_comma) {
                fprintf(stderr, ", %s", hatrack_yn_counter_names[i]);
            }
            else {
                fprintf(stderr, "%s", hatrack_yn_counter_names[i]);
            }
        }
        fprintf(stderr, "\n");
    }

    return;
}

#endif
