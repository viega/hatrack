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
 *  Name:           counters.h
 *  Description:    In-memory counters for performance monitoring,
 *                  when HATRACK_COUNTERS is on.
 *
 *                  When these are off, no counter-related code will
 *                  be generated. For instance, hatomic.h has
 *                  compare-and-swap wrapper macros that can increment
 *                  a Yes / No counter, based on the result, but if
 *                  HATRACK_COUNTERS is not defined, it just compiles
 *                  down to atomic_compare_exchange_strong().
 *
 *                  When they're on, we still do what we can to
 *                  minimize the performance impact, while keeping
 *                  them atomic, using sequentially consistent updates
 *                  (via atomic_fetch_add()), and keeping them in
 *                  statically allocated memory (though, it would be
 *                  better to keep them in dynamic memory, if it's in
 *                  the same cache line as what we're operating on at
 *                  the time).
 *
 *                  Right now, I'm primarily using these to monitor
 *                  CAS success and fail rates, in lock free
 *                  algorithms. And they do add about 80% overhead,
 *                  since I'm putting these counters in critical
 *                  sections and creating a lot of unnecessary cache
 *                  misses.
 *
 *                  Still, even when monitoring every single CAS and
 *                  more, while performance degrades, almost every
 *                  program under the sun could tolerate keeping these
 *                  on all the time.
 *
 *
 *  Author:         John Viega, john@zork.org
 *
 */

#ifndef __COUNTERS_H__
#define __COUNTERS_H__

#include <hatrack_config.h>

#include <stdint.h>
#include <stdalign.h>
#include <stdatomic.h>

#ifdef HATRACK_COUNTERS
extern _Atomic uint64_t hatrack_counters[];
extern char            *hatrack_counter_names[];

extern _Atomic uint64_t hatrack_yn_counters[][2];
extern char            *hatrack_yn_counter_names[];

enum64(hatrack_counter_names,
    HATRACK_CTR_MALLOCS,
    HATRACK_CTR_FREES,
    HATRACK_CTR_RETIRE_UNUSED,
    HATRACK_CTR_STORE_SHRINK,
    HATRACK_CTR_WH_HELP_REQUESTS,
    HATRACK_CTR_HIa_SLEEP1_WORKED,
    HATRACK_CTR_HIa_SLEEP1_FAILED,
    HATRACK_CTR_HIa_SLEEP2_WORKED,
    HATRACK_CTR_HIa_SLEEP2_FAILED,
    HATRACK_COUNTERS_NUM
);

enum64(hatrack_yn_counter_names,
    HATRACK_CTR_LINEAR_EPOCH_EQ = 0,
    HATRACK_CTR_COMMIT          = 1,
    HATRACK_CTR_COMMIT_HELPS    = 2,
    LOHAT_CTR_BUCKET_ACQUIRE    = 3,
    LOHAT_CTR_REC_INSTALL       = 4,
    LOHAT_CTR_DEL               = 5,
    LOHAT_CTR_NEW_STORE         = 6,
    LOHAT_CTR_F_MOVING          = 7,
    LOHAT_CTR_F_MOVED1          = 8,
    LOHAT_CTR_F_MOVED2          = 9,
    LOHAT_CTR_MIGRATE_HV        = 10,
    LOHAT_CTR_MIG_REC           = 11,
    LOHAT_CTR_F_MOVED3          = 12,
    LOHAT_CTR_LEN_INSTALL       = 13,
    LOHAT_CTR_STORE_INSTALL     = 14,
    LOHATa_CTR_BUCKET_ACQUIRE   = 15,
    LOHATa_CTR_PTR_INSTALL      = 16,
    LOHATa_CTR_HIST_HASH        = 17,
    LOHATa_CTR_REC_INSTALL      = 18,
    LOHATa_CTR_DEL              = 19,
    LOHATa_CTR_NEW_STORE        = 20,
    LOHATa_CTR_F_MOVING         = 21,
    LOHATa_CTR_F_MOVED1         = 22,
    LOHATa_CTR_F_MOVED2         = 23,
    LOHATa_CTR_MIGRATE_HV       = 24,
    LOHATa_CTR_MIG_REC          = 25,
    LOHATa_CTR_MV_IH            = 26,
    LOHATa_CTR_NEW_PTR          = 27,
    LOHATa_CTR_F_MOVED3         = 28,
    LOHATa_CTR_F_HIST           = 29,
    LOHATa_CTR_STORE_INSTALL    = 30,
    LOHATb_CTR_BUCKET_ACQUIRE   = 31,
    LOHATb_CTR_PTR_INSTALL      = 32,
    LOHATb_CTR_HIST_HASH        = 33,
    LOHATb_CTR_FWD              = 34,
    LOHATb_CTR_REC_INSTALL      = 35,
    LOHATb_CTR_DEL              = 36,
    LOHATb_CTR_NEW_STORE        = 37,
    LOHATb_CTR_F_MOVING         = 38,
    LOHATb_CTR_F_MOVED1         = 39,
    LOHATb_CTR_F_MOVED2         = 40,
    LOHATb_CTR_MIGRATE_HV       = 41,
    LOHATb_CTR_MIG_REC          = 42,
    LOHATb_CTR_MV_IH            = 43,
    LOHATb_CTR_NEW_PTR          = 44,
    LOHATb_CTR_F_MOVED3         = 45,
    LOHATb_CTR_F_HIST           = 46,
    LOHATb_CTR_STORE_INSTALL    = 47,
    HIHAT_CTR_BUCKET_ACQUIRE    = 48,
    HIHAT_CTR_REC_INSTALL       = 49,
    HIHAT_CTR_DEL               = 50,
    HIHAT_CTR_NEW_STORE         = 51,
    HIHAT_CTR_F_MOVING          = 52,
    HIHAT_CTR_F_MOVED1          = 53,
    HIHAT_CTR_MIGRATE_HV        = 54,
    HIHAT_CTR_MIG_REC           = 55,
    HIHAT_CTR_F_MOVED2          = 56,
    HIHAT_CTR_LEN_INSTALL       = 57,
    HIHAT_CTR_STORE_INSTALL     = 58,
    HIHAT_CTR_SLEEP_NO_JOB      = 59,
    WITCHHAT_CTR_BUCKET_ACQUIRE = 60,
    WITCHHAT_CTR_REC_INSTALL    = 61,
    WITCHHAT_CTR_DEL            = 62,
    WITCHHAT_CTR_F_MOVING       = 63,
    WITCHHAT_CTR_NEW_STORE      = 64,
    WITCHHAT_CTR_F_MOVED1       = 65,
    WITCHHAT_CTR_MIGRATE_HV     = 66,
    WITCHHAT_CTR_MIG_REC        = 67,
    WITCHHAT_CTR_F_MOVED2       = 68,
    WITCHHAT_CTR_LEN_INSTALL    = 69,
    WITCHHAT_CTR_STORE_INSTALL  = 70,
    WOOLHAT_CTR_BUCKET_ACQUIRE  = 71,
    WOOLHAT_CTR_REC_INSTALL     = 72,
    WOOLHAT_CTR_DEL             = 73,
    WOOLHAT_CTR_NEW_STORE       = 74,
    WOOLHAT_CTR_F_MOVING        = 75,
    WOOLHAT_CTR_F_MOVED1        = 76,
    WOOLHAT_CTR_F_MOVED2        = 77,
    WOOLHAT_CTR_MIGRATE_HV      = 78,
    WOOLHAT_CTR_MIG_REC         = 79,
    WOOLHAT_CTR_F_MOVED3        = 80,
    WOOLHAT_CTR_LEN_INSTALL     = 81,
    WOOLHAT_CTR_STORE_INSTALL   = 82,
    HATRACK_YN_COUNTERS_NUM
);

static inline _Bool
hatrack_yn_ctr_t(uint64_t id)
{
    atomic_fetch_add(&hatrack_yn_counters[id][0], 1);

    return 1;
}

static inline _Bool
hatrack_yn_ctr_f(uint64_t id)
{
    atomic_fetch_add(&hatrack_yn_counters[id][1], 1);

    return 0;
}

void counters_output_delta(void);
void counters_output_alltime(void);

#define HATRACK_CTR_ON(id) atomic_fetch_add(&hatrack_counters[id], 1)
#define HATRACK_CTR_OFF(id)
#define HATRACK_YN_ON(x, id)  ((x) ? hatrack_yn_ctr_t(id) : hatrack_yn_ctr_f(id))
#define HATRACK_YN_OFF(x, id) (x)
#define HATRACK_YN_ON_NORET(x, id)                                             \
    ((x) ? hatrack_yn_ctr_t(id) : hatrack_yn_ctr_f(id))
#define HATRACK_YN_OFF_NORET(x, id)

#else

#define HATRACK_CTR_ON(id)
#define HATRACK_CTR_OFF(id)
#define HATRACK_YN_ON(x, id) (x)
#define HATRACK_YN_ON_NORET(x, id)
#define HATRACK_YN_OFF(x, id) (x)
#define HATRACK_YN_OFF_NORET(x, id)
#define counters_output_delta()
#define counters_output_alltime()

#endif /* defined(HATRACK_COUNTERS) */

#define HATRACK_CTR(id)             HATRACK_CTR_ON(id)
#define HATRACK_YN_CTR(x, id)       HATRACK_YN_ON(x, id)
#define HATRACK_YN_CTR_NORET(x, id) HATRACK_YN_ON_NORET(x, id)

#endif /* __COUNTERS_H__ */
