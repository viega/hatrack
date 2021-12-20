/*
 * Copyright © 2021 John Viega
 *
 * See LICENSE.txt for licensing info.
 *
 *  Name:           counters.h
 *  Description:    In-memory counters for performance monitoring,
 *                  when HATRACK_DEBUG is on.
 *
 *  Author:         John Viega, john@zork.org
 *
 */

#ifndef __COUNTERS_H__
#define __COUNTERS_H__

#include <stdint.h>
#include <stdatomic.h>
#include <stdalign.h>

#define CAS(target, expected, desired)                                         \
    atomic_compare_exchange_weak(target, expected, desired)

#ifdef HATRACK_COUNTERS
extern _Atomic uint64_t hatrack_counters[];
extern char            *hatrack_counter_names[];

extern _Atomic uint64_t hatrack_yn_counters[][2];
extern char            *hatrack_yn_counter_names[];

enum : uint64_t
{
    MMM_CTR_MALLOCS,
    MMM_CTR_FREES,
    MMM_CTR_RETIRE_UNUSED,
    HATRACK_COUNTERS_NUM
};

enum : uint64_t
{
    HATRACK_CTR_LINEARIZE_RETRIES = 0,
    HATRACK_CTR_COMMIT            = 1,
    HATRACK_CTR_COMMIT_HELPS      = 2,
    LOWHAT0_CTR_BUCKET_ACQUIRE    = 3,
    LOWHAT0_CTR_REC_INSTALL       = 4,
    LOWHAT0_CTR_DEL               = 5,
    LOWHAT0_CTR_NEW_STORE         = 6,
    LOWHAT0_CTR_F_MOVING          = 7,
    LOWHAT0_CTR_F_MOVED1          = 8,
    LOWHAT0_CTR_F_MOVED2          = 9,
    LOWHAT0_CTR_MIGRATE_HV        = 10,
    LOWHAT0_CTR_MIG_REC           = 11,
    LOWHAT0_CTR_F_MOVED3          = 12,
    LOWHAT0_CTR_LEN_INSTALL       = 13,
    LOWHAT0_CTR_STORE_INSTALL     = 14,
    LOWHAT1_CTR_BUCKET_ACQUIRE    = 15,
    LOWHAT1_CTR_PTR_INSTALL       = 16,
    LOWHAT1_CTR_HIST_HASH         = 17,
    LOWHAT1_CTR_REC_INSTALL       = 18,
    LOWHAT1_CTR_DEL               = 19,
    LOWHAT1_CTR_NEW_STORE         = 20,
    LOWHAT1_CTR_F_MOVING          = 21,
    LOWHAT1_CTR_F_MOVED1          = 22,
    LOWHAT1_CTR_F_MOVED2          = 23,
    LOWHAT1_CTR_MIGRATE_HV        = 24,
    LOWHAT1_CTR_MIG_REC           = 25,
    LOWHAT1_CTR_MV_IH             = 26,
    LOWHAT1_CTR_NEW_PTR           = 27,
    LOWHAT1_CTR_F_MOVED3          = 28,
    LOWHAT1_CTR_F_HIST            = 29,
    LOWHAT1_CTR_STORE_INSTALL     = 30,
    LOWHAT2_CTR_BUCKET_ACQUIRE    = 31,
    LOWHAT2_CTR_PTR_INSTALL       = 32,
    LOWHAT2_CTR_HIST_HASH         = 33,
    LOWHAT2_CTR_FWD               = 34,
    LOWHAT2_CTR_REC_INSTALL       = 35,
    LOWHAT2_CTR_DEL               = 36,
    LOWHAT2_CTR_NEW_STORE         = 37,
    LOWHAT2_CTR_F_MOVING          = 38,
    LOWHAT2_CTR_F_MOVED1          = 39,
    LOWHAT2_CTR_F_MOVED2          = 40,
    LOWHAT2_CTR_MIGRATE_HV        = 41,
    LOWHAT2_CTR_MIG_REC           = 42,
    LOWHAT2_CTR_MV_IH             = 43,
    LOWHAT2_CTR_NEW_PTR           = 44,
    LOWHAT2_CTR_F_MOVED3          = 45,
    LOWHAT2_CTR_F_HIST            = 46,
    LOWHAT2_CTR_STORE_INSTALL     = 47,
    HIHAT1_CTR_BUCKET_ACQUIRE     = 48,
    HIHAT1_CTR_REC_INSTALL        = 49,
    HIHAT1_CTR_DEL                = 50,
    HIHAT1_CTR_NEW_STORE          = 51,
    HIHAT1_CTR_F_MOVING           = 52,
    HIHAT1_CTR_F_MOVED1           = 53,
    HIHAT1_CTR_MIGRATE_HV         = 54,
    HIHAT1_CTR_MIG_REC            = 55,
    HIHAT1_CTR_F_MOVED2           = 56,
    HIHAT1_CTR_LEN_INSTALL        = 57,
    HIHAT1_CTR_STORE_INSTALL      = 58,
    HIHAT1_CTR_SLEEP_NO_JOB       = 59,
    HATRACK_YN_COUNTERS_NUM
};

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
#define HATRACK_YN_ON(x, id)  (x) ? hatrack_yn_ctr_t(id) : hatrack_yn_ctr_f(id)
#define HATRACK_YN_OFF(x, id) (x)

#define LCAS_DEBUG(target, expected, desired, id)                              \
    HATRACK_YN_CTR(CAS(target, expected, desired), id)

#define LCAS_SKIP(target, expected, desired, id) CAS(target, expected, desired)
#define LCAS(target, expected, desired, id)                                    \
    LCAS_DEBUG(target, expected, desired, id)

#else

#define HATRACK_CTR_ON(id)
#define HATRACK_CTR_OFF(id)
#define HATRACK_YN_ON(x, id)  (x)
#define HATRACK_YN_OFF(x, id) (x)

#define LCAS_SKIP(target, expected, desired, id)  CAS(target, expected, desired)
#define LCAS_DEBUG(target, expected, desired, id) CAS(target, expected, desired)

#define counters_output_delta()
#define counters_output_alltime()

#endif /* defined(HATRACK_COUNTERS) */

#define HATRACK_CTR(id)       HATRACK_CTR_ON(id)
#define HATRACK_YN_CTR(x, id) HATRACK_YN_ON(x, id)
#define LCAS(target, expected, desired, id)                                    \
    LCAS_DEBUG(target, expected, desired, id)

#endif /* __COUNTERS_H__ */
