/*
 * Copyright © 2021 John Viega
 *
 * See LICENSE.txt for licensing info.
 *
 *  Name:           counters.h
 *  Description:    In-memory counters for performance monitoring,
 *                  when LOWHAT_DEBUG is on.
 *
 *  Author:         John Viega, john@zork.org
 *
 */

#ifndef __COUNTERS_H__
#define __COUNTERS_H__

#include <stdint.h>
#include <stdatomic.h>

#define CAS(target, expected, desired)                                         \
    atomic_compare_exchange_weak(target, expected, desired)

#ifdef LOWHAT_COUNTERS
extern _Atomic uint64_t lowhat_counters[];
extern char            *lowhat_counter_names[];

extern _Atomic uint64_t lowhat_yn_counters[][2];
extern char            *lowhat_yn_counter_names[];

enum : uint64_t
{
    MMM_CTR_MALLOCS,
    MMM_CTR_FREES,
    MMM_CTR_RETIRE_UNUSED,
    LOWHAT_COUNTERS_NUM
};

enum : uint64_t
{
    LOWHAT_CTR_LINEARIZE_RETRIES,
    LOWHAT_CTR_COMMIT,
    LOWHAT_CTR_COMMIT_HELPS,
    LOWHAT0_CTR_BUCKET_ACQUIRE,
    LOWHAT0_CTR_REC_INSTALL,
    LOWHAT0_CTR_DEL,
    LOWHAT0_CTR_NEW_STORE,
    LOWHAT0_CTR_F_MOVING,
    LOWHAT0_CTR_F_MOVED1,
    LOWHAT0_CTR_F_MOVED2,
    LOWHAT0_CTR_MIGRATE_HV,
    LOWHAT0_CTR_MIG_REC,
    LOWHAT0_CTR_F_MOVED3,
    LOWHAT0_CTR_LEN_INSTALL,
    LOWHAT0_CTR_STORE_INSTALL,
    LOWHAT1_CTR_BUCKET_ACQUIRE,
    LOWHAT1_CTR_PTR_INSTALL,
    LOWHAT1_CTR_HIST_HASH,
    LOWHAT1_CTR_REC_INSTALL,
    LOWHAT1_CTR_DEL,
    LOWHAT1_CTR_NEW_STORE,
    LOWHAT1_CTR_F_MOVING,
    LOWHAT1_CTR_F_MOVED1,
    LOWHAT1_CTR_F_MOVED2,
    LOWHAT1_CTR_MIGRATE_HV,
    LOWHAT1_CTR_MIG_REC,
    LOWHAT1_CTR_MV_IH,
    LOWHAT1_CTR_NEW_PTR,
    LOWHAT1_CTR_F_MOVED3,
    LOWHAT1_CTR_F_HIST,
    LOWHAT1_CTR_STORE_INSTALL,
    LOWHAT2_CTR_BUCKET_ACQUIRE,
    LOWHAT2_CTR_PTR_INSTALL,
    LOWHAT2_CTR_HIST_HASH,
    LOWHAT2_CTR_FWD,
    LOWHAT2_CTR_REC_INSTALL,
    LOWHAT2_CTR_DEL,
    LOWHAT2_CTR_NEW_STORE,
    LOWHAT2_CTR_F_MOVING,
    LOWHAT2_CTR_F_MOVED1,
    LOWHAT2_CTR_F_MOVED2,
    LOWHAT2_CTR_MIGRATE_HV,
    LOWHAT2_CTR_MIG_REC,
    LOWHAT2_CTR_MV_IH,
    LOWHAT2_CTR_NEW_PTR,
    LOWHAT2_CTR_F_MOVED3,
    LOWHAT2_CTR_F_HIST,
    LOWHAT2_CTR_STORE_INSTALL,

    LOWHAT_YN_COUNTERS_NUM
};

static inline _Bool
lowhat_yn_ctr_t(uint64_t id)
{
    atomic_fetch_add(&lowhat_yn_counters[id][0], 1);
    return 1;
}

static inline _Bool
lowhat_yn_ctr_f(uint64_t id)
{
    atomic_fetch_add(&lowhat_yn_counters[id][1], 1);
    return 0;
}

void counters_output_delta(void);
void counters_output_alltime(void);

#define LOWHAT_CTR_ON(id) atomic_fetch_add(&lowhat_counters[id], 1)
#define LOWHAT_CTR_OFF(id)
#define LOWHAT_YN_ON(x, id)  (x) ? lowhat_yn_ctr_t(id) : lowhat_yn_ctr_f(id)
#define LOWHAT_YN_OFF(x, id) (x)

#define LCAS_DEBUG(target, expected, desired, id)                              \
    LOWHAT_YN_CTR(CAS(target, expected, desired), id)

#define LCAS_SKIP(target, expected, desired, id) CAS(target, expected, desired)
#define LCAS(target, expected, desired, id)                                    \
    LCAS_DEBUG(target, expected, desired, id)

#else

#define LOWHAT_CTR_ON(id)
#define LOWHAT_CTR_OFF(id)
#define LOWHAT_YN_ON(x, id)  (x)
#define LOWHAT_YN_OFF(x, id) (x)

#define LCAS_SKIP(target, expected, desired, id)  CAS(target, expected, desired)
#define LCAS_DEBUG(target, expected, desired, id) CAS(target, expected, desired)

#define counters_output_delta()
#define counters_output_alltime()

#endif /* defined(LOWHAT_COUNTERS) */

#define LOWHAT_CTR(id)       LOWHAT_CTR_ON(id)
#define LOWHAT_YN_CTR(x, id) LOWHAT_YN_ON(x, id)
#define LCAS(target, expected, desired, id)                                    \
    LCAS_DEBUG(target, expected, desired, id)

#endif /* __COUNTERS_H__ */
