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
 *  Name:           hatomic.h
 *  Description:    Macros useful for atomicity
 *
 *  Author:         John Viega, john@zork.org
 *
 */

#ifndef __HATOMIC_H__
#define __HATOMIC_H__

#include "counters.h"
#include <stdatomic.h>

// Most of our reads are agnostic to memory ordering. We use this
// macro whenever it doesn't matter if an update is happening
// concurrently, we'll take either version.
#define atomic_read(x) atomic_load_explicit(x, memory_order_relaxed)

/* Most of our writes will need ordering, except when initializing
 * variables, which isn't worth worrying about.
 *
 * Compare-And-Swap is our workhorse for writing, and by default
 * provides sequentially consistent memory ordering.
 */

#define CAS(target, expected, desired)                                         \
    atomic_compare_exchange_strong(target, expected, desired)

#ifdef HATRACK_COUNTERS
#define LCAS_DEBUG(target, expected, desired, id)                              \
    HATRACK_YN_CTR(CAS(target, expected, desired), id)
#else
#define LCAS_DEBUG(target, expected, desired, id) CAS(target, expected, desired)
#endif

#define LCAS(target, expected, desired, id)                                    \
    LCAS_DEBUG(target, expected, desired, id)
#define LCAS_SKIP(target, expected, desired, id) CAS(target, expected, desired)

#endif
