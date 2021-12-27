/* Copyright © 2021 John Viega
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
 *  Name:           config.h
 *  Description:    Preprocessor configurtion variables / defaults.
 *                  You can make changes here where appropriate, but
 *                  generally it's best to do it externally, by editing
 *                  the Makefile.
 *
 *  Author:         John Viega, john@zork.org
 */

#ifndef __CONFIG_H__
#define __CONFIG_H__

#ifdef HATRACK_DEBUG
#ifndef HATRACK_DEBUG_MSG_SIZE
#define HATRACK_DEBUG_MSG_SIZE 128
#endif

#if !defined(HATRACK_DEBUG_RING_LOG) || HATRACK_DEBUG_RING_LOG < 17
#undef HATRACK_DEBUG_RING_LOG
#define HATRACK_DEBUG_RING_LOG 17
#endif

#undef HATRACK_DEBUG_RING_SIZE
#define HATRACK_DEBUG_RING_SIZE (1 << HATRACK_DEBUG_RING_LOG)

#undef HATRACK_DEBUG_RING_LAST_SLOT
#define HATRACK_DEBUG_RING_LAST_SLOT (HATRACK_DEBUG_RING_SIZE - 1)

#ifndef HATRACK_PTR_CHRS
#define HATRACK_PTR_CHRS 16
#endif
#endif

/* Our memory management algorithm keeps an array of thread reader
 * epochs that's shared across threads. The basic idea is that each
 * reader writes the current epoch into their slot in the array in
 * order to declare the current epoch as the one they're reading in.
 * Readers will ignore any writes that are from after the epoch, as
 * well as any objects that were retired before or duing this epoch
 * (retirements are essentially deletions, and write operations are
 * always expected to logically happen at the beginning of an epoch).
 *
 * When we go to clean up a record that has been "retired", we
 * essentially need to check whether there are any readers still
 * active in an epoch after the record was created, and before the
 * record was retired. If there is, then we continue to defer
 * deletion.
 *
 * To do this, we have to scan the reservation for every single
 * thread.  It'd be bad to have to resize the reservations, so we'll
 * keep them in static memory, and only allow a fixed number of
 * threads (HATRACK_THREADS_MAX).
 */
#ifndef HATRACK_THREADS_MAX
#define HATRACK_THREADS_MAX 8192
#endif

/* Each thread goes through its list of retired objects periodically,
 * and deletes anything that can never again be accessed. We basically
 * look every N times we go through the list, where N is a power of
 * two.  I believe this number can stay very low.
 */
#ifndef HATRACK_RETIRE_FREQ_LOG
#define HATRACK_RETIRE_FREQ_LOG 5
#undef HATRACK_RETIRE_FREQ
#endif

#define HATRACK_RETIRE_FREQ (1 << HATRACK_RETIRE_FREQ_LOG)

// Epochs are truncated to this many hex digits for brevity.
#ifndef HATRACK_EPOCH_DEBUG_LEN
#define HATRACK_EPOCH_DEBUG_LEN 8
#endif

/* Doing the macro this way forces you to pick a power-of-two boundary
 * for the hash table size, which is best for alignment, and allows us
 * to use an & to calculate bucket indicies, instead of the more
 * expensive mod operator.
 */
#ifndef HATRACK_MIN_SIZE_LOG
#define HATRACK_MIN_SIZE_LOG 3
#endif

#undef HATRACK_MIN_SIZE
#define HATRACK_MIN_SIZE (1 << HATRACK_MIN_SIZE_LOG)

#ifndef HIHAT1a_MIGRATE_SLEEP_TIME_NS
#define HIHAT1a_MIGRATE_SLEEP_TIME_NS 500000
#endif

#ifndef HATRACK_RETRY_THRESHOLD
#define HATRACK_RETRY_THRESHOLD 6
#endif

// Off by default. Uncommment, or turn them on via the compiler.
// #define HATRACK_ALLOW_TID_GIVEBACKS
// #define HATRACK_DEBUG
// #define HATRACK_MMMALLOC_CTRS  (requires counters to be turned on).
// #define SWIMCAP_INCONSISTENT_VIEW_IS_OKAY
// #define HIHAT64_USE_FULL_HASH
// #define HATRACK_MMMALLOC_CTRS
// #define HATRACK_EXPAND_THRESHOLD
// #define HATRACK_CONTRACT_THRESHOLD
// #define HATRACK_ALWAYS_USE_QSORT

#endif

// Sanity checks.
#if defined(HATRACK_QSORT_THRESHOLD) && defined(HATRACK_ALWAYS_USE_QSORT)
#error "Cannot have both HATRACK_QSORT_THRESHOLD and HATRACK_ALWAYS_USE_QSORT"
#endif

#if !defined(HATRACK_ALWAYS_USE_QSORT) && !defined(HATRACK_QSORT_THRESHOLD)
// A reasonable default.
#define HATRACK_QSORT_THRESHOLD 256
#endif
