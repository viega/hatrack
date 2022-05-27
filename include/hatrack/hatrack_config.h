/* Copyright © 2021-2022 John Viega
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
 *  Name:           hatrack_config.h
 *  Description:    Preprocessor configurtion variables / defaults.
 *                  You can make changes here where appropriate, but
 *                  generally it's best to do it externally, by editing
 *                  the Makefile.
 *
 *                  The variables you can consider setting are
 *                  commented; other preprocessor variables are
 *                  calculated here from those variables.
 *
 *  Author:         John Viega, john@zork.org
 */

#ifndef __HATRACK_CONFIG_H__
#define __HATRACK_CONFIG_H__

#include <hatrack/config.h>

/* HATRACK_MIN_SIZE_LOG
 *
 * Specifies the minimum table size, but represented as a base two
 * logarithm.  That is, if you set this to 3 (the minimum value we
 * accept), then you are specifying a minimum table size of 2^3
 * entries.
 *
 * Doing the macro this way forces you to pick a power-of-two boundary
 * for the hash table size, which is best for alignment, and allows us
 * to use an & to calculate bucket indicies, instead of the more
 * expensive mod operator.
 *
 * Also, if you tried to use a table size that isn't a power of two
 * without changing all the code that depends on it, things would
 * break badly :)
 *
 * If you not provide this, it will get the default value 4, which is
 * a table with 16 buckets in it.
 */
#if !defined(HATRACK_MIN_SIZE_LOG) || (HATRACK_MIN_SIZE_LOG < 3)
#define HATRACK_MIN_SIZE_LOG 4
#endif

#undef HATRACK_MIN_SIZE
#define HATRACK_MIN_SIZE (1 << HATRACK_MIN_SIZE_LOG)

/*
 * HATRACK_DEBUG
 *
 * Determines whether or not to compile in code that can be useful for
 * debugging parallel processes.
 *
 * This includes an ASSERT() macro that busy-loops instead of
 * aborting, so that we can keep other threads alive, and attach a
 * debugger to a process (the system assert will cause the process to
 * abort, which isn't ideal if we want to debug when there's a
 * failure).
 *
 * The main feature though is a ring buffer for storing debug
 * messages, in a threadsafe way. When rare events happen, we want to
 * be able to see the debug messages near that event, without having
 * to clog up log files, or slow down the program with a lot of file
 * i/o to write out those log files.
 *
 * The size of the ring buffer is controlled with the variable
 * HATRACK_DEBUG_RING_LOG (see below).
 *
 * Note that, if you use our ASSERT() macro, the most recent log
 * messages will get dumped to stderr, and the thread will spin,
 * waiting for a debugger to attach. There is also an XASSERT() macro
 * that gives a bit more flexibility.
 *
 * When you attach via debugger, there's a small API for searching
 * through the logs that you can call.  See debug.h and debug.c for
 * more information.
 */
// #define HATRACK_DEBUG

/* HATRACK_MMM_DEBUG
 *
 * This variable turns ON debugging information in the memory manager,
 * that causes it to recore when it allocs, retires and frees records,
 * complete with epoch information associated with the memory address.
 *
 * Note that this is seprate from the DEBUG_MMM() macro... which is
 * available any time HATRACK_DEBUG is on.  Note that, if you set
 * HATRACK_MMM_DEBUG on, it imples HATRACK_DEBUG, and we turn it on
 * automatically, if it isn't already.
 */
// #define HATRACK_MMM_DEBUG

#if defined(HATRACK_MMM_DEBUG) && !defined(HATRACK_DEBUG)
#define HATRACK_DEBUG
#endif

#ifdef HATRACK_DEBUG

/* HATRACK_DEBUG_MSG_SIZE
 *
 * When using the ring bugger, this variable controls how many bytes
 * are available for each ring buffer entry. Try to keep it small. And
 * for alignment purposes, you probably want to keep it a power of
 * two, but at least pointer-aligned.
 *
 * If you do not provide this, and debugging is turned on, you'll get
 * 128 bytes per record.  The minimum value is 32 bytes; any lower
 * value will get you 32 bytes.
 *
 * Note that this number does NOT include the extra stuff we keep
 * along with our records, like thread ID and a message counter. Those
 * things get shown when viewing the log, but you don't have to worry
 * about them with the value you set here.
 *
 * Also note that, if you log something that would take up more space
 * than this value allows, it will be truncated to fit.
 */
#if !defined(HATRACK_DEBUG_MSG_SIZE)
#ifndef HATRACK_DEBUG_MSG_SIZE
#define HATRACK_DEBUG_MSG_SIZE 128
#endif
#endif

#if HATRACK_DEBUG_MSG_SIZE & 0x7
#error "Must keep HATRACK_DEBUG_MSG_SIZE aligned to an 8 byte boundary"
#endif

#if HATRACK_DEBUG_MSG_SIZE < 32
#define HATRACK_DEBUG_MSG_SIZE 32
#endif

/* HATRACK_DEBUG_RING_LOG
 *
 * This controls how many entries are in the debugging system's ring
 * buffer. As with HATRACK_MIN_SIZE_LOG above, this is expressed as a
 * base 2 logarithm, so the minimum value of 13 is a mere 8192
 * entries.
 *
 * When building my test cases, they can do so much logging across so many
 * threads, that I've definitely found it valuable to have more than
 * 100K messages in memory, which is why the default value for this is
 * 17 (2^17 = 131,072 entries).
 *
 * In those cases, I've been using 100 threads, reading or writing as
 * fast as they can.  I might need to go back pretty far for data,
 * because threads get preempted...
 *
 * Of course, if you set this value TOO large, you could end up with
 * real memory issues on your system.  But we don't enforce a maximum.
 */
#if !defined(HATRACK_DEBUG_RING_LOG) || HATRACK_DEBUG_RING_LOG < 13
#undef HATRACK_DEBUG_RING_LOG
#define HATRACK_DEBUG_RING_LOG 17
#endif

#undef HATRACK_DEBUG_RING_SIZE
#define HATRACK_DEBUG_RING_SIZE (1 << HATRACK_DEBUG_RING_LOG)

#undef HATRACK_DEBUG_RING_LAST_SLOT
#define HATRACK_DEBUG_RING_LAST_SLOT (HATRACK_DEBUG_RING_SIZE - 1)

/* HATRACK_ASSERT_FAIL_RECORD_LEN
 *
 * When using our ASSERT() macro, when an assertion fails, this
 * variable controls how many previous records to dump from the ring
 * buffer to stdout automatically.
 *
 * Things to note:
 *
 * 1) You can control how much to dump on a case-by-case basis with
 *    the XASSERT() macro, instead.  See debug.h.
 *
 * 2) Since the ASSERT() macro does not stop the entire process, other
 *    threads will continue writing to the ring buffer while the dump
 *    is happening.  If your ring buffer is big enough, I wouldn't
 *    expect that to be a problem, but it's possible that threads
 *    scribble over your debugging data.
 *
 *    If that's an issue, try to get the ASSERT to trigger in a
 *    debugger, setting a breakpoint that only triggers along with the
 *    ASSERT.  That will quickly suspend all the other threads, and
 *    allow you to inspect the state at your leisure.
 *
 * 3) This is NOT a power of two... set it to the actual number of
 *    records you think you want to see.  Default is just 64 records;
 *    you can always choose to dump more.
 */
#ifndef HATRACK_ASSERT_FAIL_RECORD_LEN
#define HATRACK_ASSERT_FAIL_RECORD_LEN (1 << 6)
#endif

#if HATRACK_ASSERT_FAIL_RECORD_LEN > HATRACK_DEBUG_RING_SIZE
#undef HATRACK_ASSERT_FAIL_RECORD_LEN
#define HATRACK_ASSERT_FAIL_RECORD_LEN HATRACK_DEBUG_RING_SIZE
#endif

/* HATRACK_PTR_CHRS
 *
 * This variable controls how much of a pointer you'll see when
 * viewing the ring buffer.
 *
 * The default is the full 16 byte pointer; you probably don't need to
 * ever consider changing this one.
 */
#ifndef HATRACK_PTR_CHRS
#define HATRACK_PTR_CHRS 16
#endif

/* HATRACK_EPOCH_DEBUG_LEN
 *
 * When dumping records from the ring log using MMM_DEBUG(), this
 * variable controls how many digits of the 64-bit epoch to output,
 * for the sake of brevity.  This is measured in characters output,
 * not bits of the epoch.
 *
 * Naturally, we output the least significant bits.
 *
 * Since we keep 64 bit epochs, 8 is the maximum. Anything below 4 is
 * probably worthless.
 */
#ifndef HATRACK_EPOCH_DEBUG_LEN
#define HATRACK_EPOCH_DEBUG_LEN 8
#endif

#if HATRACK_EPOCH_DEBUG_LEN > 8
#error "HATRACK_EPOCH_DEBUG_LEN is set too high."
#endif

#if HATRACK_EPOCH_DEBUG_LEN < 2
#error "HATRACK_EPOCH_DEBUG_LEN is set too low."
#endif

#endif // #ifdef HATRACK_DEBUG

/* HATRACK_THREADS_MAX
 *
 * Our memory management algorithm keeps an array of thread reader
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
 * thread.  It'd be bad to have to resize the reservations array, so
 * we currently keep them in static memory, and only allow a fixed
 * number of threads (HATRACK_THREADS_MAX).
 *
 * For all places in this code base, a threads' index into this array
 * is what we consider its thread ID.  That means, you'll see this
 * value in debug dumps, even though it will usually be different than
 * a pthread id, or the ID that a debugger selects.
 *
 * If you're spinning up and down lots of threads, then make sure to
 * not run out of these.  Though, there's an API for "tid givebacks";
 * see mmm.h.
 */
#ifndef HATRACK_THREADS_MAX
#define HATRACK_THREADS_MAX 4096
#endif

#if HATRACK_THREADS_MAX > 32768
#error "Vector assumes HATRACK_THREADS_MAX is no higher than 32768"
#endif

/* HATRACK_RETIRE_FREQ_LOG
 *
 * Each thread goes through its list of retired objects periodically,
 * and deletes anything that can never again be accessed. We basically
 * look every N times we go through the list, where N is a power of
 * two.  I believe this number can stay very low.
 *
 * On one hand, you don't want to keep scanning stuff over and over
 * that isn't likely to be ready to free yet (in busy systems). On the
 * other hand, you don't want to keep a huge backlog of unfreed
 * records; it could also be problematic when busy.
 *
 * I've experimented a bit with this number on my laptop, and even a
 * value of 0 isn't horrible-- about 1-5x slower than a 5 (when
 * testing with lohat). The improvement starts slowing down quickly
 * when you get to 4, and eventually gets insignificant. The real
 * issue is how much memory you want to spend on dead
 * records... cleaning them up is effectively a really tiny garbage
 * collector pause.
 *
 * My initial conclusion is that 7 seems to be a good default to
 * balance memory usage and performance (run the cleanup routine ever
 * 128 allocs). It may also depend somewhat on the underlying system
 * memory manager.
 */
#ifndef HATRACK_RETIRE_FREQ_LOG
#define HATRACK_RETIRE_FREQ_LOG 7
#endif

#ifdef HATRACK_RETIRE_FREQ
#undef HATRACK_RETIRE_FREQ
#endif

#define HATRACK_RETIRE_FREQ (1 << HATRACK_RETIRE_FREQ_LOG)

/* HIHATa_MIGRATE_SLEEP_TIME_NS
 *
 * The hihat-a variant of the hihat algorithm has late migraters do
 * some modest waiting to see if it can speed up migrations.
 *
 * Generally, this isn't likely to make migrations happen faster per
 * se. The likely impact is that we'll reduce the number of clock
 * cycles wasted, yielding cores for productive work. That seems to be
 * the case so far-- I may expand this to other tables, and even make
 * it a default, in which case the variable name might change to
 * reflect its broaded applicability.
 */
#ifndef HIHATa_MIGRATE_SLEEP_TIME_NS
#define HIHATa_MIGRATE_SLEEP_TIME_NS 500000
#endif

/* HATRACK_RETRY_THRESHOLD
 *
 * Witchhat and Woolhat make migrations wait-free by trying to
 * perform their operations a fixed number of times... which either
 * ends in a successful operation, or a migrate-then-retry.
 *
 * This variable controls how many times these algorithms attempt to
 * retry, before falling back on our "helping" mechanism (basically
 * forcing the table to grow on future resizes until help is not
 * needed).
 *
 * Unless you've got workloads that both add and delete with great
 * frequency and in fairly equal amounts, you will probably never,
 * ever see 7 retries-- it's a logarithmic curve, and at least on my
 * laptop, 6 seems to mean, "almost never use the helping mechanism".
 */

#ifndef HATRACK_RETRY_THRESHOLD
#define HATRACK_RETRY_THRESHOLD 7
#endif

/* HATRACK_COUNTERS
 *
 * This controls whether the event counters get compiled in or not,
 * which we have used to help better understand the performance
 * implication of algorithmic decisions.  See counter.h and counter.c,
 * as well as hatomic.h, that has a macro wrapping our
 * compare-and-swap operation.
 *
 * If this variable is off, there is no performance penalty
 * whatsoever.
 *
 */
// #define HATRACK_COUNTERS

/* HATRACK_MMMALLOC_CTRS
 *
 * If turned on, we will use our counters to count:
 *
 * 1) The number of records allocated via either mmm_alloc() or
 *    mmm_alloc_committed()
 *
 * 2) The number of records actually freed (not just retired).
 *
 * 3) The number of records that were freed via mmm_retire_unused().
 *
 * See counter.{h, c} for more info.  Implies HATRACK_COUNTERS.
 */
// #define HATRACK_MMMALLOC_CTRS

#if defined(HATRACK_MMMALLOC_CTRS) && !defined(HATRACK_COUNTERS)
#define HATRACK_COUNTERS
#endif

/* SWIMCAP_CONSISTENT_VIEWS
 *
 * In swimcap, we give a compile time option to select whether you
 * want consistent views, implemented by having the viewer grab the
 * write lock.
 *
 * Being a single-writer hash table, and given that we have faster
 * multi-writer lock-free tables, this should be pretty worthless
 * to you either way-- don't use swimcap :)
 */
// #define SWIMCAP_CONSISTENT_VIEWS

/* HATRACK_ALWAYS_USE_QUICKSORT
 *
 * For lohat-a and lohat-b, we explore the performance impact on sort
 * operations, if we try different storage strategies. This is all
 * explained in the lowhat-a source code, but: for whatever reason, it
 * generally will make sense when we have partially-ordered arrays to
 * switch from insertion sort, which is optimized for mostly-ordered
 * arrays, and go to qsort(), which is great for general-purpose,
 * randomized contents.
 *
 * If this variable isn't set, the lohat variants will start with an
 * insertion sort, and switch to quicksort if the table gets too
 * large (controlled by HATRACK_QSORT_THRESHOLD below).
 *
 * If this variable IS set, then those tables just will never use the
 * insertion sort at all.
 */
// #define HATRACK_ALWAYS_USE_QSORT

/* HATRACK_ALWAYS_USE_INSERTION_SORT
 *
 * This doesn't live up to its name-- it does not cause us *always*
 * use an insertion sort. Only the lohat variants (lohat-a and
 * lohat-b) support insertion sort as an option... no other algorithms
 * should benefit from it.  Those algorithms are FORCED to use
 * quicksort, if sorting is asked for.
 *
 * Therefore, this applies ONLY to lohat-a and lohat-b.
 *
 * And, generally, I would not recommend this flag for anyone at any
 * time, as it appears to perform abysmally on large arrays, for some
 * reason that I have not yet discerned (PEBCAK?).
 */
// #define HATRACK_ALWAYS_USE_INSSORT

#if defined(HATRACK_ALWAYS_USE_INSSORT) && defined(HATRACK_ALWAYS_USE_QSORT)
#error                                                                         \
    "Cannot have both HATRACK_ALWAYS_USE_INSSORT and "                         \
       "HATRACK_ALWAYS_USE_QSORT"
#endif

/* HATRACK_QSORT_THRESHOLD
 *
 * This variable controls how big the table should be (in terms of
 * number of buckets), before the lohat variants (lohat-a and lohat-b)
 * switch from insertion sort to quicksort. If this is unset, and if
 * HATRACK_ALWAYS_USE_QUICKSORT is also unset, then these algorithms
 * will ALWAYS use an insertion sort.
 *
 * In my VERY limited testing so far, the best threshold on my laptop
 * seems to be disappointingly low-- only about 256 elements. I don't
 * yet understand what's going on, but haven't gotten around to
 * exploring this yet.
 */

#if defined(HATRACK_QSORT_THRESHOLD) && defined(HATRACK_ALWAYS_USE_QSORT)
#error "Cannot have both HATRACK_QSORT_THRESHOLD and HATRACK_ALWAYS_USE_QSORT"
#endif

#if defined(HATRACK_QSORT_THRESHOLD) && defined(HATRACK_ALWAYS_USE_INSSORT)
#error "Cannot have both HATRACK_QSORT_THRESHOLD and HATRACK_ALWAYS_USE_INSSORT"
#endif

#if !defined(HATRACK_ALWAYS_USE_INSORT) && !defined(HATRACK_ALWAYS_USED_QSORT) \
    && !defined(HATRACK_QSORT_THRESHOLD)
#define HATRACK_QSORT_THRESHOLD 256
#endif

/* TOPHAT_USE_LOCKING_ALGORITHMS
 *
 * Tophat is a proof-of-concept that shows how a language or library
 * can start with single-threaded hash tables, and then switch to
 * multi-threaded hash tables seamlessly, the second a new thread
 * spins up. See tophat.h for more details.
 *
 * This variable controls whether, when we migrate, we migrate to an
 * algorithm that uses locks (newshat or ballcap), or a wait free
 * algorithm (witchhat and woolhat).
 *
 * Selecting whether you want better linearization is done at runtime,
 * however.  I did this because there generally doesn't seem to be a
 * good reason to use the locking algorithms, except for performance
 * comparisons.
 *
 * In contrast, one might want to be able to, if the program goes
 * parallel, use witchhat for regular dictionaries to maximize
 * performance, and woolhat for sets, where you need to perform set
 * operations.
 */
// #define TOPHAT_USE_LOCKING_ALGORITHMS

/* HATRACK_SEED_SIZE
 *
 * How many bytes to seed our random number generator with??
 */
#ifndef HATRACK_SEED_SIZE
#define HATRACK_SEED_SIZE 32
#endif

/* HATRACK_RAND_SEED_SIZE
 *
 * To try to make algorithm comparisons as fair as possible, I try to
 * do everything I can to eliminate places where the OS might use a
 * mutex, where there might be contention among threads.
 *
 * Top of that list is malloc() -- where I recommend addressing by
 * linking in the hoard malloc implementation.
 *
 * Second on that list is the random number generator, since we use a
 * lot of random numbers in our testing, and would like to avoid
 * several things:
 *
 * 1) Calling into the kernel more than we need to (e.g., if we were
 *    to read from /dev/urandom).
 *
 * 2) Any locks around RNG APIs.  For instance, I'm pretty sure
 *    arc4random() has such a lock on my machine.
 *
 * 3) Holding on to too much memory.
 *
 * My basic approach is to implement ARC4 ourselves, and keep the
 * state on a per-thread basis, with the seed xor'd with the bottom
 * byte of the thread's pthread id (just to get some variance in the
 * number streams; multiple threads can definitely end up with
 * identical streams of numbers).  We read the seed once, at
 * initialization time.  Generally, this is from /dev/urandom, unless
 * you are looking to do "repeatable" tests, which only make sense in
 * the context of a single thread.  In such a case, the seed will be
 * 128-bits, and zero-padded beyond that.
 *
 * Therefore, HATRACK_RAND_SEED_SIZE needs to be at least 16.
 *
 * This variable controls that.
 *
 * Note that ARC4 isn't very good cryptographically, but we don't need
 * cryptographically strong random numbers for our purposes. This just
 * gets the job done with a quick algorithm, that can be done without
 * hitting the kernel, after initialization.
 */
#ifndef HATRACK_RAND_SEED_SIZE
#define HATRACK_RAND_SEED_SIZE 32
#endif

#if HATRACK_RAND_SEED_SIZE < 16
#error "Invalid seed size"
#endif

/* HATRACK_COMPILE_ALL_ALGORITHMS
 *
 * If this is defined, we will compile in all algorithms, not just the
 * ones used to back hatrack_dict() and hatrack_set().
 *
 * This is used in the test harness, mainly for comparative /
 * performance testing.
 *
 * Note that, without this on, we only compile in witchhat and
 * woolhat.
 */

// #define HATRACK_COMPILE_ALL_ALGORITHMS

/* HATRACK_MAX_HATS
 *
 * testhat has an interface to "register" algorithms, and then
 * testhat_new() can select algorithms that have been registered.
 *
 * The array of algorithm info is currently static, and this controls
 * how big it is. I honestly hope I will never, ever get anywhere near
 * this number!
 */
#ifndef HATRACK_MAX_HATS
#define HATRACK_MAX_HATS 1024
#endif

/* HATRACK_SKIP_XXH_INLINING
 *
 * By default, we inline XXH-128.  Defining this will skip the inlining, and
 * we will end up with a bunch of symbols in the binary.
 */
//#define HATRACK_SKIP_XXH_INLINING

#ifndef HATRACK_SKIP_XXH_INLINING
#define XXH_INLINE_ALL
#endif

/* HATRACK_FULL_LINEAR_PROBES
 *
 * Crown currently can choose between two strategies to avoid a
 * probing race condition.  Use this to change which strategy we use.
 *
 * The default is to skip but ensure we "help" other threads who have
 * the same hash value as us, and have acquired buckets.
 *
 * I'd expect it to be faster, and it does seem to be the case.
 */
// #define HATRACK_FULL_LINEAR_PROBES

/* HATRACK_SKIP_ON_MIGRATIONS
 *
 * If you do NOT define HATRACK_FULL_LINEAR_PROBES, then the put and
 * add operations will use the bucket cache by default. However, the
 * migration operation will NOT, because I am pretty sure that, on
 * average, tables will be sparse enough on migration that the
 * optimization won't help, and might even hurt.
 */
// #define HATRACK_SKIP_ON_MIGRATIONS

/* QUEUE_HELP_STEPS
 *
 * The "bonus" directory has a fast, wait-free queue
 * implementation. Enqueue operations will try to enqueue
 * QUEUE_HELP_STEPS times, before asking for "help".
 */

#ifndef QUEUE_HELP_STEPS
#define QUEUE_HELP_STEPS 4
#endif

#if QUEUE_HELP_STEPS > 60 || QUEUE_HELP_STEPS < 2
#error "QUEUE_HELP_STEPS must be between 2 and 60, inclusive"
#endif

#ifndef QSIZE_LOG_DEFAULT
#define QSIZE_LOG_DEFAULT 14
#endif

#ifndef QSIZE_LOG_MIN
#define QSIZE_LOG_MIN 6
#endif

#ifndef QSIZE_LOG_MAX
#define QSIZE_LOG_MAX 25
#endif

#if QSIZE_LOG_MIN > QSIZE_LOG_DEFAULT
#error "QSIZE_LOG_MIN must be <= QSIZE_LOG_DEFAULT"
#endif

#if QSIZE_LOG_MAX < QSIZE_LOG_DEFAULT
#error "QSIZE_LOG_MAX must be >= QSIZE_LOG_DEFAULT"
#endif

/* HATSTACK_WAIT_FREE
 *
 * You can define this to make HATSTACK wait free. It adds a tiny bit
 * of overhead in the average case, but seems to be worth it for
 * prevening maximal contention.
 */
#undef HATSTACK_WAIT_FREE

/* HATSTACK_RETRY_THRESHOLD
 *
 * This is the number of times a push operation needs to retry before
 * we ask pops to perform backoff.
 */
#define HATSTACK_RETRY_THRESHOLD  7

/* HATSTACK_MAX_BACKOFF
 *
 * Maximum backoff value for pops.
 */

#define HATSTACK_MAX_BACKOFF 4

/* HATSTACK_MIN_STORE_SZ_LOG
 *
 * Minimium size of a hatstack store, expressed as a base 2 log.
 * I.e., this represents a power-of-two.
 */
#define HATSTACK_MIN_STORE_SZ_LOG 6

/*
 * HATSTACK_TEST_LLSTACK
 *
 * A linked list stack can be compiled into queue example apps.  But
 * it often performs very poorly, so it's off by default.
 */
#undef HATSTACK_TEST_LLSTACK


#ifndef FLEXARRAY_DEFAULT_GROW_SIZE_LOG
#define FLEXARRAY_DEFAULT_GROW_SIZE_LOG 8
#endif

#ifdef HAVE_C11_ENUMS
#define enum64(x, ...)                                                         \
    typedef enum : uint64_t                                                    \
    {                                                                          \
        __VA_ARGS__                                                            \
    } x
#else
#define enum64(x, ...)                                                         \
    typedef enum                                                               \
    {                                                                          \
        __VA_ARGS__,                                                           \
        HACK_TO_MAKE_64_BIT_##x = 0xffffffffffffffff                           \
    } x
#endif

#endif

