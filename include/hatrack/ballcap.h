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
 *  Name:           ballcap.h
 *  Description:    Besides a Lot of Locking, Clearly Awesomely Parallel
 *
 *                  Uses pthread locks on a per-bucket basis, and
 *                  allows multiple simultaneous writers, except when
 *                  performing table migration.
 *
 *                  Also uses our strategy from Lohat to ensure we can
 *                  provide a fully consistent ordered view of the hash
 *                  table.
 *
 *                  This table is based on newshat, but changes the
 *                  record structure to use our approach to managing
 *                  record history that we developed for the lohat
 *                  algorithms (including woolhat, which is an
 *                  incremental improvement on lohat, adding wait
 *                  freedom; something a lock-based algorithm can
 *                  never have).
 *
 *                  This hash table is mainly intended to allow us to
 *                  benchmark our lock-free and wait free hash tables
 *                  that use consistent ordering, with a table that
 *                  uses locks, but has the same consistent ordering
 *                  capabilities. And indeed, in my testing so far,
 *                  performance is not appreciably different from the
 *                  lohats (though it's probably better on systems
 *                  without a 128-bit CAS operation).
 *
 *                  That is to say, I don't really expect people to
 *                  consider using this table in real-world
 *                  applications (or patterning a table off of it).
 *
 *                  Given all the above, the implementation of ballcap
 *                  is mostly uncommented; see implementations of
 *                  either newshat or lohat0 for documentation on the
 *                  algorithm.
 *
 *  Author:         John Viega, john@zork.org
 */

#ifndef __BALLCAP_H__
#define __BALLCAP_H__

#include <hatrack_common.h>

#include <pthread.h>

// clang-format off
typedef struct ballcap_record_st ballcap_record_t;

/*
 * Ballcap is our only locking table with a flag on records to
 * indicate whether or not they're deleted. Everything else relies on
 * an explicit epoch counter that lives in the data structure being
 * set to 0.
 *
 * However, like the lohat family (including woolhat), records are
 * allocated via mmm, and so the epoch field is somewhat transparent
 * to us. We could use a macro to extract it quickly, since it's at a
 * fixed offset, but space is cheap. All of those implementations
 * support a delete field.
 *
 * For the lock-free variants that do not use mmm (and do not provide
 * consistency), we go back to using a 0 epoch to indicate deletion.
 *
 *
 */

struct ballcap_record_st {
    bool                 deleted;
    void                *item;
    ballcap_record_t    *next;
};

typedef struct {
    hatrack_hash_t       hv;
    ballcap_record_t    *record;
    bool                 migrated;
    pthread_mutex_t      mutex;
} ballcap_bucket_t;

typedef struct {
    uint64_t             last_slot;
    uint64_t             threshold;
    _Atomic uint64_t     used_count;
    ballcap_bucket_t     buckets[];
} ballcap_store_t;

typedef struct {
    _Atomic uint64_t     item_count;
    ballcap_store_t     *store_current;
    pthread_mutex_t      migrate_mutex;
} ballcap_t;

ballcap_t      *ballcap_new      (void);
ballcap_t      *ballcap_new_size (char);
void            ballcap_init     (ballcap_t *);
void            ballcap_init_size(ballcap_t *, char);
void            ballcap_cleanup  (ballcap_t *);
void            ballcap_delete   (ballcap_t *);
void           *ballcap_get      (ballcap_t *, hatrack_hash_t, bool *);
void           *ballcap_put      (ballcap_t *, hatrack_hash_t, void *, bool *);
void           *ballcap_replace  (ballcap_t *, hatrack_hash_t, void *, bool *);
bool            ballcap_add      (ballcap_t *, hatrack_hash_t, void *);
void           *ballcap_remove   (ballcap_t *, hatrack_hash_t, bool *);
uint64_t        ballcap_len      (ballcap_t *);
hatrack_view_t *ballcap_view     (ballcap_t *, uint64_t *, bool);

#endif
