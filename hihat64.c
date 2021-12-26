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
 *  Name:           hihat64.c
 *  Description:    Half-Interesting HAsh Table w/ single-word CAS only.
 *                  This is much like hihat1, with the exception that
 *                  we do not use a double-word Compare-And-Swap, as
 *                  it is not available on all architectures.
 *
 *  Author:         John Viega, john@zork.org
 *
 */

#include "hihat64.h"

// clang-format off
static hihat64_store_t  *hihat64_store_new        (uint64_t);
static void            *hihat64_store_get         (hihat64_store_t *,
						   hihat64_t *,
					           hatrack_hash_t *, bool *);
static void            *hihat64_store_put         (hihat64_store_t *,
						   hihat64_t *,
						   hatrack_hash_t *,
						   void *, bool *);
static bool             hihat64_store_put_if_empty(hihat64_store_t *,
						   hihat64_t *,
						   hatrack_hash_t *,
						   void *);
static void            *hihat64_store_remove      (hihat64_store_t *,
						   hihat64_t *,
					           hatrack_hash_t *,
						   bool *);
static hihat64_store_t *hihat64_store_migrate     (hihat64_store_t *,
						   hihat64_t *);
// clang-format on

void
hihat64_init(hihat64_t *self)
{
    hihat64_store_t *store = hihat64_store_new(HATRACK_MIN_SIZE);

    atomic_store(&self->store_current, store);
}

void *
hihat64_get(hihat64_t *self, hatrack_hash_t *hv, bool *found)
{
    void *ret;

    mmm_start_basic_op();
    ret = hihat64_store_get(self->store_current, self, hv, found);
    mmm_end_op();

    return ret;
}

void *
hihat64_put(hihat64_t *self, hatrack_hash_t *hv, void *item, bool *found)
{
    void *ret;

    mmm_start_basic_op();
    ret = hihat64_store_put(self->store_current, self, hv, item, found);
    mmm_end_op();

    return ret;
}

bool
hihat64_put_if_empty(hihat64_t *self, hatrack_hash_t *hv, void *item)
{
    bool ret;

    mmm_start_basic_op();
    ret = hihat64_store_put_if_empty(self->store_current, self, hv, item);
    mmm_end_op();

    return ret;
}

void *
hihat64_remove(hihat64_t *self, hatrack_hash_t *hv, bool *found)
{
    void *ret;

    mmm_start_basic_op();
    ret = hihat64_store_remove(self->store_current, self, hv, found);
    mmm_end_op();

    return ret;
}

void
hihat64_delete(hihat64_t *self)
{
    hihat64_store_t  *store   = atomic_load(&self->store_current);
    hihat64_bucket_t *buckets = store->buckets;
    hihat64_bucket_t *p       = buckets;
    hihat64_bucket_t *end     = store->buckets + (store->last_slot + 1);
    hihat64_record_t *rec;

    while (p < end) {
        rec = hatrack_pflag_clear(atomic_load(&p->record),
                                  HIHAT64_F_MOVED | HIHAT64_F_MOVING
                                      | HIHAT64_F_USED);
        if (rec) {
            mmm_retire_unused(rec);
        }
        p++;
    }

    mmm_retire(store);
    free(self);
}

uint64_t
hihat64_len(hihat64_t *self)
{
    return self->store_current->used_count - self->store_current->del_count;
}

hatrack_view_t *
hihat64_view(hihat64_t *self, uint64_t *num)
{
    hatrack_view_t *view;
    hatrack_view_t *p;
    uint64_t        hv;
#ifdef HIHAT64_USE_FULL_HASH
    uint64_t hv2;
#endif
    hihat64_bucket_t *cur;
    hihat64_bucket_t *end;
    hihat64_record_t *record;
    hihat64_record_t *deflagged;
    uint64_t          num_items;
    hihat64_store_t  *store;

    mmm_start_basic_op();

    store = atomic_load(&self->store_current);
    view  = (hatrack_view_t *)malloc(sizeof(hatrack_view_t)
                                    * (store->last_slot + 1));
    p     = view;
    cur   = store->buckets;
    end   = cur + (store->last_slot + 1);

    while (cur < end) {
        record = atomic_load(&cur->record);
        if (!hatrack_pflag_test(record, HIHAT64_F_USED)) {
            cur++;
            continue;
        }
        deflagged = hatrack_pflag_clear(record,
                                        HIHAT64_F_USED | HIHAT64_F_MOVED
                                            | HIHAT64_F_MOVING);
        hv        = atomic_load(&cur->h1);
#ifdef HIHAT64_USE_FULL_HASH
        hv2 = atomic_load(&cur->h2);
#endif
        p->hv.w1 = hv;
#ifdef HIHAT64_USE_FULL_HASH
        p->hv.w2 = hv2;
#endif
        p->item       = deflagged->item;
        p->sort_epoch = mmm_get_create_epoch(deflagged);

        p++;
        cur++;
    }

    num_items = p - view;
    *num      = num_items;

    if (!num_items) {
        free(view);
        mmm_end_op();
        return NULL;
    }

    view = realloc(view, *num * sizeof(hatrack_view_t));

    // Unordered buckets should be in random order, so quicksort is a
    // good option.
    qsort(view, num_items, sizeof(hatrack_view_t), hatrack_quicksort_cmp);

    mmm_end_op();

    return view;
}

static hihat64_store_t *
hihat64_store_new(uint64_t size)
{
    hihat64_store_t *store = (hihat64_store_t *)mmm_alloc_committed(
        sizeof(hihat64_store_t) + sizeof(hihat64_bucket_t) * size);

    store->last_slot  = size - 1;
    store->threshold  = hatrack_compute_table_threshold(size);
    store->used_count = ATOMIC_VAR_INIT(0);
    store->del_count  = ATOMIC_VAR_INIT(0);

    return store;
}

static void *
hihat64_store_get(hihat64_store_t *self,
                  hihat64_t       *top,
                  hatrack_hash_t  *hvp,
                  bool            *found)
{
    uint64_t bix = hatrack_bucket_index(hvp, self->last_slot);
    uint64_t i;
    uint64_t shv1;
#ifdef HIHAT64_USE_FULL_HASH
    uint64_t shv2;
#endif
    hihat64_bucket_t *bucket;
    hihat64_record_t *record;

    for (i = 0; i <= self->last_slot; i++) {
        bucket = &self->buckets[bix];
        shv1   = atomic_load(&bucket->h1);
#ifndef HIHAT64_USE_FULL_HASH
        if (!shv1) {
            goto not_found;
        }
#endif
        if (shv1 != hvp->w1) {
            bix = (bix + 1) & self->last_slot;
            continue;
        }
#ifdef HIHAT64_USE_FULL_HASH
        shv2 = atomic_load(&bucket->h2);
        // We could also check for !shv1 here, but it's statistically
        // a wasted operation.
        if (!shv2) {
            goto not_found;
        }
        if (shv2 != hvp->w2) {
            bix = (bix + 1) & self->last_slot;
            continue;
        }
#endif
        goto found_bucket;
    }
not_found:
    if (found) {
        *found = false;
    }
    return NULL;

found_bucket:
    record = hatrack_pflag_clear(atomic_load(&bucket->record),
                                 HIHAT64_F_MOVING | HIHAT64_F_MOVED);
    if (!hatrack_pflag_test(record, HIHAT64_F_USED)) {
        goto not_found;
    }
    record = hatrack_pflag_clear(record, HIHAT64_F_USED);
    if (found) {
        *found = true;
    }
    return record->item;
}

static void *
hihat64_store_put(hihat64_store_t *self,
                  hihat64_t       *top,
                  hatrack_hash_t  *hvp,
                  void            *item,
                  bool            *found)
{
    uint64_t          bix = hatrack_bucket_index(hvp, self->last_slot);
    uint64_t          i;
    hihat64_bucket_t *bucket;
    hihat64_record_t *record;
    hihat64_record_t *deflagged = NULL;
    hihat64_record_t *candidate;
    hihat64_record_t *raw_candidate;
    uint64_t          hv;

    for (i = 0; i < self->last_slot; i++) {
        bucket = &self->buckets[bix];
        hv     = 0;
        if (!LCAS(&bucket->h1, &hv, hvp->w1, HIHAT64_CTR_BUCKET_ACQUIRE)) {
            if (hv != hvp->w1) {
                bix = (bix + 1) & self->last_slot;
                continue;
            }
        }
#ifdef HIHAT64_USE_FULL_HASH
        hv = 0;
        if (!LCAS(&bucket->h2, &hv, hvp->w2, HIHAT64_CTR_BUCKET_ACQUIRE2)) {
            if (hv != hvp->w2) {
                bix = (bix + 1) & self->last_slot;
                continue;
            }
        }
#endif
        goto found_bucket;
    }

migrate_and_retry:
    self = hihat64_store_migrate(self, top);
    return hihat64_store_put(self, top, hvp, item, found);

found_bucket:
    record = atomic_load(&bucket->record);
    if (hatrack_pflag_test(record, HIHAT64_F_MOVING)) {
        goto migrate_and_retry;
    }
    candidate       = mmm_alloc_committed(sizeof(hihat64_record_t));
    candidate->item = item;
    raw_candidate   = candidate;
    candidate       = hatrack_pflag_set(candidate, HIHAT64_F_USED);

    if (!record) {
        if (atomic_fetch_add(&self->used_count, 1) >= self->threshold) {
            mmm_retire_unused(raw_candidate);
            goto migrate_and_retry;
        }
    }
    else {
        deflagged = hatrack_pflag_clear(record, HIHAT64_F_USED);
    }
    if (!LCAS(&bucket->record, &record, candidate, HIHAT64_CTR_REC_INSTALL)) {
        mmm_retire_unused(raw_candidate);
        if (hatrack_pflag_test(record, HIHAT64_F_MOVING)) {
            goto migrate_and_retry;
        }
        if (found) {
            *found = true;
        }

        return item;
    }

    if (deflagged) {
        mmm_retire(deflagged);
        if (!hatrack_pflag_test(record, HIHAT64_F_USED)) {
            atomic_fetch_sub(&self->del_count, 1);
            if (found) {
                *found = false;
                return NULL;
            }
        }
        else {
            if (found) {
                *found = true;
                return deflagged->item;
            }
        }
    }
    if (found) {
        *found = false;
    }

    return NULL;
}

static bool
hihat64_store_put_if_empty(hihat64_store_t *self,
                           hihat64_t       *top,
                           hatrack_hash_t  *hvp,
                           void            *item)
{
    uint64_t          bix = hatrack_bucket_index(hvp, self->last_slot);
    uint64_t          i;
    uint64_t          hv;
    hihat64_bucket_t *bucket;
    hihat64_record_t *record;
    hihat64_record_t *candidate;
    hihat64_record_t *raw_candidate;

    for (i = 0; i < self->last_slot; i++) {
        bucket = &self->buckets[bix];
        hv     = 0;
        if (!LCAS(&bucket->h1, &hv, hvp->w1, HIHAT64_CTR_BUCKET_ACQUIRE)) {
            if (hv != hvp->w1) {
                bix = (bix + 1) & self->last_slot;
                continue;
            }
        }
#ifdef HIHAT64_USE_FULL_HASH
        hv = 0;
        if (!LCAS(&bucket->h2, &hv, hvp->w2, HIHAT64_CTR_BUCKET_ACQUIRE2)) {
            if (hv != hvp->w2) {
                bix = (bix + 1) & self->last_slot;
                continue;
            }
        }
#endif
        goto found_bucket;
    }

migrate_and_retry:
    self = hihat64_store_migrate(self, top);
    return hihat64_store_put_if_empty(self, top, hvp, item);

found_bucket:
    record = atomic_load(&bucket->record);
    if (hatrack_pflag_test(record, HIHAT64_F_MOVING)) {
        goto migrate_and_retry;
    }
    if (hatrack_pflag_test(record, HIHAT64_F_USED)) {
        return false;
    }
    candidate
        = (hihat64_record_t *)mmm_alloc_committed(sizeof(hihat64_record_t));
    raw_candidate   = candidate;
    candidate->item = item;
    candidate       = hatrack_pflag_set(candidate, HIHAT64_F_USED);

    if (!record) {
        if (atomic_fetch_add(&self->used_count, 1) >= self->threshold) {
            mmm_retire_unused(raw_candidate);
            goto migrate_and_retry;
        }
    }
    if (!LCAS(&bucket->record, &record, candidate, HIHAT64_CTR_REC_INSTALL)) {
        mmm_retire_unused(raw_candidate);
        if (hatrack_pflag_test(record, HIHAT64_F_MOVING)) {
            goto migrate_and_retry;
        }
        return true;
    }

    if (record) {
        mmm_retire(record);
        atomic_fetch_sub(&self->del_count, 1);
    }

    return true;
}

static void *
hihat64_store_remove(hihat64_store_t *self,
                     hihat64_t       *top,
                     hatrack_hash_t  *hvp,
                     bool            *found)
{
    uint64_t bix = hatrack_bucket_index(hvp, self->last_slot);
    uint64_t i;
    uint64_t shv1;
#ifdef HIHAT64_USE_FULL_HASH
    uint64_t shv2;
#endif
    hihat64_bucket_t *bucket;
    hihat64_record_t *record;
    hihat64_record_t *candidate;

    for (i = 0; i <= self->last_slot; i++) {
        bucket = &self->buckets[bix];
        shv1   = atomic_load(&bucket->h1);
#ifndef HIHAT64_USE_FULL_HASH
        if (!shv1) {
            goto not_found;
        }
#endif
        if (shv1 != hvp->w1) {
            bix = (bix + 1) & self->last_slot;
            continue;
        }
#ifdef HIHAT64_USE_FULL_HASH
        shv2 = atomic_load(&bucket->h2);
        // We could also check for !shv1 here, but it's statistically
        // a wasted operation.
        if (!shv2) {
            goto not_found;
        }
        if (shv2 != hvp->w2) {
            bix = (bix + 1) & self->last_slot;
            continue;
        }
#endif
        goto found_bucket;
    }
not_found:
    if (found) {
        *found = false;
    }
    return NULL;

found_bucket:
    record = hatrack_pflag_clear(atomic_load(&bucket->record),
                                 HIHAT64_F_MOVING | HIHAT64_F_MOVED);
    if (!hatrack_pflag_test(record, HIHAT64_F_USED)) {
        goto not_found;
    }
    candidate = hatrack_pflag_clear(record, HIHAT64_F_USED);
    if (!LCAS(&bucket->record, &record, candidate, HIHAT64_CTR_DEL)) {
        if (hatrack_pflag_test(record, HIHAT64_F_MOVING)) {
            self = hihat64_store_migrate(self, top);
            return hihat64_store_remove(self, top, hvp, found);
        }

        if (!hatrack_pflag_test(record, HIHAT64_F_USED)) {
            goto not_found;
        }

        if (found) {
            *found = true;
        }

        return NULL;
    }

    if (found) {
        *found = true;
    }

    atomic_fetch_add(&self->del_count, 1);

    return candidate->item;
}

static hihat64_store_t *
hihat64_store_migrate(hihat64_store_t *self, hihat64_t *top)
{
    hihat64_store_t  *new_store;
    hihat64_store_t  *candidate_store;
    uint64_t          new_size;
    hihat64_bucket_t *bucket;
    hihat64_bucket_t *new_bucket;
    hihat64_record_t *record;
    hihat64_record_t *deflagged;
    hihat64_record_t *candidate_record;
    hihat64_record_t *expected_record;
    uint64_t          expected_hv;
    uint64_t          hv;
    uint64_t          i, j;
    uint64_t          bix;
    uint64_t          new_used      = 0;
    uint64_t          expected_used = ~0;

    /* Quickly run through every bucket, and mark any bucket that
     * doesn't already have F_MOVING set.  Note that the CAS could
     * fail due to some other updater, so we keep CASing until we know
     * it was successful.
     */
    for (i = 0; i <= self->last_slot; i++) {
        bucket = &self->buckets[i];
        record = atomic_load(&bucket->record);

        do {
            if (hatrack_pflag_test(record, HIHAT64_F_MOVING)) {
                break;
            }
            candidate_record = hatrack_pflag_set(record, HIHAT64_F_MOVING);
        } while (!LCAS(&bucket->record,
                       &record,
                       candidate_record,
                       HIHAT64_CTR_F_MOVING));
        deflagged = hatrack_pflag_clear(record,
                                        HIHAT64_F_USED | HIHAT64_F_MOVED
                                            | HIHAT64_F_MOVING);
        if (hatrack_pflag_test(record, HIHAT64_F_USED)) {
            new_used++;
        }
    }

    new_store = atomic_load(&self->store_next);

    // If we couldn't acquire a store, try to install one. If we fail, free it.
    if (!new_store) {
        new_size        = hatrack_new_size(self->last_slot, new_used);
        candidate_store = hihat64_store_new(new_size);
        // This helps address a potential race condition, where
        // someone could drain the table after resize, having
        // us swap in the wrong length.
        atomic_store(&candidate_store->used_count, ~0);

        if (!LCAS(&self->store_next,
                  &new_store,
                  candidate_store,
                  HIHAT64_CTR_NEW_STORE)) {
            mmm_retire_unused(candidate_store);
        }
        else {
            new_store = candidate_store;
        }
    }

    // At this point, we're sure that any late writers will help us
    // with the migration. Therefore, we can go through each item,
    // and, if it's not fully migrated, we can attempt to migrate it.

    for (i = 0; i <= self->last_slot; i++) {
        bucket    = &self->buckets[i];
        record    = atomic_load(&bucket->record);
        deflagged = hatrack_pflag_clear(record,
                                        HIHAT64_F_USED | HIHAT64_F_MOVED
                                            | HIHAT64_F_MOVING);
        if (hatrack_pflag_test(record, HIHAT64_F_MOVED)) {
            continue;
        }
        if (!hatrack_pflag_test(record, HIHAT64_F_USED)) {
            candidate_record = hatrack_pflag_set(record, HIHAT64_F_MOVED);
            if (LCAS(&bucket->record,
                     &record,
                     candidate_record,
                     HIHAT64_CTR_F_MOVED1)) {
                if (deflagged) {
                    mmm_retire(deflagged);
                }
            }
            continue;
        }

        hv  = atomic_load(&bucket->h1);
        bix = hv & new_store->last_slot;

        for (j = 0; j <= new_store->last_slot; j++) {
            new_bucket  = &new_store->buckets[bix];
            expected_hv = 0;
            if (!LCAS(&new_bucket->h1,
                      &expected_hv,
                      hv,
                      HIHAT64_CTR_MIGRATE_HV)) {
                if (expected_hv != hv) {
                    bix = (bix + 1) & new_store->last_slot;
                    continue;
                }
            }
#ifdef HIHAT64_USE_FULL_HASH
            expected_hv = 0;
            hv          = atomic_load(&bucket->h2);
            if (!LCAS(&new_bucket->h2,
                      &expected_hv,
                      hv,
                      HIHAT64_CTR_MIGRATE_HV2)) {
                if (__builtin_expect((expected_hv != hv), 0)) {
                    bix = (bix + 1) & new_store->last_slot;
                    continue;
                }
            }
#endif

            break;
        }

        candidate_record = hatrack_pflag_set(deflagged, HIHAT64_F_USED);
        expected_record  = NULL;
        LCAS(&new_bucket->record,
             &expected_record,
             candidate_record,
             HIHAT64_CTR_MIG_REC);
        candidate_record = hatrack_pflag_set(record, HIHAT64_F_MOVED);
        LCAS(&bucket->record, &record, candidate_record, HIHAT64_CTR_F_MOVED2);
    }

    LCAS(&new_store->used_count,
         &expected_used,
         new_used,
         HIHAT64_CTR_LEN_INSTALL);

    if (LCAS(&top->store_current,
             &self,
             new_store,
             HIHAT64_CTR_STORE_INSTALL)) {
        mmm_retire(self);
    }

    return new_store;
}
