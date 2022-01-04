/*
 * Copyright © 2022 John Viega
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
 *  Name:           oldhat.c
 *  Description:    Old, Legacy, Dated Hardware-Acceptable Table
 *
 *                  This table stays away from 128-bit compare-and
 *                  swap operations.  It does so by keeping all bucket
 *                  information in a single structure, and only ever
 *                  CASing a pointer to said structure.
 *
 *                  The net result is we require a lot of dynamic
 *                  memory allocation.
 *
 *  Author:         John Viega, john@zork.org
 *
 * TODO: Add a cleanup handler to retire all the old store records.
 * TODO: Fix the hihat / witchhat expected_use = 0 assignment location.
 *
 */

#include "oldhat.h"

// clang-format off
static oldhat_store_t  *oldhat_store_new    (uint64_t);
static void             oldhat_store_delete (oldhat_store_t *);
static void            *oldhat_store_get    (oldhat_store_t *, oldhat_t *,
					      hatrack_hash_t *, bool *);
static void            *oldhat_store_put    (oldhat_store_t *, oldhat_t *,
					      hatrack_hash_t *, void *, bool *);
static void            *oldhat_store_replace(oldhat_store_t *, oldhat_t *,
					      hatrack_hash_t *, void *, bool *);
static bool             oldhat_store_add    (oldhat_store_t *, oldhat_t *,
					      hatrack_hash_t *, void *);
static void            *oldhat_store_remove (oldhat_store_t *, oldhat_t *,
					      hatrack_hash_t *, bool *);
static oldhat_store_t  *oldhat_store_migrate(oldhat_store_t *, oldhat_t *);
// clang-format on

void
oldhat_init(oldhat_t *self)
{
    oldhat_store_t *store = oldhat_store_new(HATRACK_MIN_SIZE);

    atomic_store(&self->store_current, store);
    atomic_store(&self->item_count, 0);
}

void *
oldhat_get(oldhat_t *self, hatrack_hash_t *hv, bool *found)
{
    void *ret;

    mmm_start_basic_op();
    ret = oldhat_store_get(self->store_current, self, hv, found);
    mmm_end_op();

    return ret;
}

void *
oldhat_put(oldhat_t *self, hatrack_hash_t *hv, void *item, bool *found)
{
    void *ret;

    mmm_start_basic_op();
    ret = oldhat_store_put(self->store_current, self, hv, item, found);
    mmm_end_op();

    return ret;
}

void *
oldhat_replace(oldhat_t *self, hatrack_hash_t *hv, void *item, bool *found)
{
    void *ret;

    mmm_start_basic_op();
    ret = oldhat_store_replace(self->store_current, self, hv, item, found);
    mmm_end_op();

    return ret;
}

bool
oldhat_add(oldhat_t *self, hatrack_hash_t *hv, void *item)
{
    bool ret;

    mmm_start_basic_op();
    ret = oldhat_store_add(self->store_current, self, hv, item);
    mmm_end_op();

    return ret;
}

void *
oldhat_remove(oldhat_t *self, hatrack_hash_t *hv, bool *found)
{
    void *ret;

    mmm_start_basic_op();
    ret = oldhat_store_remove(self->store_current, self, hv, found);
    mmm_end_op();

    return ret;
}

void
oldhat_delete(oldhat_t *self)
{
    oldhat_store_t *store;

    store = atomic_load(&self->store_current);

    mmm_retire(store);
    free(self);
}

uint64_t
oldhat_len(oldhat_t *self)
{
    return self->item_count;
}

hatrack_view_t *
oldhat_view(oldhat_t *self, uint64_t *num, bool sort)
{
    hatrack_view_t  *view;
    hatrack_view_t  *p;
    oldhat_record_t *record;
    uint64_t         i;
    uint64_t         num_items;
    uint64_t         alloc_len;
    oldhat_store_t  *store;

    mmm_start_basic_op();

    store     = atomic_read(&self->store_current);
    alloc_len = sizeof(hatrack_view_t) * (store->last_slot + 1);
    view      = (hatrack_view_t *)malloc(alloc_len);
    p         = view;

    for (i = 0; i <= store->last_slot; i++) {
        record = atomic_read(&store->buckets[i]);
        if (!record || !record->used) {
            continue;
        }
        p->hv         = record->hv;
        p->item       = record->item;
        p->sort_epoch = mmm_get_create_epoch(record);
        p++;
    }

    num_items = p - view;
    *num      = num_items;

    if (!num_items) {
        free(view);
        mmm_end_op();
        return NULL;
    }

    view = realloc(view, *num * sizeof(hatrack_view_t));

    if (sort) {
        qsort(view, num_items, sizeof(hatrack_view_t), hatrack_quicksort_cmp);
    }

    mmm_end_op();

    return view;
}

static oldhat_store_t *
oldhat_store_new(uint64_t size)
{
    uint64_t        alloc_len;
    oldhat_store_t *store;

    alloc_len = sizeof(oldhat_store_t);
    alloc_len += sizeof(oldhat_record_t *) * size;
    store            = (oldhat_store_t *)mmm_alloc_committed(alloc_len);
    store->last_slot = size - 1;
    store->threshold = hatrack_compute_table_threshold(size);

    mmm_add_cleanup_handler(store, (void (*)(void *))oldhat_store_delete);

    return store;
}

static void
oldhat_store_delete(oldhat_store_t *self)
{
    uint64_t         i;
    oldhat_record_t *record;

    for (i = 0; i <= self->last_slot; i++) {
        record = atomic_load(&self->buckets[i]);

        if (record) {
            mmm_retire(record);
        }
    }
}

static void *
oldhat_store_get(oldhat_store_t *self,
                 oldhat_t       *top,
                 hatrack_hash_t *hvp,
                 bool           *found)
{
    uint64_t         bix;
    uint64_t         i;
    oldhat_record_t *record;

    bix = hatrack_bucket_index(hvp, self->last_slot);

    for (i = 0; i <= self->last_slot; i++) {
        record = atomic_load(&self->buckets[bix]);
        if (!record) {
            break;
        }
        if (!hatrack_hashes_eq(hvp, &record->hv)) {
            bix = (bix + 1) & self->last_slot;
            continue;
        }
        if (!record->used) {
            break;
        }
        if (found) {
            *found = true;
        }
        return record->item;
    }
    // not found.
    if (found) {
        *found = false;
    }
    return NULL;
}

static void *
oldhat_store_put(oldhat_store_t *self,
                 oldhat_t       *top,
                 hatrack_hash_t *hvp,
                 void           *item,
                 bool           *found)
{
    oldhat_record_t *candidate;
    oldhat_record_t *record;
    uint64_t         bix;
    uint64_t         i;

    candidate       = mmm_alloc_committed(sizeof(oldhat_record_t));
    candidate->hv   = *hvp;
    candidate->item = item;
    candidate->used = true;

    bix = hatrack_bucket_index(hvp, self->last_slot);

    for (i = 0; i <= self->last_slot; i++) {
        record = atomic_load(&self->buckets[bix]);
        if (!record) {
            if (CAS(&self->buckets[bix], &record, candidate)) {
                if (found) {
                    *found = false;
                }
                atomic_fetch_add(&top->item_count, 1);
                if (atomic_fetch_add(&self->used_count, 1) >= self->threshold) {
                    oldhat_store_migrate(self, top);
                }
                return NULL;
            }
        }
        if (!hatrack_hashes_eq(hvp, &record->hv)) {
            bix = (bix + 1) & self->last_slot;
            continue;
        }
        goto found_bucket;
    }

migrate_and_try_again:
    mmm_retire_unused(candidate);
    self = oldhat_store_migrate(self, top);
    return oldhat_store_put(self, top, hvp, item, found);

found_bucket:
    if (record->moving) {
        goto migrate_and_try_again;
    }
    if (record->used) {
        mmm_copy_create_epoch(candidate, record);
    }
    if (CAS(&self->buckets[bix], &record, candidate)) {
        mmm_retire(record);
        if (!record->used) {
            if (found) {
                *found = false;
            }
            atomic_fetch_add(&top->item_count, 1);
            return NULL;
        }
        else {
            if (found) {
                *found = true;
            }
            return record->item;
        }
    }
    /* If we get here, the CAS failed. Either it's time to migrate, or
     * someone beat us to the punch, in which case we pretend we were
     * successful, but overwritten.
     */
    if (record->moving) {
        goto migrate_and_try_again;
    }
    mmm_retire_unused(candidate);
    return item;
}

static void *
oldhat_store_replace(oldhat_store_t *self,
                     oldhat_t       *top,
                     hatrack_hash_t *hvp,
                     void           *item,
                     bool           *found)
{
    oldhat_record_t *candidate;
    oldhat_record_t *record;
    uint64_t         bix;
    uint64_t         i;

    candidate       = mmm_alloc_committed(sizeof(oldhat_record_t));
    candidate->hv   = *hvp;
    candidate->item = item;
    candidate->used = true;

    bix = hatrack_bucket_index(hvp, self->last_slot);

    for (i = 0; i <= self->last_slot; i++) {
        record = atomic_load(&self->buckets[bix]);
        if (!record) {
            goto not_found;
        }
        if (!hatrack_hashes_eq(hvp, &record->hv)) {
            bix = (bix + 1) & self->last_slot;
            continue;
        }
        goto found_bucket;
    }

not_found:
    if (found) {
        *found = false;
    }
    mmm_retire_unused(candidate);
    return NULL;

migrate_and_try_again:
    mmm_retire_unused(candidate);
    self = oldhat_store_migrate(self, top);
    return oldhat_store_replace(self, top, hvp, item, found);

found_bucket:
    if (record->moving) {
        goto migrate_and_try_again;
    }
    if (!record->used) {
        goto not_found;
    }

    mmm_copy_create_epoch(candidate, record);

    if (CAS(&self->buckets[bix], &record, candidate)) {
        mmm_retire(record);
        if (found) {
            *found = true;
        }
        return record->item;
    }
    /* If we get here, the CAS failed. Either it's time to migrate, or
     * someone beat us to the punch, in which case we pretend we were
     * successful, but overwritten.
     */
    if (record->moving) {
        goto migrate_and_try_again;
    }
    mmm_retire_unused(candidate);
    return item;
}

static bool
oldhat_store_add(oldhat_store_t *self,
                 oldhat_t       *top,
                 hatrack_hash_t *hvp,
                 void           *item)
{
    oldhat_record_t *candidate;
    oldhat_record_t *record;
    uint64_t         bix;
    uint64_t         i;

    candidate       = mmm_alloc_committed(sizeof(oldhat_record_t));
    candidate->hv   = *hvp;
    candidate->item = item;
    candidate->used = true;

    bix = hatrack_bucket_index(hvp, self->last_slot);

    for (i = 0; i <= self->last_slot; i++) {
        record = atomic_load(&self->buckets[bix]);
        if (!record) {
            if (CAS(&self->buckets[bix], &record, candidate)) {
                if (atomic_fetch_add(&self->used_count, 1) >= self->threshold) {
                    oldhat_store_migrate(self, top);
                }
                return true;
            }
        }
        if (!hatrack_hashes_eq(hvp, &record->hv)) {
            bix = (bix + 1) & self->last_slot;
            continue;
        }
        goto found_bucket;
    }

migrate_and_try_again:
    mmm_retire_unused(candidate);
    self = oldhat_store_migrate(self, top);
    return oldhat_store_add(self, top, hvp, item);

found_bucket:
    if (record->moving) {
        goto migrate_and_try_again;
    }
    if (record->used) {
        mmm_retire_unused(candidate);
        return false;
    }
    if (CAS(&self->buckets[bix], &record, candidate)) {
        mmm_retire(record);
        atomic_fetch_add(&top->item_count, 1);
        return true;
    }
    /* If we get here, the CAS failed. Either it's time to migrate, or
     * someone beat us to the punch, in which case we return false.
     */
    if (record->moving) {
        goto migrate_and_try_again;
    }
    mmm_retire_unused(candidate);
    return false;
}

static void *
oldhat_store_remove(oldhat_store_t *self,
                    oldhat_t       *top,
                    hatrack_hash_t *hvp,
                    bool           *found)
{
    oldhat_record_t *candidate;
    oldhat_record_t *record;
    uint64_t         bix;
    uint64_t         i;

    candidate       = mmm_alloc_committed(sizeof(oldhat_record_t));
    candidate->hv   = *hvp;
    candidate->used = false;

    bix = hatrack_bucket_index(hvp, self->last_slot);

    for (i = 0; i <= self->last_slot; i++) {
        record = atomic_load(&self->buckets[bix]);
        if (!record) {
            goto not_found;
        }
        if (!hatrack_hashes_eq(hvp, &record->hv)) {
            bix = (bix + 1) & self->last_slot;
            continue;
        }
        goto found_bucket;
    }

not_found:
    mmm_retire_unused(candidate);
    if (found) {
        *found = false;
    }
    return NULL;

migrate_and_try_again:
    mmm_retire_unused(candidate);
    self = oldhat_store_migrate(self, top);
    return oldhat_store_remove(self, top, hvp, found);

found_bucket:
    if (record->moving) {
        goto migrate_and_try_again;
    }
    if (!record->used) {
        goto not_found;
    }
    if (CAS(&self->buckets[bix], &record, candidate)) {
        mmm_retire(record);
        if (found) {
            *found = true;
        }
        atomic_fetch_sub(&top->item_count, 1);
        return record->item;
    }
    /* If we get here, the CAS failed. Either it's time to migrate, or
     * someone beat us to the punch, in which case we fail.
     */
    if (record->moving) {
        goto migrate_and_try_again;
    }

    goto not_found;
}

static oldhat_store_t *
oldhat_store_migrate(oldhat_store_t *self, oldhat_t *top)
{
    oldhat_record_t *candidate_record;
    oldhat_store_t  *new_store;
    oldhat_store_t  *candidate_store;
    uint64_t         new_size;
    oldhat_record_t *record;
    uint64_t         i, j;
    uint64_t         bix;
    uint64_t         record_sz;
    uint64_t         new_used      = 0;
    uint64_t         expected_used = 0;

    /* Run through every bucket, and mark any bucket that
     * doesn't already know we're moving.  Note that the CAS could
     * fail due to some other updater, so we keep CASing until we know
     * it was successful.
     */
    record_sz        = sizeof(oldhat_record_t);
    candidate_record = (oldhat_record_t *)mmm_alloc_committed(record_sz);

    for (i = 0; i <= self->last_slot; i++) {
        record = atomic_read(&self->buckets[i]);
        do {
            if (!record) {
                candidate_record->hv.w1  = 0;
                candidate_record->hv.w2  = 0;
                candidate_record->item   = NULL;
                candidate_record->used   = false;
                candidate_record->moving = true;
                candidate_record->moved  = true;
            }
            else {
                if (record->moving) {
                    goto add_to_length;
                }
                candidate_record->hv     = record->hv;
                candidate_record->item   = record->item;
                candidate_record->used   = record->used;
                candidate_record->moving = true;
                candidate_record->moved  = false;
                if (candidate_record->used) {
                    mmm_copy_create_epoch(candidate_record, record);
                }
            }
        } while (!CAS(&self->buckets[i], &record, candidate_record));
        // If I'm here, I exited due to the CAS being successful,
        // which means I need to replenish my candidate_record, and
        // retire the old record, if any.
        if (record) {
            mmm_retire(record);
        }
        candidate_record = (oldhat_record_t *)mmm_alloc_committed(record_sz);

add_to_length:
        if (record && record->used) {
            new_used++;
        }
    }

    new_store = atomic_read(&self->store_next);

    // If we couldn't acquire a store, try to install one. If we fail, free it.
    if (!new_store) {
        new_size        = hatrack_new_size(self->last_slot, new_used);
        candidate_store = oldhat_store_new(new_size);
        // This helps address a potential race condition, where
        // someone could drain the table after resize, having
        // us swap in the wrong length.
        atomic_store(&candidate_store->used_count, 0);

        if (!CAS(&self->store_next, &new_store, candidate_store)) {
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
        record = atomic_read(&self->buckets[i]);

        if (record->moved) {
            goto next_migration;
        }
        bix = hatrack_bucket_index(&record->hv, new_store->last_slot);
        candidate_record->hv     = record->hv;
        candidate_record->item   = record->item;
        candidate_record->used   = true;
        candidate_record->moving = false;
        candidate_record->moved  = false;
        mmm_copy_create_epoch(candidate_record, record);

        for (j = 0; j <= new_store->last_slot; j++) {
            record = atomic_read(&new_store->buckets[bix]);
            if (!record) {
                if (CAS(&new_store->buckets[bix], &record, candidate_record)) {
                    candidate_record
                        = (oldhat_record_t *)mmm_alloc_committed(record_sz);
                    goto next_migration;
                }
            }
            if (!hatrack_hashes_eq(&record->hv, &candidate_record->hv)) {
                bix = (bix + 1) & new_store->last_slot;
                continue;
            }
            break; // Someone else got the job done.
        }
next_migration:
        continue;
    }

    for (i = 0; i <= self->last_slot; i++) {
        record = atomic_read(&self->buckets[i]);
        do {
            if (record->moved) {
                goto next_mark_finished;
            }
            candidate_record->hv     = record->hv;
            candidate_record->item   = record->item;
            candidate_record->used   = record->used;
            candidate_record->moving = true;
            candidate_record->moved  = true;
            if (candidate_record->used) {
                mmm_copy_create_epoch(candidate_record, record);
            }
        } while (!CAS(&self->buckets[i], &record, candidate_record));
        // If I'm here, I exited due to the CAS being successful,
        // which means I need to replenish my candidate_record, and
        // retire the old record, which there will definitely be, this
        // time.
        mmm_retire(record);
        candidate_record = (oldhat_record_t *)mmm_alloc_committed(record_sz);

next_mark_finished:
        continue;
    }

    mmm_retire_unused(candidate_record);

    CAS(&new_store->used_count, &expected_used, new_used);

    if (CAS(&top->store_current, &self, new_store)) {
        mmm_retire(self);
    }

    return top->store_current;
}
