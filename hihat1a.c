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
 *  Name:           hihat1a.c
 *  Description:    Half-Interesting HAsh Table.
 *
 *                  This attempts to do some waiting if a migration
 *                  seems to be in progress.  Early indication is that
 *                  this will never make more than a small (< 5%)
 *                  difference in performance, and can easily hurt if
 *                  you do not pick the right length of time to sleep.
 *
 *                  It seems to be better on larger tables right now.
 *                  But I've not done enough testing, by far.
 *
 *  Author:         John Viega, john@zork.org
 *
 */

#include "hihat1.h"

// clang-format off
static hihat1_store_t  *hihat1a_store_new         (uint64_t);
static void            *hihat1a_store_get         (hihat1_store_t *, hihat1_t *,
					          hatrack_hash_t *, bool *);
static void            *hihat1a_store_put         (hihat1_store_t *, hihat1_t *,
					          hatrack_hash_t *, void *,
						  bool *);
static bool             hihat1a_store_put_if_empty(hihat1_store_t *,
						  hihat1_t *,
						  hatrack_hash_t *,
						  void *);
static void            *hihat1a_store_remove      (hihat1_store_t *, hihat1_t *,
					          hatrack_hash_t *, bool *);
static hihat1_store_t *hihat1a_store_migrate      (hihat1_store_t *,
						   hihat1_t *);

void
hihat1a_init(hihat1_t *self)
{
    hihat1_store_t *store = hihat1a_store_new(HATRACK_MIN_SIZE);

    self->epoch = 0;
    atomic_store(&self->store_current, store);
}

void *
hihat1a_get(hihat1_t *self, hatrack_hash_t *hv, bool *found)
{
    void *ret;

    mmm_start_basic_op();
    ret = hihat1a_store_get(self->store_current, self, hv, found);
    mmm_end_op();

    return ret;
}

void *
hihat1a_put(hihat1_t *self, hatrack_hash_t *hv, void *item, bool *found)
{
    void *ret;

    mmm_start_basic_op();
    ret = hihat1a_store_put(self->store_current, self, hv, item, found);
    mmm_end_op();

    return ret;
}

bool
hihat1a_put_if_empty(hihat1_t *self, hatrack_hash_t *hv, void *item)
{
    bool ret;

    mmm_start_basic_op();
    ret = hihat1a_store_put_if_empty(self->store_current, self, hv, item);
    mmm_end_op();

    return ret;
}

void *
hihat1a_remove(hihat1_t *self, hatrack_hash_t *hv, bool *found)
{
    void *ret;

    mmm_start_basic_op();
    ret = hihat1a_store_remove(self->store_current, self, hv, found);
    mmm_end_op();

    return ret;
}

void
hihat1a_delete(hihat1_t *self)
{
    mmm_retire(atomic_load(&self->store_current));
    free(self);
}

uint64_t
hihat1a_len(hihat1_t *self)
{
    return self->store_current->used_count - self->store_current->del_count;
}

// This version cannot be linearized.
hatrack_view_t *
hihat1a_view(hihat1_t *self, uint64_t *num)
{
    hatrack_view_t  *view;
    hatrack_view_t  *p;
    hatrack_hash_t   hv;
    hihat1_bucket_t *cur;
    hihat1_bucket_t *end;
    hihat1_record_t  record;
    uint64_t         num_items;
    uint64_t         alloc_len;
    hihat1_store_t  *store;

    mmm_start_basic_op();

    store     = atomic_load(&self->store_current);
    alloc_len = sizeof(hatrack_view_t) * (store->last_slot + 1);
    view      = (hatrack_view_t *)malloc(alloc_len);
    p         = view;
    cur       = store->buckets;
    end       = cur + (store->last_slot + 1);

    while (cur < end) {
        hv            = atomic_load(&cur->hv);
        record        = atomic_load(&cur->record);
        p->hv         = hv;
        p->item       = record.item;
        p->sort_epoch = record.info & HIHAT_F_MASK;

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

    view = realloc(view, num_items * sizeof(hatrack_view_t));

    // Unordered buckets should be in random order, so quicksort is a
    // good option.
    qsort(view, num_items, sizeof(hatrack_view_t), hatrack_quicksort_cmp);

    mmm_end_op();
    return view;
}

static hihat1_store_t *
hihat1a_store_new(uint64_t size)
{
    hihat1_store_t *store;
    uint64_t        alloc_len;

    alloc_len = sizeof(hihat1_store_t) + sizeof(hihat1_bucket_t) * size;
    store     = (hihat1_store_t *)mmm_alloc_committed(alloc_len);

    store->last_slot  = size - 1;
    store->threshold  = hatrack_compute_table_threshold(size);
    store->used_count = ATOMIC_VAR_INIT(0);
    store->del_count  = ATOMIC_VAR_INIT(0);
    store->store_next = ATOMIC_VAR_INIT(NULL);

    return store;
}

static void *
hihat1a_store_get(hihat1_store_t *self,
                 hihat1_t       *top,
                 hatrack_hash_t *hv1,
                 bool           *found)
{
    uint64_t         bix = hatrack_bucket_index(hv1, self->last_slot);
    uint64_t         i;
    hatrack_hash_t   hv2;
    hihat1_bucket_t *bucket;
    hihat1_record_t  record;

    for (i = 0; i <= self->last_slot; i++) {
        bucket = &self->buckets[bix];
        hv2    = atomic_load(&bucket->hv);
        if (hatrack_bucket_unreserved(&hv2)) {
            goto not_found;
        }
        if (!hatrack_hashes_eq(hv1, &hv2)) {
            bix = (bix + 1) & self->last_slot;
            continue;
        }

        record = atomic_load(&bucket->record);
        if (record.info & HIHAT_F_USED) {
            if (found) {
                *found = true;
            }
            return record.item;
        }
        break;
    }
not_found:
    if (found) {
        *found = false;
    }
    return NULL;
}

static void *
hihat1a_store_put(hihat1_store_t *self,
                 hihat1_t       *top,
                 hatrack_hash_t *hv1,
                 void           *item,
                 bool           *found)
{
    void            *old_item;
    uint64_t         bix = hatrack_bucket_index(hv1, self->last_slot);
    uint64_t         i;
    hatrack_hash_t   hv2;
    hihat1_bucket_t *bucket;
    hihat1_record_t  record;
    hihat1_record_t  candidate;

    for (i = 0; i <= self->last_slot; i++) {
        bucket = &self->buckets[bix];
        hv2.w1 = 0;
        hv2.w2 = 0;
        if (!LCAS(&bucket->hv, &hv2, *hv1, HIHAT1_CTR_BUCKET_ACQUIRE)) {
            if (!hatrack_hashes_eq(hv1, &hv2)) {
                bix = (bix + 1) & self->last_slot;
                continue;
            }
        }
        else {
            if (atomic_fetch_add(&self->used_count, 1) >= self->threshold) {
		goto migrate_and_retry;
            }
        }
        goto found_bucket;
    }
 migrate_and_retry:
    self = hihat1a_store_migrate(self, top);
    return hihat1a_store_put(self, top, hv1, item, found);

found_bucket:
    record = atomic_load(&bucket->record);
    if (record.info & HIHAT_F_MOVING) {
	goto migrate_and_retry;
    }

    if (found) {
        if (record.info & HIHAT_F_USED) {
            *found = true;
        }
        else {
            *found = false;
        }
    }

    old_item       = record.item;
    candidate.item = item;
    candidate.info = top->epoch++ | HIHAT_F_USED;

    if (LCAS(&bucket->record, &record, candidate, HIHAT1_CTR_REC_INSTALL)) {
        if (record.info & HIHAT_F_RMD) {
            atomic_fetch_sub(&self->del_count, 1);
        }
        return old_item;
    }
    if (record.info & HIHAT_F_MOVING) {
	goto migrate_and_retry;
    }

    return item;
}

static bool
hihat1a_store_put_if_empty(hihat1_store_t *self,
                          hihat1_t       *top,
                          hatrack_hash_t *hv1,
                          void           *item)
{
    uint64_t         bix = hatrack_bucket_index(hv1, self->last_slot);
    uint64_t         i;
    hatrack_hash_t   hv2;
    hihat1_bucket_t *bucket;
    hihat1_record_t  record;
    hihat1_record_t  candidate;

    for (i = 0; i <= self->last_slot; i++) {
        bucket = &self->buckets[bix];
        hv2.w1 = 0;
        hv2.w2 = 0;
        if (!LCAS(&bucket->hv, &hv2, *hv1, HIHAT1_CTR_BUCKET_ACQUIRE)) {
            if (!hatrack_hashes_eq(hv1, &hv2)) {
                bix = (bix + 1) & self->last_slot;
                continue;
            }
        }
        else {
            if (atomic_fetch_add(&self->used_count, 1) >= self->threshold) {
                return hihat1a_store_put_if_empty(
                    hihat1a_store_migrate(self, top),
                    top,
                    hv1,
                    item);
            }
        }
        goto found_bucket;
    }

 migrate_and_retry:
    self = hihat1a_store_migrate(self, top);
    return hihat1a_store_put_if_empty(self, top, hv1, item);

found_bucket:
    record = atomic_load(&bucket->record);
    if (record.info & HIHAT_F_MOVING) {
	goto migrate_and_retry;
    }
    if (record.info & HIHAT_F_USED) {
        return false;
    }

    candidate.item = item;
    candidate.info = top->epoch++ | HIHAT_F_USED;

    if (LCAS(&bucket->record, &record, candidate, HIHAT1_CTR_REC_INSTALL)) {
        if (record.info & HIHAT_F_RMD) {
            atomic_fetch_sub(&self->del_count, 1);
        }
        return true;
    }
    if (record.info & HIHAT_F_MOVING) {
	goto migrate_and_retry;
    }

    return false;
}

static void *
hihat1a_store_remove(hihat1_store_t *self,
                    hihat1_t       *top,
                    hatrack_hash_t *hv1,
                    bool           *found)
{
    void            *old_item;
    uint64_t         bix = hatrack_bucket_index(hv1, self->last_slot);
    uint64_t         i;
    hatrack_hash_t   hv2;
    hihat1_bucket_t *bucket;
    hihat1_record_t  record;
    hihat1_record_t  candidate;

    for (i = 0; i <= self->last_slot; i++) {
        bucket = &self->buckets[bix];
        hv2    = atomic_load(&bucket->hv);
        if (hatrack_bucket_unreserved(&hv2)) {
            break;
        }
        if (!hatrack_hashes_eq(hv1, &hv2)) {
            bix = (bix + 1) & self->last_slot;
            continue;
        }

        goto found_bucket;
    }

    if (found) {
        *found = false;
    }

    return NULL;

found_bucket:
    record = atomic_load(&bucket->record);
    if (record.info & HIHAT_F_MOVING) {
    migrate_and_retry:
	self = hihat1a_store_migrate(self, top);
        return hihat1a_store_remove(self, top, hv1, found);
    }
    if (!(record.info & HIHAT_F_USED)) {
        if (found) {
            *found = false;
        }

        return NULL;
    }

    old_item       = record.item;
    candidate.item = NULL;
    candidate.info = HIHAT_F_RMD;

    if (LCAS(&bucket->record, &record, candidate, HIHAT1_CTR_DEL)) {
        atomic_fetch_add(&self->del_count, 1);

        if (found) {
            *found = true;
        }
        return old_item;
    }
    if (record.info & HIHAT_F_MOVING) {
	goto migrate_and_retry;
    }

    if (found) {
        *found = false;
    }
    return NULL;
}

const static struct timespec sleep_time = {
    .tv_sec  = 0,
    .tv_nsec = HIHAT1a_MIGRATE_SLEEP_TIME_NS
};

static hihat1_store_t *
hihat1a_store_migrate(hihat1_store_t *self, hihat1_t *top)
{
    hihat1_store_t  *new_store;
    hihat1_store_t  *candidate_store;
    uint64_t         new_size;
    hihat1_bucket_t *bucket;
    hihat1_bucket_t *new_bucket;
    hihat1_record_t  record;
    hihat1_record_t  candidate_record;
    hihat1_record_t  expected_record;
    hatrack_hash_t   expected_hv;
    hatrack_hash_t   hv;
    uint64_t         i, j;
    uint64_t         bix;
    uint64_t         new_used      = 0;
    uint64_t         expected_used = ~0;
 

    new_store = atomic_load(&self->store_next);
    
    if (new_store) {
	// Try twice to let whoever is in front of us to complete
	// the migration.

	nanosleep(&sleep_time, NULL);
	new_store = atomic_load(&self->store_next);
	if (new_store == atomic_load(&top->store_current)) {
	    HATRACK_CTR(HATRACK_CTR_HI2_SLEEP1a_WORKED);
	    return new_store;
	}
	HATRACK_CTR(HATRACK_CTR_HI2_SLEEP1a_FAILED);
	nanosleep(&sleep_time, NULL);
	new_store = atomic_load(&self->store_next);
	if (new_store == atomic_load(&top->store_current)) {
	    HATRACK_CTR(HATRACK_CTR_HI2_SLEEP1b_WORKED);    
	    return new_store;
	}
	HATRACK_CTR(HATRACK_CTR_HI2_SLEEP1b_FAILED);
		
	goto have_new_store;
    }
				    
    /* Quickly run through every history bucket, and mark any bucket
     * that doesn't already have F_MOVING set.  Note that the CAS
     * could fail due to some other updater, so we keep CASing until
     * we know it was successful.
     */
    for (i = 0; i <= self->last_slot; i++) {
        bucket                = &self->buckets[i];
        record                = atomic_load(&bucket->record);
        candidate_record.info = record.info | HIHAT_F_MOVING;
        candidate_record.item = record.item;

        do {
            if (record.info & HIHAT_F_MOVING) {
                break;
            }
        } while (!LCAS(&bucket->record,
                       &record,
                       candidate_record,
                       HIHAT1_CTR_F_MOVING));

        if (record.info & HIHAT_F_USED) {
            new_used++;
        }
    }

    new_store = atomic_load(&self->store_next);

    // If we couldn't acquire a store, try to install one. If we fail, free it.
    if (!new_store) {
        new_size        = hatrack_new_size(self->last_slot, new_used);
        candidate_store = hihat1a_store_new(new_size);
        // This helps address a potential race condition, where
        // someone could drain the table after resize, having
        // us swap in the wrong length.
        atomic_store(&candidate_store->used_count, ~0);

        if (!LCAS(&self->store_next,
                  &new_store,
                  candidate_store,
                  HIHAT1_CTR_NEW_STORE)) {
            mmm_retire_unused(candidate_store);
	    // If we lost the CAS, give the other thread time to
	    // migrate.
	    nanosleep(&sleep_time, NULL);
	    new_store = atomic_load(&top->store_current);
	    if (new_store != self) {
		HATRACK_CTR(HATRACK_CTR_HI2_SLEEP2a_WORKED);		
		return new_store;
	    }
	    HATRACK_CTR(HATRACK_CTR_HI2_SLEEP2a_FAILED);		    
	    nanosleep(&sleep_time, NULL);
	    new_store = atomic_load(&top->store_current);
	    if (new_store != self) {
		HATRACK_CTR(HATRACK_CTR_HI2_SLEEP2b_WORKED);		
		return new_store;
	    }
		HATRACK_CTR(HATRACK_CTR_HI2_SLEEP2b_FAILED);	    
        }
        else {
            new_store = candidate_store;
        }
    }
    
 have_new_store:
    // At this point, we're sure that any late writers will help us
    // with the migration. Therefore, we can go through each item,
    // and, if it's not fully migrated, we can attempt to migrate it.
    for (i = 0; i <= self->last_slot; i++) {
        bucket = &self->buckets[i];
        record = atomic_load(&bucket->record);

        if (record.info & HIHAT_F_MOVED) {
            continue;
        }

        // If the bucket has been rm'd, or has never been used...
        if ((record.info & HIHAT_F_RMD) || !(record.info & HIHAT_F_USED)) {
            candidate_record.info = record.info | HIHAT_F_MOVED;
            candidate_record.item = record.item;
            LCAS(&bucket->record,
                 &record,
                 candidate_record,
                 HIHAT1_CTR_F_MOVED1);
            continue;
        }

        hv  = atomic_load(&bucket->hv);
        bix = hatrack_bucket_index(&hv, new_store->last_slot);

        for (j = 0; j <= new_store->last_slot; j++) {
            new_bucket     = &new_store->buckets[bix];
            expected_hv.w1 = 0;
            expected_hv.w2 = 0;
            if (!LCAS(&new_bucket->hv,
                      &expected_hv,
                      hv,
                      HIHAT1_CTR_MIGRATE_HV)) {
                if (!hatrack_hashes_eq(&expected_hv, &hv)) {
                    bix = (bix + 1) & new_store->last_slot;
                    continue;
                }
            }
            break;
        }

        candidate_record.info = record.info & HIHAT_F_MASK;
        candidate_record.item = record.item;
        expected_record.info  = 0;
        expected_record.item  = NULL;

        LCAS(&new_bucket->record,
             &expected_record,
             candidate_record,
             HIHAT1_CTR_MIG_REC);
        candidate_record.info = record.info | HIHAT_F_MOVED;
        LCAS(&bucket->record, &record, candidate_record, HIHAT1_CTR_F_MOVED2);
    }

    LCAS(&new_store->used_count,
         &expected_used,
         new_used,
         HIHAT1_CTR_LEN_INSTALL);

    if (LCAS(&top->store_current, &self, new_store, HIHAT1_CTR_STORE_INSTALL)) {
        mmm_retire(self);
    }

    return new_store;
}
