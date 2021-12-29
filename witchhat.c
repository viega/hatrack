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
 *  Name:           witchhat.h
 *  Description:    Waiting I Trully Cannot Handle
 *
 *                  This is a lock-free, and wait freehash table,
 *                  without consistency / full ordering.
 *
 *  Author:         John Viega, john@zork.org
 *
 */

#include "witchhat.h"

// clang-format off
static witchhat_store_t  *witchhat_store_new    (uint64_t);
static void              *witchhat_store_get    (witchhat_store_t *,
						 witchhat_t *,
						 hatrack_hash_t *, bool *);
static void              *witchhat_store_put    (witchhat_store_t *,
						 witchhat_t *,
						 hatrack_hash_t *, 
						 void *, bool *, uint64_t);
static void              *witchhat_store_replace(witchhat_store_t *,
						 witchhat_t *,
						 hatrack_hash_t *, 
						 void *, bool *, uint64_t);
static bool               witchhat_store_add    (witchhat_store_t *,
						 witchhat_t *,
						 hatrack_hash_t *,
						 void *, uint64_t);
static void              *witchhat_store_remove (witchhat_store_t *,
						 witchhat_t *,
						 hatrack_hash_t *,
						 bool *, uint64_t);
static witchhat_store_t  *witchhat_store_migrate(witchhat_store_t *,
						 witchhat_t *);
static inline bool        witchhat_help_required(uint64_t);
static inline bool        witchhat_need_to_help (witchhat_t *);

void
witchhat_init(witchhat_t *self)
{
    witchhat_store_t *store = witchhat_store_new(HATRACK_MIN_SIZE);

    self->epoch = 0;
    atomic_store(&self->help_needed, 0);
    atomic_store(&self->store_current, store);
}

void *
witchhat_get(witchhat_t *self, hatrack_hash_t *hv, bool *found)
{
    void *ret;
    witchhat_store_t *store;
    
    mmm_start_basic_op();
    store = atomic_read(&self->store_current);
    ret = witchhat_store_get(store, self, hv, found);
    mmm_end_op();

    return ret;
}

void *
witchhat_put(witchhat_t *self, hatrack_hash_t *hv, void *item, bool *found)
{
    void *ret;
    witchhat_store_t *store;    

    mmm_start_basic_op();
    store = atomic_read(&self->store_current);
    ret   = witchhat_store_put(store, self, hv, item, found, 0);
    mmm_end_op();

    return ret;
}

void *
witchhat_replace(witchhat_t *self, hatrack_hash_t *hv, void *item, bool *found)
{
    void *ret;
    witchhat_store_t *store;    

    mmm_start_basic_op();
    store = atomic_read(&self->store_current);
    ret   = witchhat_store_replace(store, self, hv, item, found, 0);
    mmm_end_op();

    return ret;
}

bool
witchhat_add(witchhat_t *self, hatrack_hash_t *hv, void *item)
{
    bool              ret;
    witchhat_store_t *store;        

    mmm_start_basic_op();
    store = atomic_read(&self->store_current);
    ret   = witchhat_store_add(store, self, hv, item, 0);
    mmm_end_op();

    return ret;
}

void *
witchhat_remove(witchhat_t *self, hatrack_hash_t *hv, bool *found)
{
    void             *ret;
    witchhat_store_t *store;        

    mmm_start_basic_op();
    store = atomic_read(&self->store_current);
    ret   = witchhat_store_remove(store, self, hv, found, 0);
    mmm_end_op();

    return ret;
}

void
witchhat_delete(witchhat_t *self)
{
    mmm_retire(atomic_load(&self->store_current));
    free(self);
}

uint64_t
witchhat_len(witchhat_t *self)
{
    return self->store_current->used_count - self->store_current->del_count;
}

// This version cannot be linearized.
hatrack_view_t *
witchhat_view(witchhat_t *self, uint64_t *num, bool sort)
{
    hatrack_view_t  *view;
    hatrack_view_t  *p;
    hatrack_hash_t   hv;
    witchhat_bucket_t *cur;
    witchhat_bucket_t *end;
    witchhat_record_t  record;
    uint64_t         num_items;
    uint64_t         alloc_len;
    witchhat_store_t  *store;

    mmm_start_basic_op();

    store     = atomic_read(&self->store_current);
    alloc_len = sizeof(hatrack_view_t) * (store->last_slot + 1);
    view      = (hatrack_view_t *)malloc(alloc_len);
    p         = view;
    cur       = store->buckets;
    end       = cur + (store->last_slot + 1);

    while (cur < end) {
        hv            = atomic_read(&cur->hv);
        record        = atomic_read(&cur->record);
        p->hv         = hv;
        p->item       = record.item;
        p->sort_epoch = record.info & WITCHHAT_F_MASK;

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

    if (sort) {
	// Unordered buckets should be in random order, so quicksort
	// is a good option.
	qsort(view, num_items, sizeof(hatrack_view_t), hatrack_quicksort_cmp);
    }

    mmm_end_op();
    return view;
}

static witchhat_store_t *
witchhat_store_new(uint64_t size)
{
    witchhat_store_t *store;
    uint64_t        alloc_len;

    alloc_len = sizeof(witchhat_store_t) + sizeof(witchhat_bucket_t) * size;
    store     = (witchhat_store_t *)mmm_alloc_committed(alloc_len);

    store->last_slot  = size - 1;
    store->threshold  = hatrack_compute_table_threshold(size);
    store->used_count = ATOMIC_VAR_INIT(0);
    store->del_count  = ATOMIC_VAR_INIT(0);
    store->store_next = ATOMIC_VAR_INIT(NULL);

    return store;
}

static void *
witchhat_store_get(witchhat_store_t *self,
		   witchhat_t       *top,
		   hatrack_hash_t   *hv1,
		   bool             *found)
{
    uint64_t         bix;
    uint64_t         i;
    hatrack_hash_t   hv2;
    witchhat_bucket_t *bucket;
    witchhat_record_t  record;

    bix = hatrack_bucket_index(hv1, self->last_slot);
    
    for (i = 0; i <= self->last_slot; i++) {
        bucket = &self->buckets[bix];
        hv2    = atomic_read(&bucket->hv);
        if (hatrack_bucket_unreserved(&hv2)) {
            goto not_found;
        }
        if (!hatrack_hashes_eq(hv1, &hv2)) {
            bix = (bix + 1) & self->last_slot;
            continue;
        }

        record = atomic_read(&bucket->record);
        if (record.info & WITCHHAT_F_USED) {
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
witchhat_store_put(witchhat_store_t *self,
		   witchhat_t       *top,
		   hatrack_hash_t   *hv1,
		   void             *item,
		   bool             *found,
		   uint64_t          count)
{
    void              *old_item;
    uint64_t           bix;
    uint64_t           i;
    hatrack_hash_t     hv2;
    witchhat_bucket_t *bucket;
    witchhat_record_t  record;
    witchhat_record_t  candidate;

    bix  = hatrack_bucket_index(hv1, self->last_slot);
    
    for (i = 0; i <= self->last_slot; i++) {
        bucket = &self->buckets[bix];
	hv2    = atomic_read(&bucket->hv);
	if (hatrack_bucket_unreserved(&hv2)) {
	    if (LCAS(&bucket->hv, &hv2, *hv1, WITCHHAT_CTR_BUCKET_ACQUIRE)) {
		if (atomic_fetch_add(&self->used_count, 1) >= self->threshold) {
		    goto migrate_and_retry;
		}
		goto found_bucket;
	    }
	}
	if (!hatrack_hashes_eq(hv1, &hv2)) {
	    bix = (bix + 1) & self->last_slot;
	    continue;
	}
        goto found_bucket;
    }

 migrate_and_retry:
    count = count + 1;
    if (witchhat_help_required(count)) {
	HATRACK_CTR(HATRACK_CTR_WH_HELP_REQUESTS);
	atomic_fetch_add(&top->help_needed, 1);
	self     = witchhat_store_migrate(self, top);
	old_item = witchhat_store_put(self, top, hv1, item, found, count);
	atomic_fetch_sub(&top->help_needed, 1);
	return old_item;
    }
    self = witchhat_store_migrate(self, top);
    return witchhat_store_put(self, top, hv1, item, found, count);

found_bucket:
    record = atomic_read(&bucket->record);
    
    if (record.info & WITCHHAT_F_MOVING) {
	goto migrate_and_retry;
    }

    if (found) {
        if (record.info & WITCHHAT_F_USED) {
            *found = true;
        }
        else {
            *found = false;
        }
    }

    old_item       = record.item;
    candidate.item = item;
    candidate.info = top->epoch++ | WITCHHAT_F_USED;

    if (LCAS(&bucket->record, &record, candidate, WITCHHAT_CTR_REC_INSTALL)) {
        if (record.info & WITCHHAT_F_RMD) {
            atomic_fetch_sub(&self->del_count, 1);
        }
        return old_item;
    }

    /* If the CAS failed, there are two possible reasons:
     *
     * 1) Another thread beat us to updating the bucket, in which case
     *    we can consider our update "successful", as if it happened
     *    first, and then got overwritten.  In that case, we return
     *    the item we were going to insert, for the sake of memory
     *    management.
     *
     * 2) When we checked, no migration was in progress, but when we
     *    tried to update the record, there was one, in which case we
     *    need to help.
     */
    if (record.info & WITCHHAT_F_MOVING) {
	goto migrate_and_retry;
    }

    return item;
}

static void *
witchhat_store_replace(witchhat_store_t *self,
		       witchhat_t       *top,
		       hatrack_hash_t   *hv1,
		       void             *item,
		       bool             *found,
		       uint64_t          count)
{
    void              *old_item;
    uint64_t           bix;
    uint64_t           i;
    hatrack_hash_t     hv2;
    witchhat_bucket_t *bucket;
    witchhat_record_t  record;
    witchhat_record_t  candidate;
    
    bix  = hatrack_bucket_index(hv1, self->last_slot);
    
    for (i = 0; i <= self->last_slot; i++) {
        bucket = &self->buckets[bix];
	hv2    = atomic_read(&bucket->hv);
	if (hatrack_bucket_unreserved(&hv2)) {
	    goto not_found;
	}
	if (!hatrack_hashes_eq(hv1, &hv2)) {
	    bix = (bix + 1) & self->last_slot;
	    continue;
	}
        goto found_bucket;
    }

 not_found:
    if (found) {
	*found = false;
    }

    return NULL;

found_bucket:
    record = atomic_read(&bucket->record);
    
    if (record.info & WITCHHAT_F_MOVING) {
    migrate_and_retry:
	count = count + 1;
	if (witchhat_help_required(count)) {
	    HATRACK_CTR(HATRACK_CTR_WH_HELP_REQUESTS);
	    atomic_fetch_add(&top->help_needed, 1);
	    self     = witchhat_store_migrate(self, top);
	    old_item =
		witchhat_store_replace(self, top, hv1, item, found, count);
	    
	    atomic_fetch_sub(&top->help_needed, 1);
	    return old_item;
	}
	self = witchhat_store_migrate(self, top);
	return witchhat_store_replace(self, top, hv1, item, found, count);
    }

    if (!(record.info & WITCHHAT_F_USED)) {
	goto not_found;
    }

    if (record.info & WITCHHAT_F_RMD) {
	goto not_found;
    }


    old_item       = record.item;
    candidate.item = item;
    candidate.info = top->epoch++ | WITCHHAT_F_USED;

    /* If the CAS failed, there are two possible reasons:
     *
     * 1) When we checked, no migration was in progress, but when we
     *    tried to update the record, there was one, in which case we
     *    need to help.
     *
     * 2) Another thread beat us to updating the bucket, in which case
     *    we will consider our update "successful", as if it happened
     *    first, and then got overwritten.  In that case, we return
     *    the item we were going to insert, for the sake of memory
     *    management. For purposes of ordering, we are claiming that
     *    we successfully overwrote the old record before the item
     *    that won the CAS race. But we're unable to return the item,
     *    because the thing that came along and knocked us out, didn't
     *    free out memory.
     */
    
    if (!LCAS(&bucket->record, &record, candidate, WITCHHAT_CTR_REC_INSTALL)) {
	if (record.info & WITCHHAT_F_MOVING) {
	    goto migrate_and_retry;
	}
	goto not_found;
    }
    if (found) {
	*found = true;
    }
    return old_item;
}

static bool
witchhat_store_add(witchhat_store_t *self,
			    witchhat_t       *top,
			    hatrack_hash_t   *hv1,
			    void             *item,
			    uint64_t          count)
{
    uint64_t           bix;
    uint64_t           i;
    hatrack_hash_t     hv2;
    witchhat_bucket_t *bucket;
    witchhat_record_t  record;
    witchhat_record_t  candidate;

    bix = hatrack_bucket_index(hv1, self->last_slot);

    for (i = 0; i <= self->last_slot; i++) {
        bucket = &self->buckets[bix];
	hv2    = atomic_read(&bucket->hv);
	if (hatrack_bucket_unreserved(&hv2)) {
	    if (LCAS(&bucket->hv, &hv2, *hv1, WITCHHAT_CTR_BUCKET_ACQUIRE)) {
		if (atomic_fetch_add(&self->used_count, 1) >= self->threshold) {
		    goto migrate_and_retry;
		}
		goto found_bucket;
	    }
	}
	if (!hatrack_hashes_eq(hv1, &hv2)) {
	    bix = (bix + 1) & self->last_slot;
	    continue;
	}
	
        goto found_bucket;
    }

 migrate_and_retry:
    count = count + 1;
    if (witchhat_help_required(count)) {
	bool ret;

	HATRACK_CTR(HATRACK_CTR_WH_HELP_REQUESTS);	
	atomic_fetch_add(&top->help_needed, 1);
	self = witchhat_store_migrate(self, top);
	ret  = witchhat_store_add(self, top, hv1, item, count);
	atomic_fetch_sub(&top->help_needed, 1);

	return ret;
    }
    self = witchhat_store_migrate(self, top);
    return witchhat_store_add(self, top, hv1, item, count);

found_bucket:
    record = atomic_read(&bucket->record);
    if (record.info & WITCHHAT_F_MOVING) {
	goto migrate_and_retry;
    }
    if (record.info & WITCHHAT_F_USED) {
        return false;
    }

    candidate.item = item;
    candidate.info = top->epoch++ | WITCHHAT_F_USED;

    if (LCAS(&bucket->record, &record, candidate, WITCHHAT_CTR_REC_INSTALL)) {
        if (record.info & WITCHHAT_F_RMD) {
            atomic_fetch_sub(&self->del_count, 1);
        }
        return true;
    }
    
    if (record.info & WITCHHAT_F_MOVING) {
	goto migrate_and_retry;
    }

    return false;
}

static void *
witchhat_store_remove(witchhat_store_t *self,
                    witchhat_t         *top,
                    hatrack_hash_t     *hv1,
		      bool             *found,
		      uint64_t          count)
{
    void              *old_item;
    uint64_t           bix;
    uint64_t           i;
    hatrack_hash_t     hv2;
    witchhat_bucket_t *bucket;
    witchhat_record_t  record;
    witchhat_record_t  candidate;

    bix = hatrack_bucket_index(hv1, self->last_slot);
    
    for (i = 0; i <= self->last_slot; i++) {
        bucket = &self->buckets[bix];
        hv2    = atomic_read(&bucket->hv);
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
    record = atomic_read(&bucket->record);
    if (record.info & WITCHHAT_F_MOVING) {
    migrate_and_retry:
	count = count + 1;
	if (witchhat_help_required(count)) {
	    HATRACK_CTR(HATRACK_CTR_WH_HELP_REQUESTS);
	    atomic_fetch_add(&top->help_needed, 1);
	    self     = witchhat_store_migrate(self, top);
	    old_item = witchhat_store_remove(self, top, hv1, found, count);
	    atomic_fetch_sub(&top->help_needed, 1);
	    return old_item;
	}
	self = witchhat_store_migrate(self, top);
	return witchhat_store_remove(self, top, hv1, found, count);
    }
    if (!(record.info & WITCHHAT_F_USED)) {
        if (found) {
            *found = false;
        }

        return NULL;
    }

    old_item       = record.item;
    candidate.item = NULL;
    candidate.info = WITCHHAT_F_RMD;

    if (LCAS(&bucket->record, &record, candidate, WITCHHAT_CTR_DEL)) {
        atomic_fetch_add(&self->del_count, 1);

        if (found) {
            *found = true;
        }
        return old_item;
    }

    if (record.info & WITCHHAT_F_MOVING) {
	goto migrate_and_retry;
    }

    if (found) {
        *found = false;
    }
    return NULL;
}

static witchhat_store_t *
witchhat_store_migrate(witchhat_store_t *self, witchhat_t *top)
{
    witchhat_store_t  *new_store;
    witchhat_store_t  *candidate_store;
    uint64_t         new_size;
    witchhat_bucket_t *bucket;
    witchhat_bucket_t *new_bucket;
    witchhat_record_t  record;
    witchhat_record_t  candidate_record;
    witchhat_record_t  expected_record;
    hatrack_hash_t   expected_hv;
    hatrack_hash_t   hv;
    uint64_t         i, j;
    uint64_t         bix;
    uint64_t         new_used      = 0;
    uint64_t         expected_used = ~0;

    /* Quickly run through every history bucket, and mark any bucket
     * that doesn't already have F_MOVING set.  Note that the CAS
     * could fail due to some other updater, so we keep CASing until
     * we know it was successful.
     */
    for (i = 0; i <= self->last_slot; i++) {
        bucket                = &self->buckets[i];
        record                = atomic_read(&bucket->record);
        candidate_record.info = record.info | WITCHHAT_F_MOVING;
        candidate_record.item = record.item;

        do {
            if (record.info & WITCHHAT_F_MOVING) {
                break;
            }
        } while (!LCAS(&bucket->record,
                       &record,
                       candidate_record,
                       WITCHHAT_CTR_F_MOVING));

        if (record.info & WITCHHAT_F_USED) {
            new_used++;
        }
    }

    new_store = atomic_read(&self->store_next);

    // If we couldn't acquire a store, try to install one. If we fail, free it.
    if (!new_store) {
	// Different threads might end up producing different store sizes,
	// if the value of top->help_needed changes. It's ultimately
	// irrelevent; either store will be big enough to handle the migration.
	if (witchhat_need_to_help(top)) {
	    new_size = (self->last_slot + 1) << 1;
	} else {
	    new_size = hatrack_new_size(self->last_slot, new_used);
	}
        candidate_store = witchhat_store_new(new_size);
        // This helps address a potential race condition, where
        // someone could drain the table after resize, having
        // us swap in the wrong length.
        atomic_store(&candidate_store->used_count, ~0);

        if (!LCAS(&self->store_next,
                  &new_store,
                  candidate_store,
                  WITCHHAT_CTR_NEW_STORE)) {
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
        bucket = &self->buckets[i];
        record = atomic_read(&bucket->record);

        if (record.info & WITCHHAT_F_MOVED) {
            continue;
        }

        // If the bucket has been rm'd, or has never been used...
        if ((record.info & WITCHHAT_F_RMD) ||
	    !(record.info & WITCHHAT_F_USED)) {
            candidate_record.info = record.info | WITCHHAT_F_MOVED;
            candidate_record.item = record.item;
            LCAS(&bucket->record,
                 &record,
                 candidate_record,
                 WITCHHAT_CTR_F_MOVED1);
            continue;
        }

        hv  = atomic_read(&bucket->hv);
        bix = hatrack_bucket_index(&hv, new_store->last_slot);

        for (j = 0; j <= new_store->last_slot; j++) {
            new_bucket     = &new_store->buckets[bix];
            expected_hv.w1 = 0;
            expected_hv.w2 = 0;
            if (!LCAS(&new_bucket->hv,
                      &expected_hv,
                      hv,
                      WITCHHAT_CTR_MIGRATE_HV)) {
                if (!hatrack_hashes_eq(&expected_hv, &hv)) {
                    bix = (bix + 1) & new_store->last_slot;
                    continue;
                }
            }
            break;
        }

        candidate_record.info = record.info & WITCHHAT_F_MASK;
        candidate_record.item = record.item;
        expected_record.info  = 0;
        expected_record.item  = NULL;

        LCAS(&new_bucket->record,
             &expected_record,
             candidate_record,
             WITCHHAT_CTR_MIG_REC);
        candidate_record.info = record.info | WITCHHAT_F_MOVED;
        LCAS(&bucket->record, &record, candidate_record, WITCHHAT_CTR_F_MOVED2);
    }

    LCAS(&new_store->used_count,
         &expected_used,
         new_used,
         WITCHHAT_CTR_LEN_INSTALL);

    if (LCAS(&top->store_current,
	     &self,
	     new_store,
	     WITCHHAT_CTR_STORE_INSTALL)) {
        mmm_retire(self);
    }

    return new_store;
}

static inline bool
witchhat_help_required(uint64_t count)
{
    if (count == HATRACK_RETRY_THRESHOLD) {
	return true;
    }
    return false;
}


static inline bool
witchhat_need_to_help(witchhat_t *self) {
    return (bool)atomic_read(&self->help_needed);
}
