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
 *                  It seems to show the best results on larger tables
 *                  right now.  But I've not done enough testing, by
 *                  far.
 *
 *                  NOTE THAT EVERYTHING EXCEPT hihat1a_store_migrate()
 *                  IS THE SAME AS IN THE MAIN HIHAT1, SO WE ONLY
 *                  COMMENT IN THAT FUNCTION. PLEASE SEE hihat1.c FOR
 *                  COMMENTS ON THE REST OF THE CODE.
 *
 *
 *  Author:         John Viega, john@zork.org
 *
 */

#include "hihat1.h"

// clang-format off
static hihat1_store_t  *hihat1a_store_new    (uint64_t);
static void            *hihat1a_store_get    (hihat1_store_t *, hihat1_t *,
					      hatrack_hash_t *, bool *);
static void            *hihat1a_store_put    (hihat1_store_t *, hihat1_t *,
					      hatrack_hash_t *, void *, bool *);
static void            *hihat1a_store_replace(hihat1_store_t *, hihat1_t *,
					      hatrack_hash_t *, void *, bool *);
static bool             hihat1a_store_add    (hihat1_store_t *, hihat1_t *,
					      hatrack_hash_t *, void *);
static void            *hihat1a_store_remove (hihat1_store_t *, hihat1_t *,
					      hatrack_hash_t *, bool *);
static hihat1_store_t *hihat1a_store_migrate (hihat1_store_t *, hihat1_t *);

void
hihat1a_init(hihat1_t *self)
{
    hihat1_store_t *store;

    store            = hihat1a_store_new(HATRACK_MIN_SIZE);
    self->next_epoch = 1; 
    atomic_store(&self->store_current, store);
}

void *
hihat1a_get(hihat1_t *self, hatrack_hash_t *hv, bool *found)
{
    void           *ret;
    hihat1_store_t *store;

    mmm_start_basic_op();
    store = atomic_read(&self->store_current);
    ret   = hihat1a_store_get(store, self, hv, found);
    mmm_end_op();

    return ret;
}

void *
hihat1a_put(hihat1_t *self, hatrack_hash_t *hv, void *item, bool *found)
{
    void           *ret;
    hihat1_store_t *store;

    mmm_start_basic_op();
    store = atomic_read(&self->store_current);
    ret   = hihat1a_store_put(store, self, hv, item, found);
    mmm_end_op();

    return ret;
}

void *
hihat1a_replace(hihat1_t *self, hatrack_hash_t *hv, void *item, bool *found)
{
    void           *ret;
    hihat1_store_t *store;

    mmm_start_basic_op();
    store = atomic_read(&self->store_current);
    ret   = hihat1a_store_replace(store, self, hv, item, found);
    mmm_end_op();

    return ret;
}

bool
hihat1a_add(hihat1_t *self, hatrack_hash_t *hv, void *item)
{
    bool            ret;
    hihat1_store_t *store;

    mmm_start_basic_op();
    store = atomic_read(&self->store_current);    
    ret   = hihat1a_store_add(store, self, hv, item);
    mmm_end_op();

    return ret;
}

void *
hihat1a_remove(hihat1_t *self, hatrack_hash_t *hv, bool *found)
{
    void           *ret;
    hihat1_store_t *store;

    mmm_start_basic_op();
    store = atomic_read(&self->store_current);    
    ret   = hihat1a_store_remove(store, self, hv, found);
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
    return self->store_current->item_count;
}

hatrack_view_t *
hihat1a_view(hihat1_t *self, uint64_t *num, bool sort)
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

    store     = atomic_read(&self->store_current);
    alloc_len = sizeof(hatrack_view_t) * (store->last_slot + 1);
    view      = (hatrack_view_t *)malloc(alloc_len);
    p         = view;
    cur       = store->buckets;
    end       = cur + (store->last_slot + 1);

    while (cur < end) {
        record        = atomic_read(&cur->record);
        p->sort_epoch = record.info & HIHAT_EPOCH_MASK;

	if (!p->sort_epoch) {
	    cur++;
	    continue;
	}
	
        hv            = atomic_read(&cur->hv);
        p->hv         = hv;
        p->item       = record.item;
	
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
	qsort(view, num_items, sizeof(hatrack_view_t), hatrack_quicksort_cmp);
    }

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

    return store;
}

static void *
hihat1a_store_get(hihat1_store_t *self,
                 hihat1_t       *top,
                 hatrack_hash_t *hv1,
                 bool           *found)
{
    uint64_t         bix;
    uint64_t         i;
    hatrack_hash_t   hv2;
    hihat1_bucket_t *bucket;
    hihat1_record_t  record;

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
        if (record.info & HIHAT_EPOCH_MASK) {
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
    bool             new_item;
    uint64_t         bix;
    uint64_t         i;
    hatrack_hash_t   hv2;
    hihat1_bucket_t *bucket;
    hihat1_record_t  record;
    hihat1_record_t  candidate;

    bix = hatrack_bucket_index(hv1, self->last_slot);
    
    for (i = 0; i <= self->last_slot; i++) {
        bucket = &self->buckets[bix];
	hv2    = atomic_read(&bucket->hv);
	if (hatrack_bucket_unreserved(&hv2)) {
	    if (LCAS(&bucket->hv, &hv2, *hv1, HIHAT1_CTR_BUCKET_ACQUIRE)) {
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
    self = hihat1a_store_migrate(self, top);
    return hihat1a_store_put(self, top, hv1, item, found);

 found_bucket:
    record = atomic_read(&bucket->record);
    if (record.info & HIHAT_F_MOVING) {
	goto migrate_and_retry;
    }

    if (record.info & HIHAT_EPOCH_MASK) {
	if (found) {
	    *found = true;
	}
	old_item       = record.item;
	new_item       = false;
	candidate.info = record.info;
    }
    else {
	if (found) {
	    *found = false;
	}
	old_item       = NULL;
	new_item       = true;
	candidate.info = top->next_epoch++;
    }

    candidate.item = item;

    if (LCAS(&bucket->record, &record, candidate, HIHAT1_CTR_REC_INSTALL)) {
        if (new_item) {
            atomic_fetch_add(&self->item_count, 1);
        }
        return old_item;
    }

    if (record.info & HIHAT_F_MOVING) {
	goto migrate_and_retry;
    }

    return item;
}

static void *
hihat1a_store_replace(hihat1_store_t *self,
		     hihat1_t       *top,
		     hatrack_hash_t *hv1,
		     void           *item,
		     bool           *found)
{
    void            *old_item;
    uint64_t         bix;
    uint64_t         i;
    hatrack_hash_t   hv2;
    hihat1_bucket_t *bucket;
    hihat1_record_t  record;
    hihat1_record_t  candidate;

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
        goto found_bucket;
    }

 not_found:
    if (found) {
	*found = false;
    }
    return NULL;

 found_bucket:
    record = atomic_read(&bucket->record);
    if (record.info & HIHAT_F_MOVING) {
    migrate_and_retry:
	self = hihat1a_store_migrate(self, top);
	return hihat1a_store_replace(self, top, hv1, item, found);
    }

    if (!record.info) {
	goto not_found;
    }

    old_item       = record.item;
    candidate.item = item;
    candidate.info = record.info;

    while(!LCAS(&bucket->record, &record, candidate, HIHAT1_CTR_REC_INSTALL)) {
	if (record.info & HIHAT_F_MOVING) {
	    goto migrate_and_retry;
	}
	if (!record.info) {
	    goto not_found;
	}
    }
    
    if (found) {
	*found = true;
    }

    return record.item;
}

static bool
hihat1a_store_add(hihat1_store_t *self,
		 hihat1_t       *top,
		 hatrack_hash_t *hv1,
		 void           *item)
{
    uint64_t         bix;
    uint64_t         i;
    hatrack_hash_t   hv2;
    hihat1_bucket_t *bucket;
    hihat1_record_t  record;
    hihat1_record_t  candidate;

    bix = hatrack_bucket_index(hv1, self->last_slot);
    
    for (i = 0; i <= self->last_slot; i++) {
        bucket = &self->buckets[bix];
	hv2    = atomic_read(&bucket->hv);
	if (hatrack_bucket_unreserved(&hv2)) {
	    if (LCAS(&bucket->hv, &hv2, *hv1, HIHAT1_CTR_BUCKET_ACQUIRE)) {
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
    self = hihat1a_store_migrate(self, top);
    return hihat1a_store_add(self, top, hv1, item);

found_bucket:
    record = atomic_read(&bucket->record);
    if (record.info & HIHAT_F_MOVING) {
	goto migrate_and_retry;
    }
    if (record.info) {
        return false;
    }

    candidate.item = item;
    candidate.info = top->next_epoch++;

    if (LCAS(&bucket->record, &record, candidate, HIHAT1_CTR_REC_INSTALL)) {
	atomic_fetch_add(&self->item_count, 1);
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
    uint64_t         bix;
    uint64_t         i;
    hatrack_hash_t   hv2;
    hihat1_bucket_t *bucket;
    hihat1_record_t  record;
    hihat1_record_t  candidate;

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
    if (record.info & HIHAT_F_MOVING) {
    migrate_and_retry:
	self = hihat1a_store_migrate(self, top);
	return hihat1a_store_remove(self, top, hv1, found);
    }
    if (!record.info) {
        if (found) {
            *found = false;
        }

        return NULL;
    }

    old_item       = record.item;
    candidate.item = NULL;
    candidate.info = 0;

    if (LCAS(&bucket->record, &record, candidate, HIHAT1_CTR_DEL)) {
        atomic_fetch_sub(&self->item_count, 1);

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

/*
 * This function is the only thing changed from the hihat1
 * implementation, besides renaming.  Please see hihat1.c for comments
 * on the overall migration approach before reviewing this, as we only 
 * comment on the differences.
 *
 * It starts with the below static const value, which is our sleep time,
 * when we choose to wait for threads in front of us.
 */
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
    uint64_t         expected_used = 0;

    /* We first check to see if there's already a new
     * store in place in the top level. If there is, we can succeed
     * quickly.
     *
     * If there isn't, unlike hihat1, we look at the current store's
     * next pointer, to see if there is a migration in progress. If
     * there is, we consider ourselves a late writer, and give the
     * threads in front of us the opportunity to get the migration
     * done, by sleeping a tiny bit, potentially a couple of times.
     * 
     * But if sleeping doesn't work, we still have to be prepared to
     * do the migration ourselves in order to stay lock free, so when
     * we do these sleeps and we fail, the extra time spent waiting
     * was absolutely for nothing.
     */

    new_store = atomic_read(&top->store_current);
    
    if (new_store != self) {
	return new_store;
    }

    new_store = atomic_read(&self->store_next);
    if (new_store) {
	// Try twice to let anyone in front of us complete the migration.
	nanosleep(&sleep_time, NULL);
	new_store = atomic_read(&self->store_next);
	if (new_store == atomic_read(&top->store_current)) {
	    HATRACK_CTR(HATRACK_CTR_HI2_SLEEP1a_WORKED);
	    return new_store;
	}
	HATRACK_CTR(HATRACK_CTR_HI2_SLEEP1a_FAILED);
	nanosleep(&sleep_time, NULL);
	new_store = atomic_read(&self->store_next);
	if (new_store == atomic_read(&top->store_current)) {
	    HATRACK_CTR(HATRACK_CTR_HI2_SLEEP1b_WORKED);    
	    return new_store;
	}
	HATRACK_CTR(HATRACK_CTR_HI2_SLEEP1b_FAILED);
		
	goto have_new_store;
    }

    for (i = 0; i <= self->last_slot; i++) {
        bucket                = &self->buckets[i];
        record                = atomic_read(&bucket->record);
        candidate_record.item = record.item;

        do {
            if (record.info & HIHAT_F_MOVING) {
                break;
            }
	    if (record.info) {
		candidate_record.info = record.info | HIHAT_F_MOVING;
	    } else {
		candidate_record.info = HIHAT_F_MOVING | HIHAT_F_MOVED;
	    }
        } while (!LCAS(&bucket->record,
                       &record,
                       candidate_record,
                       HIHAT1_CTR_F_MOVING));

        if (record.info & HIHAT_EPOCH_MASK) {
            new_used++;
        }
    }

    new_store = atomic_read(&self->store_next);

    if (!new_store) {
        new_size        = hatrack_new_size(self->last_slot, new_used);
        candidate_store = hihat1a_store_new(new_size);
        if (!LCAS(&self->store_next,
                  &new_store,
                  candidate_store,
                  HIHAT1_CTR_NEW_STORE)) {
            mmm_retire_unused(candidate_store);
	    /* This is another place we can potentially sleep if we
	     * lose. However, we're not that far behind, and if there
	     * are only two threads going, the bigger the table, the
	     * more likely the lead thread is to get pre-empted at
	     * some point. So let's just soldier on through.
	     */
        }
        else {
            new_store = candidate_store;
        }
    }

    /* At this point, we've given up on waiting for writers in front
     * of us. But the store might have been previously installed, and
     * we may have skipped counting how many items will be migrated.
     * 
     * That means, unlike with hihat1, we have to RE-count, just in
     * case we are the thread that installs the value in the new table.
     */
 have_new_store:
    new_used = 0;
    
    for (i = 0; i <= self->last_slot; i++) {
        bucket = &self->buckets[i];
        record = atomic_read(&bucket->record);

	if (record.info & HIHAT_EPOCH_MASK) {
	    new_used++;
	}
	
        if (record.info & HIHAT_F_MOVED) {
            continue;
        }

        hv  = atomic_read(&bucket->hv);
        bix = hatrack_bucket_index(&hv, new_store->last_slot);

        for (j = 0; j <= new_store->last_slot; j++) {
            new_bucket     = &new_store->buckets[bix];
	    expected_hv    = atomic_read(&new_bucket->hv);
	    if (hatrack_bucket_unreserved(&expected_hv)) {
		if (LCAS(&new_bucket->hv,
			 &expected_hv,
			 hv,
			 HIHAT1_CTR_MIGRATE_HV)) {
		    break;
		}
		else {
		    bix = (bix + 1) & new_store->last_slot;
		    continue;
		}
	    }
	    if (!hatrack_hashes_eq(&expected_hv, &hv)) {
		bix = (bix + 1) & new_store->last_slot;
		continue;
            }
            break;
        }

        candidate_record.info = record.info & HIHAT_EPOCH_MASK;
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

    expected_used = 0;
    
    if (LCAS(&top->store_current, &self, new_store, HIHAT1_CTR_STORE_INSTALL)) {
        mmm_retire(self);
    }

    return top->store_current;
}
