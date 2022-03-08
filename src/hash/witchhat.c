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
 *  Name:           witchhat.h
 *  Description:    Waiting I Trully Cannot Handle
 *
 *                  This is a lock-free, and wait freehash table,
 *                  without consistency / full ordering.
 *
 *                  Note that witchhat is based on hihat, with a
 *                  helping mechanism in place to ensure wait freedom.
 *                  There are only a few places in hihat where we
 *                  need such a mechanism, so we will only comment on
 *                  those places.
 *
 *                  Refer to hihat.h and hihat.c for more detail on
 *                  the core algorithm, as here, we only comment on
 *                  the things that are different about witchhat.
 *
 *  Author:         John Viega, john@zork.org
 */

#include <hatrack.h>

#ifdef HATRACK_COMPILE_ALL_ALGORITHMS

// clang-format off
// Most of the store functions are needed by other modules, for better
// or worse, so we lifted their prototypes into the header.
static witchhat_store_t  *witchhat_store_migrate(witchhat_store_t *,
						 witchhat_t *);
static inline bool        witchhat_help_required(uint64_t);
static inline bool        witchhat_need_to_help (witchhat_t *);

witchhat_t *
witchhat_new(void)
{
    witchhat_t *ret;

    ret = (witchhat_t *)malloc(sizeof(witchhat_t));

    witchhat_init(ret);

    return ret;
}

witchhat_t *
witchhat_new_size(char size)
{
    witchhat_t *ret;

    ret = (witchhat_t *)malloc(sizeof(witchhat_t));

    witchhat_init_size(ret, size);

    return ret;
}

void
witchhat_init(witchhat_t *self)
{
    witchhat_init_size(self, HATRACK_MIN_SIZE_LOG);

    return;
}

void
witchhat_init_size(witchhat_t *self, char size)
{
    witchhat_store_t *store;
    uint64_t          len;

    if (size > (ssize_t)(sizeof(intptr_t) * 8)) {
	abort();
    }

    if (size < HATRACK_MIN_SIZE_LOG) {
	abort();
    }

    len              = 1 << size;
    store            = witchhat_store_new(len);
    self->next_epoch = 1;
    
    atomic_store(&self->store_current, store);
    atomic_store(&self->item_count, 0);

    return;
}

void
witchhat_cleanup(witchhat_t *self)
{
    mmm_retire(atomic_load(&self->store_current));

    return;
}

void
witchhat_delete(witchhat_t *self)
{
    witchhat_cleanup(self);
    free(self);

    return;
}

void *
witchhat_get(witchhat_t *self, hatrack_hash_t hv, bool *found)
{
    void           *ret;
    witchhat_store_t *store;

    mmm_start_basic_op();
    
    store = atomic_read(&self->store_current);
    ret   = witchhat_store_get(store, hv, found);
    
    mmm_end_op();

    return ret;
}

void *
witchhat_put(witchhat_t *self, hatrack_hash_t hv, void *item, bool *found)
{
    void           *ret;
    witchhat_store_t *store;

    mmm_start_basic_op();
    
    store = atomic_read(&self->store_current);
    ret   = witchhat_store_put(store, self, hv, item, found, 0);
    
    mmm_end_op();

    return ret;
}

void *
witchhat_replace(witchhat_t *self, hatrack_hash_t hv, void *item, bool *found)
{
    void           *ret;
    witchhat_store_t *store;

    mmm_start_basic_op();
    
    store = atomic_read(&self->store_current);
    ret   = witchhat_store_replace(store, self, hv, item, found, 0);
    
    mmm_end_op();

    return ret;
}

bool
witchhat_add(witchhat_t *self, hatrack_hash_t hv, void *item)
{
    bool            ret;
    witchhat_store_t *store;

    mmm_start_basic_op();
    
    store = atomic_read(&self->store_current);    
    ret   = witchhat_store_add(store, self, hv, item, 0);
    
    mmm_end_op();

    return ret;
}

void *
witchhat_remove(witchhat_t *self, hatrack_hash_t hv, bool *found)
{
    void           *ret;
    witchhat_store_t *store;

    mmm_start_basic_op();
    
    store = atomic_read(&self->store_current);    
    ret   = witchhat_store_remove(store, self, hv, found, 0);
    
    mmm_end_op();

    return ret;
}

uint64_t
witchhat_len(witchhat_t *self)
{
    return atomic_read(&self->item_count);
}

hatrack_view_t *
witchhat_view(witchhat_t *self, uint64_t *num, bool start)
{
    hatrack_view_t *ret;
    
    mmm_start_basic_op();
    
    ret = witchhat_view_no_mmm(self, num, start);

    mmm_end_op();

    return ret;
}

/* Used by dict.c, which does mmm itself, so that it can give callers
 * the opportunity to refcount items put into the output.
 */
hatrack_view_t *
witchhat_view_no_mmm(witchhat_t *self, uint64_t *num, bool sort)
{
    hatrack_view_t    *view;
    hatrack_view_t    *p;
    witchhat_bucket_t *cur;
    witchhat_bucket_t *end;
    witchhat_record_t  record;
    uint64_t           num_items;
    uint64_t           alloc_len;
    witchhat_store_t  *store;

    store     = atomic_read(&self->store_current);
    alloc_len = sizeof(hatrack_view_t) * (store->last_slot + 1);
    view      = (hatrack_view_t *)malloc(alloc_len);
    p         = view;
    cur       = store->buckets;
    end       = cur + (store->last_slot + 1);

    while (cur < end) {
        record        = atomic_read(&cur->record);
        p->sort_epoch = record.info & WITCHHAT_EPOCH_MASK;

	if (!p->sort_epoch) {
	    cur++;
	    continue;
	}
	
        p->item       = record.item;
	
        p++;
        cur++;
    }

    num_items = p - view;
    *num      = num_items;

    if (!num_items) {
        free(view);
	
        return NULL;
    }

    view = realloc(view, num_items * sizeof(hatrack_view_t));

    if (sort) {
	qsort(view, num_items, sizeof(hatrack_view_t), hatrack_quicksort_cmp);
    }

    return view;
}

witchhat_store_t *
witchhat_store_new(uint64_t size)
{
    witchhat_store_t *store;
    uint64_t        alloc_len;

    alloc_len = sizeof(witchhat_store_t) + sizeof(witchhat_bucket_t) * size;
    store     = (witchhat_store_t *)mmm_alloc_committed(alloc_len);

    store->last_slot  = size - 1;
    store->threshold  = hatrack_compute_table_threshold(size);

    return store;
}

void *
witchhat_store_get(witchhat_store_t *self,
                 hatrack_hash_t      hv1,
                 bool               *found)
{
    uint64_t           bix;
    uint64_t           i;
    hatrack_hash_t     hv2;
    witchhat_bucket_t *bucket;
    witchhat_record_t  record;

    bix = hatrack_bucket_index(hv1, self->last_slot);
    
    for (i = 0; i <= self->last_slot; i++) {
        bucket = &self->buckets[bix];
        hv2    = atomic_read(&bucket->hv);
	
        if (hatrack_bucket_unreserved(hv2)) {
            goto not_found;
        }
	
        if (!hatrack_hashes_eq(hv1, hv2)) {
            bix = (bix + 1) & self->last_slot;
            continue;
        }

        record = atomic_read(&bucket->record);
	
        if (record.info & WITCHHAT_EPOCH_MASK) {
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

void *
witchhat_store_put(witchhat_store_t *self,
		   witchhat_t       *top,
		   hatrack_hash_t    hv1,
		   void             *item,
		   bool             *found,
		   uint64_t          count)
{
    void              *old_item;
    bool               new_item;
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
	
	if (hatrack_bucket_unreserved(hv2)) {
	    if (LCAS(&bucket->hv, &hv2, hv1, WITCHHAT_CTR_BUCKET_ACQUIRE)) {
		if (atomic_fetch_add(&self->used_count, 1) >= self->threshold) {
		    goto migrate_and_retry;
		}
		
		goto found_bucket;
	    }
	}
	
	if (hatrack_hashes_eq(hv1, hv2)) {
	    goto found_bucket;
	}
	
	bix = (bix + 1) & self->last_slot;
	continue;
    }
    
 migrate_and_retry:
    /* One of the places where hihat is lock-free instead of wait-free
     * is when a write operation has to help migrate. In theory, it
     * could go help migrate, and by the time it tries to write again,
     * it has to participate in the next migration. 
     *
     * Now, if we only ever doubled the size of the table, this
     * operation would be wait-free, because there's an upper bound on
     * the number of table resizes that would happen. However, table
     * sizes can shrink, or stay the same. So a workload that clutters
     * up a table with lots of deleted items could theoretically leave
     * a thread waiting indefinitely.
     *
     * This case isn't very practical in the real world, but we can
     * still guard against it, with nearly zero cost. Our approach is
     * to count the number of attempts we make to mutate the table
     * that result in a resizing, and when we hit a particular
     * threshold, we "ask for help". When a thread needs help writing
     * in the face of migrations, it means that no thread that comes
     * along to migrate after the request is registered will migrate
     * to a same-size or smaller table. It FORCES the table size to
     * double on a migration, giving us a small bound of how long we
     * might wait.
     *
     * Once the resquest is satisfied, we deregister our request for
     * help.
     *
     * With all my initial test cases, which are mainly write-heavy
     * workloads, if setting the threshold to 8, this help mechanism
     * never triggers, and it barely ever triggers at a threshold of
     * 6.
     */
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

    if (record.info & WITCHHAT_EPOCH_MASK) {
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
	candidate.info = WITCHHAT_F_INITED | top->next_epoch++;
    }

    candidate.item = item;

    if (LCAS(&bucket->record, &record, candidate, WITCHHAT_CTR_REC_INSTALL)) {
        if (new_item) {
            atomic_fetch_add(&top->item_count, 1);
        }
	
        return old_item;
    }

    if (record.info & WITCHHAT_F_MOVING) {
	goto migrate_and_retry;
    }

    /* In witchhat, whenever we successfully overwrite a value, we
     * need to help migrate, if a migration is in process.  See
     * witchhat_store_migrate() for a bit more detailed an
     * explaination, but doing this helps make
     * witchhat_store_migrate() wait free.
     */
    if (!new_item) {
	if (atomic_read(&self->used_count) >= self->threshold) {
	    witchhat_store_migrate(self, top);
	}
    }

    return item;
}

void *
witchhat_store_replace(witchhat_store_t *self,
		       witchhat_t       *top,
		       hatrack_hash_t    hv1,
		       void             *item,
		       bool             *found,
		       uint64_t          count)
{
    void              *ret;
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
	
	if (hatrack_bucket_unreserved(hv2)) {
	    goto not_found;
	}
	
	if (hatrack_hashes_eq(hv1, hv2)) {
	    goto found_bucket;
	}
	
	bix = (bix + 1) & self->last_slot;
	continue;
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
	// This uses the same helping mechanism as in
	// witchhat_store_put().  Look there for an overview.
	count = count + 1;
	
	if (witchhat_help_required(count)) {
	    HATRACK_CTR(HATRACK_CTR_WH_HELP_REQUESTS);
	    
	    atomic_fetch_add(&top->help_needed, 1);
	    self = witchhat_store_migrate(self, top);
	    ret  = witchhat_store_replace(self, top, hv1, item, found, count);
	    
	    atomic_fetch_sub(&top->help_needed, 1);
	    
	    return ret;
	}
	
	self = witchhat_store_migrate(self, top);
	return witchhat_store_replace(self, top, hv1, item, found, count);
    }

    if (!(record.info & WITCHHAT_EPOCH_MASK)) {
	goto not_found;
    }

    candidate.item = item;
    candidate.info = record.info;

    /* In our hihat implementation, when there's contention for this
     * compare-and-swap, we take a lock-free approach, but not a
     * wait-free approach.  Here, we take a wait-free approach.
     *
     * When this CAS failed, there are two possible reasons:
     *
     * 1) When we checked, no migration was in progress, but when we
     *    tried to update the record, there was one, in which case we
     *    need to go off and help.
     *
     * 2) Another thread beat us to updating the bucket, in which case
     *    we will consider our update "successful", as if it happened
     *    first, and then got overwritten.  In that case, we return
     *    the item we were going to insert, for the sake of memory
     *    management. For purposes of ordering, we are claiming that
     *    we successfully overwrote the old record before the item
     *    that won the CAS race. But we're unable to return the item,
     *    because the thing that came along and knocked us out, didn't
     *    free our memory.
     *
     * Essentially, all we are doing is simply replacing a while loop
     * that could theoretically last forever with a single attempt.
     */
    if(!LCAS(&bucket->record, &record, candidate, WITCHHAT_CTR_REC_INSTALL)) {
	if (record.info & WITCHHAT_F_MOVING) {
	    goto migrate_and_retry;
	}
	
	goto not_found;
    }
    
    if (found) {
	*found = true;
    }

    /* In witchhat, whenever we successfully overwrite a value, we
     * need to help migrate, if a migration is in process.  See
     * witchhat_store_migrate() for a bit more detailed an
     * explaination, but doing this makes witchhat_store_migrate()
     * wait free.
     */
    if (atomic_read(&self->used_count) >= self->threshold) {
	witchhat_store_migrate(self, top);
    }    

    return record.item;
}

bool
witchhat_store_add(witchhat_store_t *self,
		   witchhat_t       *top,
		   hatrack_hash_t    hv1,
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
	
	if (hatrack_bucket_unreserved(hv2)) {
	    if (LCAS(&bucket->hv, &hv2, hv1, WITCHHAT_CTR_BUCKET_ACQUIRE)) {
		if (atomic_fetch_add(&self->used_count, 1) >= self->threshold) {
		    goto migrate_and_retry;
		}
		goto found_bucket;
	    }
	}
	
	if (!hatrack_hashes_eq(hv1, hv2)) {
	    bix = (bix + 1) & self->last_slot;
	    continue;
	}
	
        goto found_bucket;
    }

 migrate_and_retry:
    // This uses the same helping mechanism as in
    // witchhat_store_put().  Look there for an overview.
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
    
    if ((record.info & WITCHHAT_EPOCH_MASK)) {
        return false;
    }

    candidate.item = item;
    candidate.info = WITCHHAT_F_INITED | top->next_epoch++;

    if (LCAS(&bucket->record, &record, candidate, WITCHHAT_CTR_REC_INSTALL)) {
	atomic_fetch_add(&top->item_count, 1);
        return true;
    }
    
    if (record.info & WITCHHAT_F_MOVING) {
	goto migrate_and_retry;
    }

    return false;
}

void *
witchhat_store_remove(witchhat_store_t *self,
		      witchhat_t       *top,
		      hatrack_hash_t    hv1,
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
	
        if (hatrack_bucket_unreserved(hv2)) {
            break;
        }
	
        if (hatrack_hashes_eq(hv1, hv2)) {
	    goto found_bucket;
        }
	
	bix = (bix + 1) & self->last_slot;
	continue;
    }

    if (found) {
        *found = false;
    }

    return NULL;

found_bucket:
    record = atomic_read(&bucket->record);
    if (record.info & WITCHHAT_F_MOVING) {
    migrate_and_retry:
	// This uses the same helping mechanism as in
	// witchhat_store_put().  Look there for an overview.
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
    
    if (!(record.info & WITCHHAT_EPOCH_MASK)) {
        if (found) {
            *found = false;
        }

        return NULL;
    }

    old_item       = record.item;
    candidate.item = NULL;
    candidate.info = WITCHHAT_F_INITED;

    if (LCAS(&bucket->record, &record, candidate, WITCHHAT_CTR_DEL)) {
        atomic_fetch_sub(&top->item_count, 1);

        if (found) {
            *found = true;
        }


	/* In witchhat, whenever we successfully overwrite a value, we
	 * need to help migrate, if a migration is in process.  See
	 * witchhat_store_migrate() for a bit more detailed an
	 * explaination, but doing this makes witchhat_store_migrate()
	 * wait free.
	 */
	if (atomic_read(&self->used_count) >= self->threshold) {
	    witchhat_store_migrate(self, top);
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
    uint64_t           new_size;
    witchhat_bucket_t *bucket;
    witchhat_bucket_t *new_bucket;
    witchhat_record_t  record;
    witchhat_record_t  candidate_record;
    witchhat_record_t  expected_record;
    hatrack_hash_t     expected_hv;
    hatrack_hash_t     hv;
    uint64_t           i, j;
    uint64_t           bix;
    uint64_t           new_used;
    uint64_t           expected_used;

    new_used  = 0;
    new_store = atomic_read(&top->store_current);
    
    if (new_store != self) {
	return new_store;
    }

    for (i = 0; i <= self->last_slot; i++) {
        bucket                = &self->buckets[i];
        record                = atomic_read(&bucket->record);
        candidate_record.item = record.item;

        if (record.info & WITCHHAT_EPOCH_MASK) {
            new_used++;
        }
	
	if (record.info & WITCHHAT_F_MOVING) {
	    continue;
	}
	    
	if (record.info & WITCHHAT_EPOCH_MASK) {
	    OR2X64L(&bucket->record, WITCHHAT_F_MOVING);
	}
	else {
	    OR2X64L(&bucket->record, WITCHHAT_F_MOVING |
		    WITCHHAT_F_MOVED);
	}
    }

    new_store = atomic_read(&self->store_next);

    if (!new_store) {
	/* When threads need help in the face of a resize, this is
	 * where we provide that help.  We do it simply by forcing 
	 * the table to resize up, when help is required.
	 *
	 * Note that different threads might end up producing
	 * different store sizes, if their value of top->help_needed
	 * changes.  This is ultimately irrelevent, because whichever
	 * store we swap in will be big enough to handle the
	 * migration. 
	 *
	 * Plus, the helper isn't the one responsible for
	 * determining when help is no longer necessary, so if the
	 * smaller store is selected, the next resize will definitely
	 * be bigger, if help was needed continuously.
	 *
	 * This mechanism is, in practice, the only mechanism that
	 * seems like it might have any sort of impact on the overall
	 * performance of the algorithm, and if it does have an
	 * impact, it seems to be completely in the noise.
	 */
	if (witchhat_need_to_help(top)) {
	    new_size = (self->last_slot + 1) << 1;
	}
	else {
	    new_size        = hatrack_new_size(self->last_slot, new_used);
	}
	
        candidate_store = witchhat_store_new(new_size);
	
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

    for (i = 0; i <= self->last_slot; i++) {
        bucket = &self->buckets[i];
        record = atomic_read(&bucket->record);

        if (record.info & WITCHHAT_F_MOVED) {
            continue;
        }

        hv  = atomic_read(&bucket->hv);
        bix = hatrack_bucket_index(hv, new_store->last_slot);

        for (j = 0; j <= new_store->last_slot; j++) {
            new_bucket     = &new_store->buckets[bix];
	    expected_hv    = atomic_read(&new_bucket->hv);
	    
	    if (hatrack_bucket_unreserved(expected_hv)) {
		if (LCAS(&new_bucket->hv,
			 &expected_hv,
			 hv,
			 WITCHHAT_CTR_MIGRATE_HV)) {
		    break;
		}
	    }
	    
	    if (!hatrack_hashes_eq(expected_hv, hv)) {
		bix = (bix + 1) & new_store->last_slot;
		continue;
            }
	    
            break;
        }

        candidate_record.info = record.info & WITCHHAT_EPOCH_MASK;
        candidate_record.item = record.item;
        expected_record.info  = 0;
        expected_record.item  = NULL;

        LCAS(&new_bucket->record,
	     &expected_record,
	     candidate_record,
	     WITCHHAT_CTR_MIG_REC);

	OR2X64L(&bucket->record, WITCHHAT_F_MOVED);
    }

    expected_used = 0;
    
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

    return top->store_current;
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

#endif
