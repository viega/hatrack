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
 *  Name:           crown.h
 *  Description:    Could Really Own.
 *
 *                  Witchhat, but with a different probing strategy,
 *                  based on hopscotch, but without the moving.
 *
 *  Author:         John Viega, john@zork.org
 *
 */

#include <hatrack.h>
#include <hatrack/crown.h>

// clang-format off
// Most of the store functions are needed by other modules, for better
// or worse, so we lifted their prototypes into the header.
static crown_store_t *crown_store_migrate(crown_store_t *, crown_t *, int64_t);
static inline bool    crown_help_required(uint64_t);
static inline bool    crown_need_to_help (crown_t *);

crown_t *
crown_new(void)
{
    crown_t *ret;

    ret = (crown_t *)malloc(sizeof(crown_t));

    crown_init(ret);

    return ret;
}

void
crown_init(crown_t *self)
{
    crown_store_t *store;
    uint64_t       len;

    len              = HATRACK_MIN_SIZE >  64 ? HATRACK_MIN_SIZE : 64;
    store            = crown_store_new(len);
    self->next_epoch = 1;
    
    atomic_store(&self->store_current, store);
    atomic_store(&self->item_count, 0);

    return;
}

void
crown_cleanup(crown_t *self)
{
    mmm_retire(atomic_load(&self->store_current));

    return;
}

void
crown_delete(crown_t *self)
{
    crown_cleanup(self);
    free(self);

    return;
}

void *
crown_get(crown_t *self, hatrack_hash_t hv, bool *found)
{
    void           *ret;
    crown_store_t *store;

    mmm_start_basic_op();
    
    store = atomic_read(&self->store_current);
    ret   = crown_store_get(store, hv, found);
    
    mmm_end_op();

    return ret;
}

void *
crown_put(crown_t *self, hatrack_hash_t hv, void *item, bool *found)
{
    void           *ret;
    crown_store_t *store;

    mmm_start_basic_op();
    
    store = atomic_read(&self->store_current);
    ret   = crown_store_put(store, self, hv, item, found, 0);
    
    mmm_end_op();

    return ret;
}

void *
crown_replace(crown_t *self, hatrack_hash_t hv, void *item, bool *found)
{
    void           *ret;
    crown_store_t *store;

    mmm_start_basic_op();
    
    store = atomic_read(&self->store_current);
    ret   = crown_store_replace(store, self, hv, item, found, 0);
    
    mmm_end_op();

    return ret;
}

bool
crown_add(crown_t *self, hatrack_hash_t hv, void *item)
{
    bool            ret;
    crown_store_t *store;

    mmm_start_basic_op();
    
    store = atomic_read(&self->store_current);    
    ret   = crown_store_add(store, self, hv, item, 0);
    
    mmm_end_op();

    return ret;
}

void *
crown_remove(crown_t *self, hatrack_hash_t hv, bool *found)
{
    void           *ret;
    crown_store_t *store;

    mmm_start_basic_op();
    
    store = atomic_read(&self->store_current);    
    ret   = crown_store_remove(store, self, hv, found, 0);
    
    mmm_end_op();

    return ret;
}

uint64_t
crown_len(crown_t *self)
{
    return atomic_read(&self->item_count);
}

hatrack_view_t *
crown_view(crown_t *self, uint64_t *num, bool start)
{
    hatrack_view_t *ret;
    
    mmm_start_basic_op();
    
    ret = crown_view_no_mmm(self, num, start);

    mmm_end_op();

    return ret;
}

/* Used by dict.c, which does mmm itself, so that it can give callers
 * the opportunity to refcount items put into the output.
 */
hatrack_view_t *
crown_view_no_mmm(crown_t *self, uint64_t *num, bool sort)
{
    hatrack_view_t *view;
    hatrack_view_t *p;
    crown_bucket_t *cur;
    crown_bucket_t *end;
    crown_record_t  record;
    uint64_t        num_items;
    uint64_t        alloc_len;
    crown_store_t  *store;

    store     = atomic_read(&self->store_current);
    alloc_len = sizeof(hatrack_view_t) * (store->last_slot + 1);
    view      = (hatrack_view_t *)malloc(alloc_len);
    p         = view;
    cur       = store->buckets;
    end       = cur + (store->last_slot + 1);

    while (cur < end) {
        record        = atomic_read(&cur->record);
        p->sort_epoch = record.info & CROWN_EPOCH_MASK;

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

crown_store_t *
crown_store_new(uint64_t size)
{
    crown_store_t *store;
    uint64_t        alloc_len;

    alloc_len = sizeof(crown_store_t) + sizeof(crown_bucket_t) * size;
    store     = (crown_store_t *)mmm_alloc_committed(alloc_len);

    store->last_slot  = size - 1;
    store->threshold  = hatrack_compute_table_threshold(size);

    return store;
}

void *
crown_store_get(crown_store_t *self, hatrack_hash_t hv1, bool *found)
{
    uint64_t        bix;
    uint64_t        neighbor_map;
    uint64_t        offset;
    hatrack_hash_t  hv2;
    crown_bucket_t *bucket;
    crown_record_t  record;

    bix          = hatrack_bucket_index(hv1, self->last_slot);
    neighbor_map = atomic_read(&self->buckets[bix].neighbor_map);

    while (neighbor_map) {
	offset = __builtin_clzll(neighbor_map);
	bucket = &self->buckets[(bix + offset) & self->last_slot];
	hv2    = atomic_read(&bucket->hv);

	if (hatrack_hashes_eq(hv1, hv2)) {
	    record = atomic_read(&bucket->record);
	    if (record.info & CROWN_EPOCH_MASK) {
		if (found) {
		    *found = true;
		}
		return record.item;
	    }
	    else {
		break; // Sorry, it's been deleted.
	    }
	}

	neighbor_map &= ~(CROWN_HOME_BIT >> offset);
    }

    if (found) {
	*found = false;
    }

    return NULL;
}

void *
crown_store_put(crown_store_t *self,
		crown_t       *top,
		hatrack_hash_t hv1,
		void          *item,
		bool          *found,
		uint64_t       count)
{
    void           *old_item;
    bool            new_item;
    uint64_t        nix;
    uint64_t        bix;
    uint64_t        map;
    uint64_t        new_map;
    uint64_t        bit_to_set;
    uint64_t        i;
    hatrack_hash_t  hv2;
    crown_bucket_t *bucket;
    crown_bucket_t *nhood_bucket;    
    crown_record_t  record;
    crown_record_t  candidate;

    nix          = hatrack_bucket_index(hv1, self->last_slot);
    bix          = nix;
    nhood_bucket = &self->buckets[nix];
    map          = atomic_read(&nhood_bucket->neighbor_map);
    i            = -1;

    while (map) {
	i      = __builtin_clzll(map);
	bucket = &self->buckets[(bix + i) & self->last_slot];
	hv2    = atomic_read(&bucket->hv);

	
	if (hatrack_hashes_eq(hv1, hv2)) {
	    goto found_bucket;
	}
	map &= ~(CROWN_HOME_BIT >> i);
    }

    i++;
    bix = (bix + i) & self->last_slot;

    for (; i < CROWN_NEIGHBORS; i++) {
        bucket = &self->buckets[bix];
	hv2    = atomic_read(&bucket->hv);

	if (hatrack_bucket_unreserved(hv2)) {
	    if (CAS(&bucket->hv, &hv2, hv1)) {
		bit_to_set   = CROWN_HOME_BIT >> i;
		map          = atomic_load(&nhood_bucket->neighbor_map);
		do {
		    new_map = map | bit_to_set;
		} while (!CAS(&nhood_bucket->neighbor_map, &map, new_map));
		goto found_bucket;
	    }
	}

	/* A racing thread could have beaten us here; just because
	 * we didn't find it in the hopping doesn't mean that we
	 * skip this check.
	 */
	if (hatrack_hashes_eq(hv1, hv2)) {
	    goto found_bucket;
	}

	bix = (bix + 1) & self->last_slot;
	continue;
    }

 migrate_and_retry:
    count = count + 1;
    if (crown_help_required(count)) {
	atomic_fetch_add(&top->help_needed, 1);
	
	self     = crown_store_migrate(self, top, nix);
	old_item = crown_store_put(self, top, hv1, item, found, count);
	
	atomic_fetch_sub(&top->help_needed, 1);
	
	return old_item;
    }

    self = crown_store_migrate(self, top, nix);
    return crown_store_put(self, top, hv1, item, found, count);

 found_bucket:
    record = atomic_read(&bucket->record);
    
    if (record.info & CROWN_F_MOVING) {
	goto migrate_and_retry;
    }

    if (record.info & CROWN_EPOCH_MASK) {
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

    if (CAS(&bucket->record, &record, candidate)) {
        if (new_item) {
            atomic_fetch_add(&top->item_count, 1);
        }
	
        return old_item;
    }

    if (record.info & CROWN_F_MOVING) {
	goto migrate_and_retry;
    }

    /* In crown, whenever we successfully overwrite a value, we
     * need to help migrate, if a migration is in process.  
     *
     * We'll determine that by whether a new store is installed.
     */
    if (!new_item) {
	if (atomic_read(&self->store_next)) {
	    crown_store_migrate(self, top, -1);
	}
    }

    return item;
}

void *
crown_store_replace(crown_store_t *self,
		    crown_t       *top,
		    hatrack_hash_t hv1,
		    void          *item,
		    bool          *found,
		    uint64_t       count)
{
    void           *ret;
    uint64_t        bix;
    uint64_t        neighbor_map;
    uint64_t        offset;    
    hatrack_hash_t  hv2;
    crown_bucket_t *bucket;
    crown_record_t  record;
    crown_record_t  candidate;

    bix          = hatrack_bucket_index(hv1, self->last_slot);
    neighbor_map = atomic_read(&self->buckets[bix].neighbor_map);
    
    while (neighbor_map) {
	offset = __builtin_clzll(neighbor_map);
	bucket = &self->buckets[(bix + offset) & self->last_slot];
	hv2    = atomic_read(&bucket->hv);
	
	if (hatrack_hashes_eq(hv1, hv2)) {
	    goto found_bucket;
	}
	
	neighbor_map &= ~(CROWN_HOME_BIT >> offset);
    }

 not_found:
    if (found) {
	*found = false;
    }
    return NULL;

 found_bucket:
    record = atomic_read(&bucket->record);
    
    if (record.info & CROWN_F_MOVING) {
    migrate_and_retry:
	// This uses the same helping mechanism as in
	// crown_store_put().  Look there for an overview.
	count = count + 1;
	
	if (crown_help_required(count)) {
	    HATRACK_CTR(HATRACK_CTR_WH_HELP_REQUESTS);
	    
	    atomic_fetch_add(&top->help_needed, 1);
	    self = crown_store_migrate(self, top, -1);
	    ret  = crown_store_replace(self, top, hv1, item, found, count);
	    
	    atomic_fetch_sub(&top->help_needed, 1);
	    
	    return ret;
	}
	
	self = crown_store_migrate(self, top, -1);
	return crown_store_replace(self, top, hv1, item, found, count);
    }

    if (!record.info) {
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
    if(!CAS(&bucket->record, &record, candidate)) {
	if (record.info & CROWN_F_MOVING) {
	    goto migrate_and_retry;
	}
	
	goto not_found;
    }
    
    if (found) {
	*found = true;
    }

    /* In crown, whenever we successfully overwrite a value, we
     * need to help migrate, if a migration is in process.  See
     * crown_store_migrate() for a bit more detailed an
     * explaination, but doing this makes crown_store_migrate()
     * wait free.
     */
    if (atomic_read(&self->store_next)) {
	crown_store_migrate(self, top, -1);
    }    

    return record.item;
}

bool
crown_store_add(crown_store_t *self,
		crown_t       *top,
		hatrack_hash_t hv1,
		void          *item,
		uint64_t       count)
{
    uint64_t        nix;
    uint64_t        bix;
    uint64_t        i;
    uint64_t        map;
    uint64_t        new_map;
    uint64_t        bit_to_set;
    hatrack_hash_t  hv2;
    crown_bucket_t *bucket;
    crown_bucket_t *nhood_bucket;    
    crown_record_t  record;
    crown_record_t  candidate;

    nix          = hatrack_bucket_index(hv1, self->last_slot);
    bix          = nix;
    nhood_bucket = &self->buckets[nix];
    map          = atomic_read(&nhood_bucket->neighbor_map);
    i            = -1;

    while (map) {
	i      = __builtin_clzll(map);
	bucket = &self->buckets[(bix + i) & self->last_slot];
	hv2    = atomic_read(&bucket->hv);
	
	if (hatrack_hashes_eq(hv1, hv2)) {
	    goto found_bucket;
	}
	
	map &= ~(CROWN_HOME_BIT >> i);
	
    }

    i++;
    bix = (bix + i) & self->last_slot;
    
    for (; i < CROWN_NEIGHBORS; i++) {
        bucket = &self->buckets[bix];
	hv2    = atomic_read(&bucket->hv);

	if (hatrack_bucket_unreserved(hv2)) {
	    if (CAS(&bucket->hv, &hv2, hv1)) {
		bit_to_set   = CROWN_HOME_BIT >> i;
		map          = atomic_load(&nhood_bucket->neighbor_map);
		do {
		    new_map = map | bit_to_set;
		} while (!CAS(&nhood_bucket->neighbor_map, &map, new_map));
		goto found_bucket;
	    }
	}

	/* A racing thread could have beaten us here; just because
	 * we didn't find it in the hopping doesn't mean that we
	 * skip this check.
	 */
	if (hatrack_hashes_eq(hv1, hv2)) {
	    goto found_bucket;
	}
	
	bix = (bix + 1) & self->last_slot;
	continue;
    }

 migrate_and_retry:
    count = count + 1;
    if (crown_help_required(count)) {
	bool ret;

	atomic_fetch_add(&top->help_needed, 1);
	
	self = crown_store_migrate(self, top, nix);
	ret  = crown_store_add(self, top, hv1, item, count);
	
	atomic_fetch_sub(&top->help_needed, 1);

	return ret;
    }
    
    self = crown_store_migrate(self, top, nix);
    return crown_store_add(self, top, hv1, item, count);

found_bucket:
    record = atomic_read(&bucket->record);
    if (record.info & CROWN_F_MOVING) {
	goto migrate_and_retry;
    }
    
    if (record.info) {
        return false;
    }

    candidate.item = item;
    candidate.info = top->next_epoch++;

    if (CAS(&bucket->record, &record, candidate)) {
	atomic_fetch_add(&top->item_count, 1);
        return true;
    }
    
    if (record.info & CROWN_F_MOVING) {
	goto migrate_and_retry;
    }

    return false;
}

void *
crown_store_remove(crown_store_t *self,
		   crown_t       *top,
		   hatrack_hash_t hv1,
		   bool          *found,
		   uint64_t       count)
{
    void           *old_item;
    uint64_t        bix;
    uint64_t        offset;
    uint64_t        neighbor_map;
    hatrack_hash_t  hv2;
    crown_bucket_t *bucket;
    crown_record_t  record;
    crown_record_t  candidate;

    bix          = hatrack_bucket_index(hv1, self->last_slot);
    neighbor_map = atomic_read(&self->buckets[bix].neighbor_map);
    
    while (neighbor_map) {
	offset = __builtin_clzll(neighbor_map);
	bucket = &self->buckets[(bix + offset) & self->last_slot];
	hv2    = atomic_read(&bucket->hv);

	if (hatrack_hashes_eq(hv1, hv2)) {
	    goto found_bucket;
	}

	neighbor_map &= ~(CROWN_HOME_BIT >> offset);
    }

    if (found) {
        *found = false;
    }

    return NULL;

found_bucket:
    record = atomic_read(&bucket->record);
    if (record.info & CROWN_F_MOVING) {
    migrate_and_retry:
	// This uses the same helping mechanism as in
	// crown_store_put().  Look there for an overview.
	count = count + 1;
	
	if (crown_help_required(count)) {
	    HATRACK_CTR(HATRACK_CTR_WH_HELP_REQUESTS);
	    atomic_fetch_add(&top->help_needed, 1);
	    self     = crown_store_migrate(self, top, -1);
	    old_item = crown_store_remove(self, top, hv1, found, count);
	    atomic_fetch_sub(&top->help_needed, 1);
	    return old_item;
	}
	
	self = crown_store_migrate(self, top, -1);
	return crown_store_remove(self, top, hv1, found, count);
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

    if (CAS(&bucket->record, &record, candidate)) {
        atomic_fetch_sub(&top->item_count, 1);

        if (found) {
            *found = true;
        }


	/* In crown, whenever we successfully overwrite a value, we
	 * need to help migrate, if a migration is in process.  See
	 * crown_store_migrate() for a bit more detailed an
	 * explaination, but doing this makes crown_store_migrate()
	 * wait free.
	 */
	if (atomic_read(&self->store_next)) {
	    crown_store_migrate(self, top, -1);
	}
	
        return old_item;
    }
    
    if (record.info & CROWN_F_MOVING) {
	goto migrate_and_retry;
    }

    if (found) {
        *found = false;
    }
    
    return NULL;
}

static crown_store_t *
crown_store_migrate(crown_store_t *self, crown_t *top, int64_t nix)
{
    crown_store_t  *new_store;
    crown_store_t  *candidate_store;
    uint64_t        old_size;
    uint64_t        new_size;
    uint64_t        neighbor_map;
    uint64_t        new_map;
    uint64_t        bit_to_set;
    crown_bucket_t *bucket;
    crown_bucket_t *new_bucket;
    crown_bucket_t *nhood_bucket;    
    crown_record_t  record;
    crown_record_t  candidate_record;
    crown_record_t  expected_record;
    hatrack_hash_t  hv;
    hatrack_hash_t  expected_hv;
    uint64_t        i;
    uint64_t        bix;
    uint64_t        new_used;

    new_used  = 0;
    new_store = atomic_read(&top->store_current);
    
    if (new_store != self) {
	return new_store;
    }

    /* Unlike other algorithms, the FIRST thing we do is create the
     * new store. We are happy to let other writers write if their
     * neighborhood has space.
     */
    new_store = atomic_read(&self->store_next);

    if (!new_store) {
	new_used = atomic_read(&top->item_count);
	old_size = self->last_slot + 1;

	if (new_used <= (old_size >> 2)) {
	    new_size = old_size >> 1;
	}

	if (crown_need_to_help(top) || (new_used > (old_size >> 1))) {
	    new_size = old_size << 1;
	}
	
        candidate_store = crown_store_new(new_size);
	
        if (!CAS(&self->store_next, &new_store, candidate_store)) {
            mmm_retire_unused(candidate_store);
        }
        else {
            new_store = candidate_store;
        }
    }

    for (i = 0; i <= self->last_slot; i++) {
        bucket                = &self->buckets[i];
        record                = atomic_read(&bucket->record);
        candidate_record.item = record.item;

        do {
            if (record.info & CROWN_F_MOVING) {
                break;
            }
	    
	    if (record.info) {
		candidate_record.info = record.info | CROWN_F_MOVING;
	    }
	    else {
		candidate_record.info = CROWN_F_MOVING | CROWN_F_MOVED;
	    }
        } while (!CAS(&bucket->record, &record, candidate_record));

        if (record.info & CROWN_EPOCH_MASK) {
            new_used++;
        }
    }

    for (i = 0; i <= self->last_slot; i++) {
        bucket = &self->buckets[i];
        record = atomic_read(&bucket->record);

        if (record.info & CROWN_F_MOVED) {
            continue;
        }

        hv           = atomic_read(&bucket->hv);
	record       = atomic_read(&bucket->record);
        nix          = hatrack_bucket_index(hv, new_store->last_slot);
	bix          = nix;
	nhood_bucket = &new_store->buckets[nix];
	neighbor_map = atomic_read(&nhood_bucket->neighbor_map);
	i            = -1;

	while (neighbor_map) {
	    i           = __builtin_clzll(neighbor_map);
	    bix         = (nix + i) & new_store->last_slot;
	    new_bucket  = &new_store->buckets[bix];
	    expected_hv = atomic_read(&new_bucket->hv);

	    if (hatrack_hashes_eq(hv, expected_hv)) {
		goto found_bucket;
	    }
	    neighbor_map &= ~(CROWN_HOME_BIT >> i);
	}
	i++;
    
	while (true) {
	    bix         = (nix + i) & new_store->last_slot;
	    new_bucket  = &new_store->buckets[bix];
	    expected_hv = atomic_read(&new_bucket->hv);
	    
	    if (hatrack_bucket_unreserved(expected_hv)) {
		if (CAS(&new_bucket->hv, &expected_hv, hv)) {
		    bit_to_set   = CROWN_HOME_BIT >> i;
		    neighbor_map = atomic_load(&nhood_bucket->neighbor_map);
		    do {
			new_map = neighbor_map | bit_to_set;
		    } while (!CAS(&nhood_bucket->neighbor_map, &neighbor_map, new_map));
		    goto found_bucket;
		}
	    }
	    
	    /* A racing thread could have beaten us here; just because
	     * we didn't find it in the hopping doesn't mean that we
	     * skip this check.
	     */
	    if (hatrack_hashes_eq(hv, expected_hv)) {
		goto found_bucket;
	    }
	    
	    i++;
	    continue;
	}

    found_bucket:
	ASSERT(i < CROWN_NEIGHBORS);
	
	candidate_record.info = record.info & CROWN_EPOCH_MASK;
	candidate_record.item = record.item;
	expected_record.info  = 0;
	expected_record.item  = NULL;
	
	CAS(&new_bucket->record, &expected_record, candidate_record);
	
	candidate_record.info = record.info | CROWN_F_MOVED;
	CAS(&bucket->record, &record, candidate_record);
    }
    
    if(CAS(&top->store_current, &self, new_store)) {
        mmm_retire(self);
    }

    return top->store_current;
}

static inline bool
crown_help_required(uint64_t count)
{
    if (count == HATRACK_RETRY_THRESHOLD) {
	return true;
    }
    
    return false;
}


static inline bool
crown_need_to_help(crown_t *self) {
    return (bool)atomic_read(&self->help_needed);
}
