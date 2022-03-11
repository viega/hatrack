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
 *  Name:           woolhat.c
 *  Description:    Linearizeable, Ordered, Wait-free HAsh Table (WOOLHAT)
 *                  This version never orders, it just sorts when needed.
 *                  Views are fully consistent.
 *
 *                  Note that this table is similar to lohat, but with
 *                  a few changes to make it wait-free. We're going to
 *                  focus comments on those changes; see lohat source
 *                  code for more detail on the overall algorithm.
 *
 *  Author:         John Viega, john@zork.org
 */

#include <hatrack.h>

// clang-format off

// Needs to be non-static because tophat needs it; nonetheless, do not
// export this explicitly; it's effectively a "friend" function not public.
       woolhat_store_t *woolhat_store_new    (uint64_t);
static void            *woolhat_store_get    (woolhat_store_t *, hatrack_hash_t,
					      bool *);
static void            *woolhat_store_put    (woolhat_store_t *, woolhat_t *,
					      hatrack_hash_t, void *, bool *,
					      uint64_t);
static void            *woolhat_store_replace(woolhat_store_t *, woolhat_t *,
					      hatrack_hash_t, void *, bool *,
					      uint64_t);
static bool             woolhat_store_add    (woolhat_store_t *, woolhat_t *,
					      hatrack_hash_t, void *,
					      uint64_t);
static void            *woolhat_store_remove (woolhat_store_t *, woolhat_t *,
					      hatrack_hash_t, bool *,
					      uint64_t);
static woolhat_store_t *woolhat_store_migrate(woolhat_store_t *, woolhat_t *);
static inline bool      woolhat_help_required(uint64_t);
static inline bool      woolhat_need_to_help (woolhat_t *);
static uint64_t         woolhat_set_ordering (woolhat_record_t *, bool);
static inline void      woolhat_new_insertion(woolhat_record_t *);

static uint64_t
woolhat_set_ordering(woolhat_record_t *record, bool deleted_below)
{
    mmm_header_t *mmm_hdr;
    
    mmm_hdr = mmm_get_header(record);
    
    if (mmm_hdr->create_epoch) {
	return mmm_hdr->create_epoch;
    }
    
    if ((!record->next) || deleted_below) {
	mmm_hdr->create_epoch = mmm_hdr->write_epoch;
	return mmm_hdr->create_epoch;
    }

    mmm_hdr->create_epoch = woolhat_set_ordering(record->next, false);
    
    return mmm_hdr->create_epoch;
}

static void
woolhat_new_insertion(woolhat_record_t *record)
{
    mmm_header_t *mmm_hdr;
    
    mmm_hdr = mmm_get_header(record);

    mmm_hdr->create_epoch = mmm_hdr->write_epoch;

    return;
}

// clang-format on

woolhat_t *
woolhat_new(void)
{
    woolhat_t *ret;

    ret = (woolhat_t *)malloc(sizeof(woolhat_t));

    woolhat_init(ret);

    return ret;
}

woolhat_t *
woolhat_new_size(char size)
{
    woolhat_t *ret;

    ret = (woolhat_t *)malloc(sizeof(woolhat_t));

    woolhat_init_size(ret, size);

    return ret;
}

void
woolhat_init(woolhat_t *self)
{
    woolhat_init_size(self, HATRACK_MIN_SIZE_LOG);

    return;
}

void
woolhat_init_size(woolhat_t *self, char size)
{
    woolhat_store_t *store;
    uint64_t         len;

    if (size > ((ssize_t)sizeof(intptr_t) * 8)) {
        abort();
    }

    if (size < HATRACK_MIN_SIZE_LOG) {
        abort();
    }

    len   = 1 << size;
    store = woolhat_store_new(len);

    atomic_store(&self->help_needed, 0);
    atomic_store(&self->item_count, 0);
    atomic_store(&self->store_current, store);

    self->cleanup_func = NULL;
    self->cleanup_aux  = NULL;

    return;
}

void
woolhat_cleanup(woolhat_t *self)
{
    woolhat_store_t   *store;
    woolhat_history_t *buckets;
    woolhat_history_t *p;
    woolhat_history_t *end;
    woolhat_state_t    state;

    store   = atomic_load(&self->store_current);
    buckets = store->hist_buckets;
    p       = buckets;
    end     = buckets + (store->last_slot + 1);

    while (p < end) {
        state = atomic_load(&p->state);
        if (state.head) {
            mmm_retire_unused(state.head);
        }
        p++;
    }

    mmm_retire(store);

    return;
}

void
woolhat_delete(woolhat_t *self)
{
    woolhat_cleanup(self);
    free(self);

    return;
}

/* woolhat_set_cleanup_func()
 *
 * If this is set, we will add the associated function as a handler
 * for when we allocate records via MMM (though not stores).
 *
 * This allows our set implementation to determine when items in the
 * set cannot again be accessed through the woolhat API, allowing us
 * to notify the caller that the hash table is totally done with the
 * item in question (unless re-inserted, of course).
 *
 * That's useful for cases where the set conceptually show "own"
 * memory management of the items contained in it.
 *
 * Such cases are very application-dependent of course.
 *
 * Note that it does not make quite as much sense to have witchhat
 * notify when an item is officially removed from the table, because
 * witchhat does not use mmm on its records, so it does not know in a
 * timely manner when all threads that might be looking at the record
 * are done with the record.
 *
 * We could address that by adding a mmm cleanup callback on stores
 * that triggers a user-callback on every item ejected since the last
 * store migration, but that doesn't seem appropriate.
 *
 * Instead, we made the appropriate 'store' functions non-static so
 * that implementations can use mmm themselves, and apply mmm on the
 * items they store.  See the implementation of hatrack_dict (dict.c)
 * for more detail on what we do there.
 */
void
woolhat_set_cleanup_func(woolhat_t *self, mmm_cleanup_func func, void *aux)
{
    self->cleanup_func = func;
    self->cleanup_aux  = aux;

    return;
}

void *
woolhat_get(woolhat_t *self, hatrack_hash_t hv, bool *found)
{
    void            *ret;
    woolhat_store_t *store;

    mmm_start_basic_op();

    store = atomic_read(&self->store_current);
    ret   = woolhat_store_get(store, hv, found);

    mmm_end_op();

    return ret;
}

void *
woolhat_put(woolhat_t *self, hatrack_hash_t hv, void *item, bool *found)
{
    void            *ret;
    woolhat_store_t *store;

    mmm_start_basic_op();

    store = atomic_read(&self->store_current);
    ret   = woolhat_store_put(store, self, hv, item, found, 0);

    mmm_end_op();

    return ret;
}

void *
woolhat_replace(woolhat_t *self, hatrack_hash_t hv, void *item, bool *found)
{
    void            *ret;
    woolhat_store_t *store;

    mmm_start_basic_op();

    store = atomic_read(&self->store_current);
    ret   = woolhat_store_replace(store, self, hv, item, found, 0);

    mmm_end_op();

    return ret;
}

bool
woolhat_add(woolhat_t *self, hatrack_hash_t hv, void *item)
{
    bool             ret;
    woolhat_store_t *store;

    mmm_start_basic_op();

    store = atomic_read(&self->store_current);
    ret   = woolhat_store_add(store, self, hv, item, 0);

    mmm_end_op();

    return ret;
}

void *
woolhat_remove(woolhat_t *self, hatrack_hash_t hv, bool *found)
{
    void            *ret;
    woolhat_store_t *store;

    mmm_start_basic_op();

    store = atomic_read(&self->store_current);
    ret   = woolhat_store_remove(store, self, hv, found, 0);

    mmm_end_op();

    return ret;
}

uint64_t
woolhat_len(woolhat_t *self)
{
    return atomic_read(&self->item_count);
}

hatrack_view_t *
woolhat_view(woolhat_t *self, uint64_t *out_num, bool sort)
{
    woolhat_history_t *cur;
    woolhat_history_t *end;
    woolhat_store_t   *store;
    hatrack_view_t    *view;
    hatrack_view_t    *p;
    woolhat_state_t    state;
    woolhat_record_t  *rec;
    uint64_t           epoch;
    uint64_t           sort_epoch;
    uint64_t           num_items;

    epoch = mmm_start_linearized_op();
    store = self->store_current;
    cur   = store->hist_buckets;
    end   = cur + (store->last_slot + 1);
    view  = (hatrack_view_t *)malloc(sizeof(hatrack_view_t) * (end - cur));
    p     = view;

    while (cur < end) {
	state = atomic_read(&cur->state);
        rec   = state.head;

        if (rec) {
            mmm_help_commit(rec);
        }

        /* This loop is bounded by the number of writes to this bucket
         * since the call to mmm_start_linearized_op(), which could
         * be large, but not unbounded.
         */
        while (rec) {
            sort_epoch = mmm_get_write_epoch(rec);
            if (sort_epoch <= epoch) {
                break;
            }
            rec = rec->next;
        }

        /* If the sort_epoch is larger than the epoch, then no records
         * in this bucket are old enough to be part of the linearization.
         * Similarly, if the top record is a delete record, then the
         * bucket was empty at the linearization point.
         */
        if (sort_epoch > epoch || !rec || rec->deleted) {
            cur++;
            continue;
        }

        p->item       = rec->item;
        p->sort_epoch = mmm_get_create_epoch(rec);
        p++;
        cur++;
    }

    num_items = p - view;
    *out_num  = num_items;

    if (!num_items) {
        free(view);
        mmm_end_op();

        return NULL;
    }

    view = (hatrack_view_t *)realloc(view, num_items * sizeof(hatrack_view_t));

    if (sort) {
        qsort(view, num_items, sizeof(hatrack_view_t), hatrack_quicksort_cmp);
    }

    mmm_end_op();

    return view;
}

/* woolhat_view_epoch()
 *
 * This version of woolhat_view allows the caller to supply an epoch
 * of interest, for use in set operations, so that we can use the same
 * epoch across multiple sets. However, note that this requires the
 * caller to use the appopriate calls to MMM start/end functions.
 *
 * Note that, unlike woolhat_view, this also plops in the hash value
 * into the output, so that we can do membership tests.
 *
 * And, there's no sort option here; the caller must choose how to
 * sort, and do it herself.
 */
hatrack_set_view_t *
woolhat_view_epoch(woolhat_t *self, uint64_t *out_num, uint64_t epoch)
{
    woolhat_history_t  *cur;
    woolhat_history_t  *end;
    woolhat_store_t    *store;
    hatrack_set_view_t *view;
    hatrack_set_view_t *p;
    woolhat_record_t   *rec;
    woolhat_state_t     state;
    uint64_t            sort_epoch;
    uint64_t            num_items;
    uint64_t            alloc_len;

    store     = self->store_current;
    cur       = store->hist_buckets;
    end       = cur + (store->last_slot + 1);
    alloc_len = sizeof(hatrack_set_view_t);
    view      = (hatrack_set_view_t *)calloc(end - cur, alloc_len);
    p         = view;

    while (cur < end) {
	state = atomic_read(&cur->state);
	rec   = state.head;

        if (rec) {
            mmm_help_commit(rec);
        }

        /* This loop is bounded by the number of writes to this bucket
         * since the call to mmm_start_linearized_op(), which could
         * be large, but not unbounded.
         */
        while (rec) {
            sort_epoch = mmm_get_write_epoch(rec);
            if (sort_epoch <= epoch) {
                break;
            }
            rec = rec->next;
        }

        /* If the sort_epoch is larger than the epoch, then no records
         * in this bucket are old enough to be part of the linearization.
         * Similarly, if the top record is a delete record, then the
         * bucket was empty at the linearization point.
         */
        if (sort_epoch > epoch || !rec || rec->deleted) {
            cur++;
            continue;
        }

        p->hv         = atomic_load(&cur->hv);
        p->item       = rec->item;
        p->sort_epoch = mmm_get_create_epoch(rec);

        p++;
        cur++;
    }

    num_items = p - view;
    *out_num  = num_items;

    if (!num_items) {
        free(view);

        return NULL;
    }

    alloc_len = num_items * sizeof(hatrack_set_view_t);
    view      = (hatrack_set_view_t *)realloc(view, alloc_len);

    return view;
}

woolhat_store_t *
woolhat_store_new(uint64_t size)
{
    woolhat_store_t *store;
    uint64_t         sz;

    sz    = sizeof(woolhat_store_t) + sizeof(woolhat_history_t) * size;
    store = (woolhat_store_t *)mmm_alloc_committed(sz);

    store->last_slot = size - 1;
    store->threshold = hatrack_compute_table_threshold(size);

    return store;
}

static void *
woolhat_store_get(woolhat_store_t *self, hatrack_hash_t hv1, bool *found)
{
    uint64_t           bix;
    uint64_t           i;
    hatrack_hash_t     hv2;
    woolhat_history_t *bucket;
    woolhat_record_t  *head;
    woolhat_state_t    state;

    bix = hatrack_bucket_index(hv1, self->last_slot);

    for (i = 0; i <= self->last_slot; i++) {
        bucket = &self->hist_buckets[bix];
        hv2    = atomic_read(&bucket->hv);

        if (hatrack_bucket_unreserved(hv2)) {
            goto not_found;
        }

        if (!hatrack_hashes_eq(hv1, hv2)) {
            bix = (bix + 1) & self->last_slot;
            continue;
        }

        goto found_history_bucket;
    }
    goto not_found;

found_history_bucket:
    state = atomic_read(&bucket->state);
    head  = state.head;
    
    if (head && !head->deleted) {
	
#ifndef WOOLHAT_DONT_LINEARIZE_GET
	mmm_help_commit(head);
#endif
	
        return hatrack_found(found, head->item);
    }
not_found:
    return hatrack_not_found(found);
}

static void *
woolhat_store_put(woolhat_store_t *self,
                  woolhat_t       *top,
                  hatrack_hash_t   hv1,
                  void            *item,
                  bool            *found,
                  uint64_t         count)
{
    uint64_t           bix;
    uint64_t           i;
    uint64_t           used_count;
    hatrack_hash_t     hv2;
    woolhat_history_t *bucket;
    woolhat_record_t  *head;
    woolhat_record_t  *newhead;
    woolhat_state_t    state;    
    woolhat_state_t    candidate;
    void              *ret;
    bool               deletion_below;

    bix = hatrack_bucket_index(hv1, self->last_slot);

    for (i = 0; i < self->last_slot; i++) {
        bucket = &self->hist_buckets[bix];
        hv2    = atomic_read(&bucket->hv);

        if (hatrack_bucket_unreserved(hv2)) {
            if (CAS(&bucket->hv, &hv2, hv1)) {
                used_count = atomic_fetch_add(&self->used_count, 1);

                if (used_count >= self->threshold) {
                    goto migrate_and_retry;
                }

                goto found_history_bucket;
            }
        }

        if (!hatrack_hashes_eq(hv1, hv2)) {
            bix = (bix + 1) & self->last_slot;
            continue;
        }

        goto found_history_bucket;
    }

migrate_and_retry:
    /* One of the places where tophat is lock-free, instead of
     * wait-free, is when a write operation has to help migrate.  In
     * theory, it could go help migrate, and by the time it tries to
     * write again, it has to participate in the next migration.
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
    if (woolhat_help_required(count)) {
        HATRACK_CTR(HATRACK_CTR_WH_HELP_REQUESTS);

        atomic_fetch_add(&top->help_needed, 1);

        self = woolhat_store_migrate(self, top);
        ret  = woolhat_store_put(self, top, hv1, item, found, count);

        atomic_fetch_sub(&top->help_needed, 1);

        return ret;
    }

    self = woolhat_store_migrate(self, top);
    return woolhat_store_put(self, top, hv1, item, found, count);

found_history_bucket:
    state = atomic_read(&bucket->state);
    head  = state.head;

    if (head) {
        mmm_help_commit(head);
    }

    if (state.flags & WOOLHAT_F_MOVING) {
        goto migrate_and_retry;
    }

    /* If the top node is flagged for deletion help, but the top node
     * is also a deletion node, we don't actually behave any
     * differently, even though conceptually we are ignoring the help
     * request.  That is, our way of 'helping' is by setting our own
     * epoch to our committment epoch, which we also do when we see
     * an unmarked deletion record.
     */
    if ((state.flags & WOOLHAT_F_DELETE_HELP) || (head && head->deleted)) {
	deletion_below = true;
    } else {
	deletion_below = false;
    }
    
    newhead         = mmm_alloc(sizeof(woolhat_record_t));
    newhead->next   = head;
    newhead->item   = item;
    candidate.head  = newhead;
    candidate.flags = 0;

    if (!CAS(&bucket->state, &state, candidate)) {
        mmm_retire_unused(newhead);

        if (state.flags & WOOLHAT_F_MOVING) {
            goto migrate_and_retry;
        }

	return hatrack_found(found, item);
    }

    mmm_commit_write(newhead);
    woolhat_set_ordering(newhead, deletion_below);
    
    
    
    if (top->cleanup_func) {
        mmm_add_cleanup_handler(newhead, top->cleanup_func, top->cleanup_aux);
    }

    if (!head) {
        atomic_fetch_add(&top->item_count, 1);
	return hatrack_not_found(found);
    }

    mmm_retire(head);

    if (deletion_below) {
	/* We 'helped' delete, but we don't need to bump
	 * the length, because we re-inserted in the same breath.
	 */
	if (!(state.flags & WOOLHAT_F_DELETE_HELP)) {
	    atomic_fetch_add(&top->item_count, 1);	
	}
	return hatrack_not_found(found);
    }

    return hatrack_found(found, head->item);
}

static void *
woolhat_store_replace(woolhat_store_t *self,
                      woolhat_t       *top,
                      hatrack_hash_t   hv1,
                      void            *item,
                      bool            *found,
                      uint64_t         count)
{
    uint64_t           bix;
    uint64_t           i;
    hatrack_hash_t     hv2;
    woolhat_history_t *bucket;
    woolhat_record_t  *head;
    woolhat_record_t  *newhead;
    woolhat_state_t    state;
    woolhat_state_t    candidate;
    void              *ret;

    bix = hatrack_bucket_index(hv1, self->last_slot);

    for (i = 0; i < self->last_slot; i++) {
        bucket = &self->hist_buckets[bix];
        hv2    = atomic_read(&bucket->hv);

        if (hatrack_bucket_unreserved(hv2)) {
            goto not_found;
        }

        if (!hatrack_hashes_eq(hv1, hv2)) {
            bix = (bix + 1) & self->last_slot;
            continue;
        }

        goto found_history_bucket;
    }

not_found:
    if (found) {
        *found = false;
    }

    return NULL;

found_history_bucket:
    state = atomic_read(&bucket->state);
    head  = state.head;

    if (!head) {
        goto not_found;
    }

    mmm_help_commit(head);

    /* If the top record is a deletion record, it doesn't matter if
     * there's a migration in progress or not, we can short-circuit.
     */
    if (head->deleted) {
	return hatrack_not_found(found);
    }
    
    if (state.flags & WOOLHAT_F_MOVING) {
migrate_and_retry:
        // This is the same helping mechanism as per above.
        count = count + 1;

        if (woolhat_help_required(count)) {
            HATRACK_CTR(HATRACK_CTR_WH_HELP_REQUESTS);
            atomic_fetch_add(&top->help_needed, 1);

            self = woolhat_store_migrate(self, top);
            ret  = woolhat_store_replace(self, top, hv1, item, found, count);

            atomic_fetch_sub(&top->help_needed, 1);

            return ret;
        }

        self = woolhat_store_migrate(self, top);
        return woolhat_store_replace(self, top, hv1, item, found, count);
    }

    if (state.flags & WOOLHAT_F_DELETE_HELP) {
	/* We need to attempt to help this delete out; we will always
	 * order ourselves immediately after the delete, as long as we
	 * know the delete succeeded before a migration happened.
	 *
	 * If we fail, either there's a migration in progress where
	 * everyone is helping, or someone successfully managed to
	 * install the deletion, in which case we will linearize
	 * ourselves to the successful deletion, meaning we will
	 * return failure.
	 */

	newhead          = mmm_alloc(sizeof(woolhat_record_t));
	newhead->next    = head;
	newhead->deleted = true;
	candidate.head   = newhead;
	candidate.flags  = 0;

	if (!CAS(&bucket->state, &state, candidate)) {
	    if (state.flags & WOOLHAT_F_MOVING) {
		goto migrate_and_retry;
	    }
	    /* Here, we got beaten to installing the delete,
	     * whether it was an explicit deletion via a call to
	     * store_delete, or an implicit call because a put or add
	     * noticed the need for help, and helped.
	     *
	     * Still, we linearize ourselves to the time of the delete.
	     *
	     * That is, if a delete was pending, and our 'replace' and
	     * an 'add' were also competing, the sequence becomes the
	     * 'delete', the 'replace' (which fails), and then the
	     * 'add'.
	     */
	    mmm_retire_unused(newhead);
	}

	return hatrack_not_found(found);
    }

    if (head->deleted) {
        goto not_found;
    }

    newhead         = mmm_alloc(sizeof(woolhat_record_t));
    newhead->next   = head;
    newhead->item   = item;
    candidate.head  = newhead;
    candidate.flags = 0;

    if (!CAS(&bucket->state, &state, candidate)) {
        /* CAS failed. This is either because a flag got updated
         * (because of a table migration), or because a new record got
         * added first.  In the later case, we act like our write
         * happened, and that we got immediately overwritten, before
         * any read was possible.  We want the caller to delete the
         * item if appropriate, so when found is passed, we return
         * *found = true, and return the item passed in as a result.
         *
         * This is a difference from lohat, which loops here if it
         * fails; it's part of how we make woolhat wait-free.
         */
        mmm_retire_unused(newhead);

        if (state.flags & WOOLHAT_F_MOVING) {
            goto migrate_and_retry;
        }

        if (found) {
            *found = true;
        }

        return item;
    }

    mmm_commit_write(newhead);
    woolhat_set_ordering(newhead, false);
    mmm_retire(head);

    if (top->cleanup_func) {
        mmm_add_cleanup_handler(newhead, top->cleanup_func, top->cleanup_aux);
    }

    return hatrack_found(found, head->item);
}

static bool
woolhat_store_add(woolhat_store_t *self,
                  woolhat_t       *top,
                  hatrack_hash_t   hv1,
                  void            *item,
                  uint64_t         count)
{
    uint64_t           bix;
    uint64_t           i;
    uint64_t           used_count;
    hatrack_hash_t     hv2;
    woolhat_history_t *bucket;
    woolhat_record_t  *head;
    woolhat_record_t  *newhead;
    woolhat_state_t    state;
    woolhat_state_t    candidate;

    bix = hatrack_bucket_index(hv1, self->last_slot);

    for (i = 0; i < self->last_slot; i++) {
        bucket = &self->hist_buckets[bix];
        hv2    = atomic_read(&bucket->hv);

        if (hatrack_bucket_unreserved(hv2)) {
            if (CAS(&bucket->hv, &hv2, hv1)) {
                used_count = atomic_fetch_add(&self->used_count, 1);

                if (used_count >= self->threshold) {
                    goto migrate_and_retry;
                }

                goto found_history_bucket;
            }
        }

        if (!hatrack_hashes_eq(hv1, hv2)) {
            bix = (bix + 1) & self->last_slot;
            continue;
        }

        goto found_history_bucket;
    }

migrate_and_retry:
    // This is where we ask for help if needed; see above for details.
    count = count + 1;

    if (woolhat_help_required(count)) {
        bool ret;

        HATRACK_CTR(HATRACK_CTR_WH_HELP_REQUESTS);
        atomic_fetch_add(&top->help_needed, 1);

        self = woolhat_store_migrate(self, top);
        ret  = woolhat_store_add(self, top, hv1, item, count);

        atomic_fetch_sub(&top->help_needed, 1);

        return ret;
    }
    self = woolhat_store_migrate(self, top);
    return woolhat_store_add(self, top, hv1, item, count);

found_history_bucket:
    state = atomic_read(&bucket->state);
    head  = state.head;

    if (head) {
	mmm_help_commit(head);
    }
    
    if (state.flags & WOOLHAT_F_MOVING) {
        goto migrate_and_retry;
    }

    /* If there's a head already, either it needs to be a deletion record,
     * or there needs to be another thread attempting to delete it.
     */ 
    if (head && !(state.flags & WOOLHAT_F_DELETE_HELP) && !head->deleted) {
        return false;
    }

    newhead         = mmm_alloc(sizeof(woolhat_record_t));
    newhead->next   = head;
    newhead->item   = item;
    candidate.head  = newhead;
    candidate.flags = 0;

    if (!CAS(&bucket->state, &state, candidate)) {
        mmm_retire_unused(newhead);
	/* If our 'add' lost, the possible cases are:
	 *
	 * 1) There was a migration, in which case we retry.
	 * 2) We attempted to overwrite a deletion record, in which
	 *    case, someone beat us to it, so we consider the failed CAS
	 *    the linearization point, ordering ourselves after whatever
	 *    overwrote the deletion record, thus returning failure.
	 * 3) The help bit was set (so we were attempting to 'help' via 
	 *    overwriting while skipping the deletion record), but
	 *    some thread managed to overwrite with a deletion record.
	 *    In this case, we linearize ourself BEFORE the deletion, and
	 *    return failure.
	 * 4) The help bit was set, and another thread succeeded by
	 *    adding their own item, but implicitly deleting.
	 *    In this case, we will also linearize ourself BEFORE the
	 *    deletion, and return failure.
	 */


        if (state.flags & WOOLHAT_F_MOVING) {
            goto migrate_and_retry;
        }

        return false;
    }

    atomic_fetch_add(&top->item_count, 1);

    mmm_commit_write     (newhead);
    woolhat_new_insertion(newhead);
    
    if (head) {
        mmm_retire(head);
    }
    
    if (top->cleanup_func) {
        mmm_add_cleanup_handler(newhead, top->cleanup_func, top->cleanup_aux);
    }

    return true;
}

static void *
woolhat_store_remove(woolhat_store_t *self,
                     woolhat_t       *top,
                     hatrack_hash_t   hv1,
                     bool            *found,
                     uint64_t         count)
{
    uint64_t           bix;
    uint64_t           i;
    hatrack_hash_t     hv2;
    woolhat_history_t *bucket;
    woolhat_record_t  *head;
    woolhat_record_t  *newhead;    
    woolhat_state_t    candidate;
    bool               deleting_for_ourselves;
    
    union {
	generic_2x64_u  kludge;
	woolhat_state_t state;
    } state;
    
    bix = hatrack_bucket_index(hv1, self->last_slot);

    for (i = 0; i < self->last_slot; i++) {
        bucket = &self->hist_buckets[bix];
        hv2    = atomic_read(&bucket->hv);

        if (hatrack_bucket_unreserved(hv2)) {
            break;
        }

        if (!hatrack_hashes_eq(hv1, hv2)) {
            bix = (bix + 1) & self->last_slot;
            continue;
        }

        goto found_history_bucket;
    }
    // If run off the loop, or break out of it, the item was not present.
empty_bucket:
    if (found) {
        *found = false;
    }

    return NULL;

found_history_bucket:
    state.state = atomic_read(&bucket->state);
    head        = state.state.head;

    if (head) {
	mmm_help_commit(head);
    }

    if (!head || head->deleted) {
        goto empty_bucket;
    }
    
    if (state.state.flags & WOOLHAT_F_MOVING) {
migrate_and_retry:
        count = count + 1;

        if (woolhat_help_required(count)) {
            void *ret;

            HATRACK_CTR(HATRACK_CTR_WH_HELP_REQUESTS);
            atomic_fetch_add(&top->help_needed, 1);

            self = woolhat_store_migrate(self, top);
            ret  = woolhat_store_remove(self, top, hv1, found, count);

            atomic_fetch_sub(&top->help_needed, 1);

            return ret;
        }

        self = woolhat_store_migrate(self, top);
        return woolhat_store_remove(self, top, hv1, found, count);
    }

    if (state.state.flags & WOOLHAT_F_DELETE_HELP) {
	deleting_for_ourselves = false;
    }
    else {
	deleting_for_ourselves = true;
    }
    
    newhead            = mmm_alloc(sizeof(woolhat_record_t));
    newhead->next      = head;
    newhead->deleted   = true; // ->item is 0'd out by mmm_alloc.
    candidate.head     = newhead;
    candidate.flags    = 0;

    if (!CAS(&bucket->state, &state.state, candidate)) {

        if (state.state.flags & WOOLHAT_F_MOVING) {
	    mmm_retire_unused(newhead);
            goto migrate_and_retry;
        }

	/* If the deletion we were helping occured in another thread,
	 * or if another deletion operation beat us, then we order
	 * ourselves right after that helped deletion.
	 */
	if (!deleting_for_ourselves || state.state.head->deleted) {
	    mmm_retire_unused(newhead);	    
            goto empty_bucket;
        }

	/* Otherwise, our own "deletion" didn't work, because
	 * something else got written here.
	 *
	 * We can either keep retrying (lock-free without a helping
	 * mechanism), or linearize ourselves BEFORE the re-insertion,
	 * in which case we'd have a potential race condition with the
	 * insertion time.  That might be livable for most
	 * applications, but since we're interested in correctness, we
	 * will go ahead and add a help mechanism.
	 *
	 * And since we'll mostly pay the price of our help mechanism
	 * whether or not we're using it, we go ahead and implement it
	 * on the first fail.
	 *
	 * Basically, when we hit our threshold (which again, is just
	 * one fail), we'll set a bit in the head flags, asking the
	 * writes to linearize themselves in around the unseen delete.
	 * 
         * If that item is a 'replace', then it should also try to
         * write a delete out on our behalf.
	 * 
	 * Meanwhile, we'll attempt to install ourselves again, but
	 * only if no write came in and cleared the help bit.
	 *
	 * Note that if we get help deleting, it doesn't linearize until
         * the point where one of our competing threads successfully writes
	 * out a record.  So get operations can ignore the help bit, as
	 * can migrations.
	 */

	state.kludge = OR2X64L(&bucket->state, WOOLHAT_F_DELETE_HELP);

	if (state.state.flags & WOOLHAT_F_DELETE_HELP) {
	    deleting_for_ourselves = false;
	}
	else {
	    state.state.flags |= WOOLHAT_F_DELETE_HELP;
	}

	/* We asked for help, but we raced with another deletion
	 * that actually SUCCEEDED, so we linearize ourselves after
	 * it.  
	 *
	 * Since the head record is a deletion bit, the help flag
	 * will get ignored.
	 */
	if (state.state.head->deleted) {
	    return hatrack_not_found(found);
	}

	newhead->next = state.state.head;

	if (CAS(&bucket->state, &state.state, candidate)) {
	    /* We managed to install a deletion record ourselves.
	     * Whether we succeeded depends on if we were deleting
	     * for ourselves or not.
	     *
	     * But either way, we tick down the item count.
	     */
	    mmm_commit_write(newhead);	    
	    mmm_retire(state.state.head);
	    atomic_fetch_sub(&top->item_count, 1);
	    
	    if (deleting_for_ourselves) {
		return hatrack_found(found, NULL);
	    }
	    return hatrack_not_found(found);
	}
	
	mmm_retire_unused(newhead);

	/* If the CAS failed and the state is the same,
	 * except for the migration bit being set, then
	 * our help request has not been serviced yet.
	 *
	 * Otherwise, it will have been serviced.
	 */
	if ((state.state.flags & WOOLHAT_F_DELETE_HELP) &&
	    (state.state.head == newhead->next) &&
	    (state.state.flags & WOOLHAT_F_MOVING)) {
	    goto migrate_and_retry;
	}

	return hatrack_found(found, NULL);
    }

    // Here, the initial delete was successful.
    mmm_commit_write(newhead);
    mmm_retire(head);
    atomic_fetch_sub(&top->item_count, 1);

    return hatrack_found(found, NULL);
}

static woolhat_store_t *
woolhat_store_migrate(woolhat_store_t *self, woolhat_t *top)
{
    woolhat_store_t   *new_store;
    woolhat_store_t   *candidate_store;
    uint64_t           new_size;
    woolhat_history_t *cur;
    woolhat_history_t *bucket;
    woolhat_state_t    expected_state;
    woolhat_state_t    candidate;
    hatrack_hash_t     hv;
    hatrack_hash_t     expected_hv;
    uint64_t           i, j;
    uint64_t           bix;
    uint64_t           new_used;
    uint64_t           expected_used;
    
    union {
	generic_2x64_u  kludge;
	woolhat_state_t state;
    } state;
    

    new_store = atomic_read(&top->store_current);

    if (new_store != self) {
        return new_store;
    }

    new_used = 0;

    for (i = 0; i <= self->last_slot; i++) {
        cur         = &self->hist_buckets[i];
	state.state = atomic_read(&cur->state);

	if (state.state.flags & WOOLHAT_F_MOVING) {
	    goto skip_some;
	}
	state.kludge = OR2X64L(&cur->state, WOOLHAT_F_MOVING);

	if ((!state.state.head) || state.state.head->deleted) {
	    state.kludge = OR2X64L(&cur->state, WOOLHAT_F_MOVED);

	    /* Only the first thread that successfully sets the MOVED
	     * bit on the state should retire the old record.
	     */
	    if (state.state.head && !(state.state.flags & WOOLHAT_F_MOVED)) {
		mmm_help_commit(state.state.head);
		mmm_retire_fast(state.state.head);
	    }
	    
	    continue;
	}

    skip_some:
        if (state.state.head && !state.state.head->deleted) {
            new_used++;
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
        if (woolhat_need_to_help(top)) {
            new_size = (self->last_slot + 1) << 1;
        }
        else {
            new_size = hatrack_new_size(self->last_slot, new_used);
        }

        candidate_store = woolhat_store_new(new_size);

        if (!CAS(&self->store_next,
                  &new_store,
		 candidate_store)) {
            mmm_retire_unused(candidate_store);
        }
        else {
            new_store = candidate_store;
        }
    }

    for (i = 0; i <= self->last_slot; i++) {
        cur   = &self->hist_buckets[i];
        state.state = atomic_read(&cur->state);
	
	if (state.state.flags & WOOLHAT_F_MOVED) {
	    continue;
	}

        hv  = atomic_read(&cur->hv);
        bix = hatrack_bucket_index(hv, new_store->last_slot);

        for (j = 0; j <= new_store->last_slot; j++) {
            bucket = &new_store->hist_buckets[bix];

            // Set expected_hv to 0.
            hatrack_bucket_initialize(&expected_hv);

            if (!CAS(&bucket->hv, &expected_hv, hv)) {
                if (!hatrack_hashes_eq(expected_hv, hv)) {
                    bix = (bix + 1) & new_store->last_slot;
                    continue;
                }
            }
            break;
        }

        expected_state.head  = NULL;
	expected_state.flags = 0;
        candidate            = state.state;
	candidate.flags      = 0;
	
        CAS(&bucket->state, &expected_state, candidate);
	OR2X64L(&cur->state, WOOLHAT_F_MOVED);
    }

    expected_used = 0;

    CAS(&new_store->used_count, &expected_used, new_used);

    if (CAS(&top->store_current, &self, new_store)) {
        mmm_retire(self);
    }

    return top->store_current;
}

static inline bool
woolhat_help_required(uint64_t count)
{
    if (count == HATRACK_RETRY_THRESHOLD) {
        return true;
    }

    return false;
}

static inline bool
woolhat_need_to_help(woolhat_t *self)
{
    return (bool)atomic_read(&self->help_needed);
}
