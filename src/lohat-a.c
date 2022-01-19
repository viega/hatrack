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
 *  Name:           lohat-a.c
 *  Description:    Linearizeable, Ordered HAsh Table (LOHAT)
 *                  This version keeps two tables, for partial ordering.
 *
 *                  An overview of the algorithm and the motivation
 *                  behind it live in the .h file.
 *
 *  Author:         John Viega, john@zork.org
 *
 */

#include <hatrack.h>

#ifdef HATRACK_COMPILE_ALL_ALGORITHMS

#ifdef SANE_FETCH_ADD_PTR_SEMANTICS
#define fa_ptr_incr(t) (1)
#else
#define fa_ptr_incr(t) (sizeof(t))
#endif

// clang-format off
static lohat_a_store_t *lohat_a_store_new          (uint64_t);
static void             lohat_a_retire_store       (lohat_a_store_t *);
static void             lohat_a_retire_unused_store(lohat_a_store_t *);
static void            *lohat_a_store_get          (lohat_a_store_t *,
						    hatrack_hash_t, bool *);
static void            *lohat_a_store_put          (lohat_a_store_t *,
						    lohat_a_t *,
						    hatrack_hash_t, void *,
						    bool *);
static void            *lohat_a_store_replace      (lohat_a_store_t *,
						    lohat_a_t *,
						    hatrack_hash_t, void *,
						    bool *);
static bool             lohat_a_store_add          (lohat_a_store_t *,
						    lohat_a_t *,
						    hatrack_hash_t, void *);
static void            *lohat_a_store_remove       (lohat_a_store_t *,
						    lohat_a_t *,
						    hatrack_hash_t, bool *);
static lohat_a_store_t *lohat_a_store_migrate      (lohat_a_store_t *,
						    lohat_a_t *);

#ifndef HATRACK_ALWAYS_USE_QSORT
static void             lohat_a_insertion_sort     (hatrack_view_t *, uint64_t);
#endif
// clang-format on

lohat_a_t *
lohat_a_new(void)
{
    lohat_a_t *ret;

    ret = (lohat_a_t *)malloc(sizeof(lohat_a_t));

    lohat_a_init(ret);

    return ret;
}

void
lohat_a_init(lohat_a_t *self)
{
    lohat_a_store_t *store;

    store = lohat_a_store_new(HATRACK_MIN_SIZE);

    atomic_store(&self->item_count, 0);
    atomic_store(&self->store_current, store);

    return;
}

void
lohat_a_cleanup(lohat_a_t *self)
{
    lohat_a_store_t   *store;
    lohat_a_history_t *buckets;
    lohat_a_history_t *p;
    lohat_a_history_t *end;
    lohat_record_t    *rec;

    store   = atomic_load(&self->store_current);
    buckets = store->hist_buckets;
    p       = buckets;
    end     = store->hist_end;

    while (p < end) {
        rec = hatrack_pflag_clear(atomic_load(&p->head),
                                  LOHAT_F_MOVED | LOHAT_F_MOVING);

        if (rec) {
            mmm_retire_unused(rec);
        }
        p++;
    }

    lohat_a_retire_store(store);

    return;
}

void
lohat_a_delete(lohat_a_t *self)
{
    lohat_a_cleanup(self);
    free(self);

    return;
}

void *
lohat_a_get(lohat_a_t *self, hatrack_hash_t hv, bool *found)
{
    void            *ret;
    lohat_a_store_t *store;

    mmm_start_basic_op();

    store = atomic_read(&self->store_current);
    ret   = lohat_a_store_get(store, hv, found);

    mmm_end_op();

    return ret;
}

void *
lohat_a_put(lohat_a_t *self, hatrack_hash_t hv, void *item, bool *found)

{
    void            *ret;
    lohat_a_store_t *store;

    mmm_start_basic_op();

    store = atomic_read(&self->store_current);
    ret   = lohat_a_store_put(store, self, hv, item, found);

    mmm_end_op();

    return ret;
}

void *
lohat_a_replace(lohat_a_t *self, hatrack_hash_t hv, void *item, bool *found)

{
    void            *ret;
    lohat_a_store_t *store;

    mmm_start_basic_op();

    store = atomic_read(&self->store_current);
    ret   = lohat_a_store_replace(store, self, hv, item, found);

    mmm_end_op();

    return ret;
}

bool
lohat_a_add(lohat_a_t *self, hatrack_hash_t hv, void *item)
{
    bool             ret;
    lohat_a_store_t *store;

    mmm_start_basic_op();

    store = atomic_read(&self->store_current);
    ret   = lohat_a_store_add(store, self, hv, item);

    mmm_end_op();

    return ret;
}

void *
lohat_a_remove(lohat_a_t *self, hatrack_hash_t hv, bool *found)
{
    void            *ret;
    lohat_a_store_t *store;

    mmm_start_basic_op();

    store = atomic_read(&self->store_current);
    ret   = lohat_a_store_remove(store, self, hv, found);

    mmm_end_op();

    return ret;
}

uint64_t
lohat_a_len(lohat_a_t *self)
{
    return atomic_read(&self->item_count);
}

hatrack_view_t *
lohat_a_view(lohat_a_t *self, uint64_t *out_num, bool sort)
{
    lohat_a_history_t *cur;
    lohat_a_history_t *end;
    lohat_a_store_t   *store;
    hatrack_view_t    *view;
    hatrack_view_t    *p;
    lohat_record_t    *rec;
    uint64_t           epoch;
    uint64_t           sort_epoch;
    uint64_t           num_items;

    epoch = mmm_start_linearized_op();
    store = self->store_current;
    cur   = store->hist_buckets;
    end   = atomic_read(&store->hist_next);

    if (store->hist_end < end) {
        end = store->hist_end;
    }

    view = (hatrack_view_t *)malloc(sizeof(hatrack_view_t) * (end - cur));
    p    = view;

    while (cur < end) {
        rec = hatrack_pflag_clear(atomic_read(&cur->head),
                                  LOHAT_F_MOVING | LOHAT_F_MOVED);

        if (rec) {
            mmm_help_commit(rec);
        }

        while (rec) {
            sort_epoch = mmm_get_write_epoch(rec);
            if (sort_epoch <= epoch) {
                break;
            }
            rec = rec->next;
        }

        if (!rec || sort_epoch > epoch || rec->deleted) {
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

    view = realloc(view, num_items * sizeof(hatrack_view_t));

    if (sort) {
        /* Since we're keeping history buckets somewhat ordered, an
         * insertion sort could buy us significant time over
         * quicksort.
         *
         * In my early testing, that only seems to be true to a
         * point... at a disappointingly low number of items, the
         * curves meet, and quicksort becomes faster, for reasons I do
         * not yet understand.
         *
         * By the time arrays get to 10s of thousands of items,
         * quicksort is a LOT faster.
         *
         * At some point I'd like to understand why, though it's not
         * too high on my priority list.
         *
         * At the moment, you can hard-code a threshold for list size,
         * where we start applying quicksort, or you can just select
         * one or the other to run, always.
         */
#ifdef HATRACK_QSORT_THRESHOLD
        if (num_items >= HATRACK_QSORT_THRESHOLD) {
            qsort(view,
                  num_items,
                  sizeof(hatrack_view_t),
                  hatrack_quicksort_cmp);
        }
        else {
            lohat_a_insertion_sort(view, num_items);
        }

#elif defined(HATRACK_ALWAYS_USE_QSORT)
        qsort(view, num_items, sizeof(hatrack_view_t), hatrack_quicksort_cmp);
#else
        lohat_a_insertion_sort(view, num_items);
#endif
    }

    mmm_end_op();

    return view;
}

/* Note that, unlike in lohat, we have TWO sets of buckets... the
 * "history" buckets, where change records are kept, and the
 * "indirection" buckets, which point us into the history array.
 *
 * In this table, the indirection buckets are the last item of the
 * struct, and declared as a variable sized array.  The history
 * buckets require an additional allocation, which is done here,
 * after first allocating the store.
 */
// clang-format off

static lohat_a_store_t *
lohat_a_store_new(uint64_t size)
{
    lohat_a_store_t *store;
    uint64_t         alloc_len;
    uint64_t         threshold;

    alloc_len           = sizeof(lohat_a_store_t);
    alloc_len          += sizeof(lohat_a_indirect_t) * size;
    store               = (lohat_a_store_t *)mmm_alloc_committed(alloc_len);
    threshold           = hatrack_compute_table_threshold(size);
    alloc_len           = threshold * sizeof(lohat_a_history_t);
    store->hist_buckets = (lohat_a_history_t *)mmm_alloc_committed(alloc_len);
    store->last_slot    = size - 1;
    store->hist_next    = store->hist_buckets;
    store->hist_end     = store->hist_buckets + threshold;

    return store;
}

static void
lohat_a_retire_store(lohat_a_store_t *self)
{
    mmm_retire(self->hist_buckets);
    mmm_retire(self);

    return;
}

static void
lohat_a_retire_unused_store(lohat_a_store_t *self)
{
    mmm_retire_unused(self->hist_buckets);
    mmm_retire_unused(self);

    return;
}

static void *
lohat_a_store_get(lohat_a_store_t *self,
                  hatrack_hash_t   hv1,
                  bool            *found)
{
    uint64_t            bix;
    uint64_t            i;
    hatrack_hash_t      hv2;
    lohat_a_history_t  *bucket;
    lohat_record_t     *head;
    lohat_a_indirect_t *ptrbucket;

    bix = hatrack_bucket_index(hv1, self->last_slot);

    for (i = 0; i <= self->last_slot; i++) {
        ptrbucket = &self->ptr_buckets[bix];
        hv2       = atomic_read(&ptrbucket->hv);
	
        if (hatrack_bucket_unreserved(hv2)) {
not_found:
            if (found) {
                *found = false;
            }
	    
            return NULL;
        }
	
        if (!hatrack_hashes_eq(hv1, hv2)) {
            bix = (bix + 1) & self->last_slot;
            continue;
        }

        bucket = atomic_read(&ptrbucket->ptr);
        /* The bucket could be reserved via a hash write, but some
         * thread could be stalled on writing out the indirection
         * pointer.
         *
         * If so, there's nothing written yet.
         */

        if (!bucket) {
            goto not_found;
        }
	
        goto found_history_bucket;
    }
    
    goto not_found;

    /* Otherwise, we've found the history bucket, and the rest of the
     * logic should look exactly like with lohat.
     */
found_history_bucket:
    head = hatrack_pflag_clear(atomic_read(&bucket->head),
                               LOHAT_F_MOVING | LOHAT_F_MOVED);
    
    if (head && !head->deleted) {
        if (found) {
            *found = true;
        }
	
        return head->item;
    }
    
    goto not_found;
}

static void *
lohat_a_store_put(lohat_a_store_t *self,
                  lohat_a_t       *top,
                  hatrack_hash_t   hv1,
                  void            *item,
                  bool            *found)
{
    uint64_t            bix;
    uint64_t            i;
    hatrack_hash_t      hv2;
    lohat_a_history_t  *bucket;
    lohat_a_history_t  *new_bucket;
    lohat_record_t     *head;
    lohat_record_t     *candidate;
    lohat_a_indirect_t *ptrbucket;

    bix = hatrack_bucket_index(hv1, self->last_slot);

    for (i = 0; i < self->last_slot; i++) {
        ptrbucket = &self->ptr_buckets[bix];
        hv2       = atomic_read(&ptrbucket->hv);

        if (hatrack_bucket_unreserved(hv2)) {
            if (LCAS(&ptrbucket->hv, &hv2, hv1, LOHATa_CTR_BUCKET_ACQUIRE)) {
                /* Other algorithms count how many buckets are
                 * acquired in the table as part of their resize
                 * metric. We skip that in lohat-a; since we have a
                 * list of history buckets we give out in sequential
                 * order, our threshold calculation is just based on
                 * whether we give out all of those buckets.
                 */
                goto found_ptr_bucket;
            }
        }
	
        if (!hatrack_hashes_eq(hv1, hv2)) {
            bix = (bix + 1) & self->last_slot;
            continue;
        }

found_ptr_bucket:
        /* If we are the first writer, or if there's a writer ahead of
         * us who was slow, both the ptr value and the hash value in
         * the history record may not be set.  For the ptr field, we
         * check to see if it's not set, before trying to "help",
         * especially because we don't want to waste space in the
         * second array, triggering more migrations than we need.
         */
        bucket = atomic_read(&ptrbucket->ptr);
	
        if (!bucket) {
            new_bucket = atomic_fetch_add(&self->hist_next,
					  fa_ptr_incr(lohat_a_history_t));
            /* This is us testing to see if we need to resize; once
             * 'hist_next' advances to 'hist_end', we know we have
             * given out all available ordered slots, which is set to
             * 75% of the total size of the hash table.
             */
            if (new_bucket >= self->hist_end) {
                goto migrate_and_retry;
            }
            /* If someone else installed ptr before we did, then its
             * value will be in bucket.  Otherwise, it will be in
             * new_bucket.
             */
            if (LCAS(&ptrbucket->ptr,
                     &bucket,
                     new_bucket,
                     LOHATa_CTR_PTR_INSTALL)) {
                bucket = new_bucket;
            }
        }

        hv2 = atomic_read(&bucket->hv);
	
        if (hatrack_bucket_unreserved(hv2)) {
            LCAS(&bucket->hv, &hv2, hv1, LOHATa_CTR_HIST_HASH);
        }

        goto found_history_bucket;
    }
migrate_and_retry:
    self = lohat_a_store_migrate(self, top);
    return lohat_a_store_put(self, top, hv1, item, found);

    /* Post-bucket acquisition, everything we do in the bucket remains
       the same as with lohat.
     */
found_history_bucket:
    head = atomic_read(&bucket->head);

    if (hatrack_pflag_test(head, LOHAT_F_MOVING)) {
        goto migrate_and_retry;
    }

    candidate       = mmm_alloc(sizeof(lohat_record_t));
    candidate->next = head;
    candidate->item = item;

    if (head) {
        mmm_help_commit(head);
	
        if (!head->deleted) {
            mmm_copy_create_epoch(candidate, head);
        }
    }

    if (!LCAS(&bucket->head, &head, candidate, LOHATa_CTR_REC_INSTALL)) {
        mmm_retire_unused(candidate);

        if (hatrack_pflag_test(head, LOHAT_F_MOVING)) {
            goto migrate_and_retry;
        }
	
        if (found) {
            *found = true;
        }
	
        return item;
    }

    mmm_commit_write(candidate);

    if (!head) {
not_overwriting:
        atomic_fetch_add(&top->item_count, 1);

        if (found) {
            *found = false;
        }
	
        return NULL;
    }

    mmm_retire(head);

    if (head->deleted) {
        goto not_overwriting;
    }

    if (found) {
        *found = true;
    }

    return head->item;
}

/* See the comments on lohat_a_store_put() for an overview of the
 * bucket acquisition logic. Everything else should be the same as
 * with lohat.
 */
static void *
lohat_a_store_replace(lohat_a_store_t *self,
                      lohat_a_t       *top,
                      hatrack_hash_t   hv1,
                      void            *item,
                      bool            *found)
{
    uint64_t            bix;
    uint64_t            i;
    hatrack_hash_t      hv2;
    lohat_a_history_t  *bucket;
    lohat_record_t     *head;
    lohat_record_t     *candidate;
    lohat_a_indirect_t *ptrbucket;

    bix = hatrack_bucket_index(hv1, self->last_slot);

    for (i = 0; i < self->last_slot; i++) {
        ptrbucket = &self->ptr_buckets[bix];
        hv2       = atomic_read(&ptrbucket->hv);

        if (hatrack_bucket_unreserved(hv2)) {
            goto not_found;
        }
	
        if (!hatrack_hashes_eq(hv1, hv2)) {
            bix = (bix + 1) & self->last_slot;
            continue;
        }

        bucket = atomic_read(&ptrbucket->ptr);
	
        if (!bucket) {
            goto not_found;
        }

        goto found_history_bucket;
    }

not_found:
    if (found) {
        *found = false;
    }
    
    return NULL;

found_history_bucket:
    head = atomic_read(&bucket->head);

    if (!head) {
        goto not_found;
    }

    if (hatrack_pflag_test(head, LOHAT_F_MOVING)) {
migrate_and_retry:
        self = lohat_a_store_migrate(self, top);
	
        return lohat_a_store_replace(self, top, hv1, item, found);
    }
    
    candidate       = mmm_alloc(sizeof(lohat_record_t));
    candidate->next = head;
    candidate->item = item;

    do {
        if (head->deleted) {
            mmm_retire_unused(candidate);
            goto not_found;
        }
	
        if (hatrack_pflag_test(head, LOHAT_F_MOVING)) {
            mmm_retire_unused(candidate);
            goto migrate_and_retry;
        }
	
        mmm_help_commit(head);
        mmm_copy_create_epoch(candidate, head);
    } while (!LCAS(&bucket->head, &head, candidate, LOHATa_CTR_REC_INSTALL));

    mmm_commit_write(candidate);
    mmm_retire(head);

    if (found) {
        *found = true;
    }

    return head->item;
}

/* See the comments on lohat_a_store_put() for an overview of the
 * bucket acquisition logic. Everything else should be the same as
 * with lohat.
 */
static bool
lohat_a_store_add(lohat_a_store_t *self,
                  lohat_a_t       *top,
                  hatrack_hash_t   hv1,
                  void            *item)
{
    uint64_t            bix;
    uint64_t            i;
    hatrack_hash_t      hv2;
    lohat_a_history_t  *bucket;
    lohat_a_history_t  *new_bucket;
    lohat_record_t     *head;
    lohat_record_t     *candidate;
    lohat_a_indirect_t *ptrbucket;

    bix = hatrack_bucket_index(hv1, self->last_slot);

    for (i = 0; i < self->last_slot; i++) {
        ptrbucket = &self->ptr_buckets[bix];
        hv2       = atomic_read(&ptrbucket->hv);

        if (hatrack_bucket_unreserved(hv2)) {
            if (LCAS(&ptrbucket->hv, &hv2, hv1, LOHATa_CTR_BUCKET_ACQUIRE)) {
                goto found_ptr_bucket;
            }
        }
	
        if (!hatrack_hashes_eq(hv1, hv2)) {
            bix = (bix + 1) & self->last_slot;
            continue;
        }

found_ptr_bucket:
        bucket = atomic_read(&ptrbucket->ptr);
        if (!bucket) {
            new_bucket = atomic_fetch_add(&self->hist_next,
					  fa_ptr_incr(lohat_a_history_t));
	    
            if (new_bucket >= self->hist_end) {
                goto migrate_and_retry;
            }
	    
            if (LCAS(&ptrbucket->ptr,
                     &bucket,
                     new_bucket,
                     LOHATa_CTR_PTR_INSTALL)) {
                bucket = new_bucket;
            }
        }

        hv2 = atomic_read(&bucket->hv);
	
        if (hatrack_bucket_unreserved(hv2)) {
            LCAS(&bucket->hv, &hv2, hv1, LOHATa_CTR_HIST_HASH);
        }

        goto found_history_bucket;
    }
migrate_and_retry:
    self = lohat_a_store_migrate(self, top);
    
    return lohat_a_store_add(self, top, hv1, item);

found_history_bucket:
    head = atomic_read(&bucket->head);

    if (hatrack_pflag_test(head, LOHAT_F_MOVING)) {
        goto migrate_and_retry;
    }

    if (head && !head->deleted) {
        return false;
    }

    candidate       = mmm_alloc(sizeof(lohat_record_t));
    candidate->next = head;
    candidate->item = item;
    
    if (!LCAS(&bucket->head, &head, candidate, LOHATa_CTR_REC_INSTALL)) {
        mmm_retire_unused(candidate);

        if (hatrack_pflag_test(head, LOHAT_F_MOVING)) {
            goto migrate_and_retry;
        }
	
        return false;
    }

    atomic_fetch_add(&top->item_count, 1);

    if (head) {
        mmm_help_commit(head);
        mmm_commit_write(candidate);
        mmm_retire(head);
    }
    else {
        mmm_commit_write(candidate);
    }

    return true;
}

/* See the comments on lohat_a_store_get() for an overview of the
 * bucket traversal logic. Everything else should be the same as with
 * lohat.
 */
static void *
lohat_a_store_remove(lohat_a_store_t *self,
                     lohat_a_t       *top,
                     hatrack_hash_t   hv1,
                     bool            *found)
{
    uint64_t            bix;
    uint64_t            i;
    hatrack_hash_t      hv2;
    lohat_a_history_t  *bucket;
    lohat_record_t     *head;
    lohat_record_t     *candidate;
    lohat_a_indirect_t *ptrbucket;

    bix = hatrack_bucket_index(hv1, self->last_slot);

    for (i = 0; i < self->last_slot; i++) {
        ptrbucket = &self->ptr_buckets[bix];
        hv2       = atomic_read(&ptrbucket->hv);
	
        if (hatrack_bucket_unreserved(hv2)) {
            break;
        }

        if (!hatrack_hashes_eq(hv1, hv2)) {
            bix = (bix + 1) & self->last_slot;
            continue;
        }

        bucket = atomic_read(&ptrbucket->ptr);
	
        if (!bucket) {
            break;
        }
	
        hv2 = atomic_read(&bucket->hv);
	
        if (hatrack_bucket_unreserved(hv2)) {
            break;
        }

        goto found_history_bucket;
    }

empty_bucket:
    if (found) {
        *found = false;
    }
    
    return NULL;

found_history_bucket:
    head = atomic_read(&bucket->head);

    if (hatrack_pflag_test(head, LOHAT_F_MOVING)) {
migrate_and_retry:
        self = lohat_a_store_migrate(self, top);
	
        return lohat_a_store_remove(self, top, hv1, found);
    }

    if (!head || head->deleted) {
        goto empty_bucket;
    }

    candidate          = mmm_alloc(sizeof(lohat_record_t));
    candidate->next    = head;
    candidate->item    = NULL;
    candidate->deleted = true;
    
    if (!LCAS(&bucket->head, &head, candidate, LOHATa_CTR_DEL)) {
        mmm_retire_unused(candidate);

        if (hatrack_pflag_test(head, LOHAT_F_MOVING)) {
            goto migrate_and_retry;
        }
	
        if (head->deleted) {
            goto empty_bucket;
        }
	
        if (found) {
            *found = true;
        }

        return NULL;
    }

    if (head) {
        mmm_help_commit(head);
    }
    
    mmm_commit_write(candidate);
    mmm_retire(head);

    if (found) {
        *found = true;
    }

    atomic_fetch_sub(&top->item_count, 1);

    return head->item;
}

static lohat_a_store_t *
lohat_a_store_migrate(lohat_a_store_t *self, lohat_a_t *top)
{
    lohat_a_store_t    *new_store;
    lohat_a_store_t    *candidate_store;
    uint64_t            new_size;
    lohat_a_history_t  *cur;
    lohat_a_history_t  *target;
    lohat_a_history_t  *store_end;
    lohat_record_t     *head;
    lohat_record_t     *candidate;
    lohat_a_indirect_t *ptr_bucket;
    hatrack_hash_t      cur_hv;
    lohat_record_t     *expected_head;
    hatrack_hash_t      expected_hv;
    lohat_a_history_t  *expected_ptr;
    uint64_t            i;
    uint64_t            bix;
    uint64_t            new_used;

    cur       = self->hist_buckets;
    store_end = self->hist_end;
    new_used  = 0;

    /* While there are N buckets in the actual hash table's top-level
     * buckets, the history array has .75N buckets, and we might want
     * to mess around with the threshold function, which would lead to
     * that multiple changing. So instead of using a loop over an
     * array index, we just dump a pointer on every iteration, to be
     * more flexible here (stopping when we get to store_end).
     */
    while (cur < store_end) {
        head = atomic_read(&cur->head);
	
        do {
            if (hatrack_pflag_test(head, LOHAT_F_MOVING)) {
                goto didnt_win;
            }
	    
            if (head && !head->deleted) {
                candidate = hatrack_pflag_set(head, LOHAT_F_MOVING);
            }
            else {
                candidate
                    = hatrack_pflag_set(head, LOHAT_F_MOVING | LOHAT_F_MOVED);
            }
        } while (!LCAS(&cur->head, &head, candidate, LOHATa_CTR_F_MOVING));

        if (head && hatrack_pflag_test(candidate, LOHAT_F_MOVED)) {
            // Then it was a delete record; retire it.
            mmm_help_commit(head);
            mmm_retire(head);
            continue;
        }

didnt_win:
        head = hatrack_pflag_clear(head, LOHAT_F_MOVING | LOHAT_F_MOVED);
	
        if (head && !head->deleted) {
            new_used++;
        }

        cur++;
    }

    new_store = atomic_read(&self->store_next);

    if (!new_store) {
        new_size        = hatrack_new_size(self->last_slot, new_used);
        candidate_store = lohat_a_store_new(new_size);

        if (!LCAS(&self->store_next,
                  &new_store,
                  candidate_store,
                  LOHATa_CTR_NEW_STORE)) {
            lohat_a_retire_unused_store(candidate_store);
        }
        else {
            new_store = candidate_store;
        }
    }

    // Again, we use pointer addition instead of an index to iterate
    // through the buckets.
    cur    = self->hist_buckets;
    target = new_store->hist_buckets;

    while (cur < store_end) {
        head      = atomic_read(&cur->head);
        candidate = hatrack_pflag_clear(head, LOHAT_F_MOVING | LOHAT_F_MOVED);

        /* If someone beat us to the move, and there was an item
         * present, we need to update our index into both of the
         * history bucket lists.
         *
         * If candidate is NULL, there was no deletion record; we
         * don't check head for null, as it will have at least
         * LOHAT_F_MIGRATE set.
         */
        if (hatrack_pflag_test(head, LOHAT_F_MOVED)) {
            if (candidate && !candidate->deleted) {
                target++;
            }
	    
            cur++;
            continue;
        }

        /* At this point, there's something to move, and no thread has
         * finished moving it. So we'll go through all the steps
         * necessary to move it, even though other threads might beat
         * us to any particular step. We do this, because other
         * threads may get suspended, and we want to ensure progress.
         *
         * With our two-tier bucket structure, we first try to write
         * the raw record to the (linearly ordered) history
         * array. After that, we try to write to the pointer
         * indirection bucket.
         *
         * New array starts off zero-initialized. If there's anything else
         * after any specific swap, it means we lost a race.
	 *
	 * hatrack_bucket_initialize() will zero-out expected_hv.
         */
        hatrack_bucket_initialize(&expected_hv);
        expected_head  = NULL;

        cur_hv = atomic_read(&cur->hv);

        LCAS(&target->hv, &expected_hv, cur_hv, LOHATa_CTR_MIGRATE_HV);
        LCAS(&target->head, &expected_head, candidate, LOHATa_CTR_MIG_REC);

        /* The history records are now successfully migrated.  But we
         * still have to claim a bucket in the indirection array, and
         * point it into the ordered array.  This basically works the
         * same way as an "add" operation above, and is how we treat
         * history records in lohat, etc.
         */

        bix = hatrack_bucket_index(cur_hv, new_store->last_slot);

        for (i = 0; i <= new_store->last_slot; i++) {
            ptr_bucket     = &new_store->ptr_buckets[bix];

	    hatrack_bucket_initialize(&expected_hv);
	    
            if (!LCAS(&ptr_bucket->hv,
                      &expected_hv,
                      cur_hv,
                      LOHATa_CTR_MV_IH)) {
                if (!hatrack_hashes_eq(expected_hv, cur_hv)) {
                    bix = (bix + 1) & new_store->last_slot;
                    // Someone else has this bucket; Need to keep probing.
                    continue;
                }
            }
            break;
        }
        /* Attempt to install the pointer that points from the hashed
         * array, so that it points to target.  The only reason this
         * might fail is if another thread helping with the migration
         * succeeded.
         */
        expected_ptr = NULL;
        LCAS(&ptr_bucket->ptr, &expected_ptr, target, LOHATa_CTR_NEW_PTR);

        // Okay, this bucket is properly set up in the destination
        // table.  We need to make sure the old bucket is updated
        // properly, by trying to add LOHAT_F_MOVED.
        LCAS(&cur->head,
             &head,
             hatrack_pflag_set(head, LOHAT_F_MOVED),
             LOHATa_CTR_F_MOVED3);
	
        target++;
        cur++;
    }

    // Now that we've gone through every bucket in the old store, we
    // try to set hist_end in the new store (again, if nobody beat us
    // to it).

    expected_ptr = new_store->hist_buckets;

    LCAS(&new_store->hist_next, &expected_ptr, target, LOHATa_CTR_F_HIST);

    if (LCAS(&top->store_current, &self, new_store, LOHATa_CTR_STORE_INSTALL)) {
        lohat_a_retire_store(self);
    }

    return top->store_current;
}

#ifndef HATRACK_ALWAYS_USE_QSORT
static inline void
lohat_a_insertion_sort(hatrack_view_t *view, uint64_t num_items)
{
    uint64_t       i, j;
    hatrack_view_t swap;

    for (i = 1; i < num_items; i++) {
        swap = view[i];
        for (j = i; j > 0 && swap.sort_epoch < view[j - 1].sort_epoch; j--) {
            view[j] = view[j - 1];
        }
        view[j] = swap;
    }

    return;
}
#endif

#endif
