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

#include "woolhat.h"

// clang-format off

// Needs to be non-static because tophat needs it; nonetheless, do not
// export this explicitly; it's effectively a "friend" function not public.
       woolhat_store_t *woolhat_store_new    (uint64_t);
static void            *woolhat_store_get    (woolhat_store_t *, woolhat_t *,
					      hatrack_hash_t *, bool *);
static void            *woolhat_store_put    (woolhat_store_t *, woolhat_t *,
					      hatrack_hash_t *, void *, bool *,
					      uint64_t);
static void            *woolhat_store_replace(woolhat_store_t *, woolhat_t *,
					      hatrack_hash_t *, void *, bool *,
					      uint64_t);
static bool             woolhat_store_add    (woolhat_store_t *, woolhat_t *,
					      hatrack_hash_t *, void *,
					      uint64_t);
static void            *woolhat_store_remove (woolhat_store_t *, woolhat_t *,
					      hatrack_hash_t *, bool *,
					      uint64_t);
static woolhat_store_t *woolhat_store_migrate(woolhat_store_t *, woolhat_t *);
static inline bool      woolhat_help_required(uint64_t);
static inline bool      woolhat_need_to_help (woolhat_t *);

// clang-format on

void
woolhat_init(woolhat_t *self)
{
    woolhat_store_t *store = woolhat_store_new(HATRACK_MIN_SIZE);

    atomic_store(&self->help_needed, 0);
    atomic_store(&self->item_count, 0);
    atomic_store(&self->store_current, store);
}

void *
woolhat_get(woolhat_t *self, hatrack_hash_t *hv, bool *found)
{
    void            *ret;
    woolhat_store_t *store;

    mmm_start_basic_op();
    store = atomic_read(&self->store_current);
    ret   = woolhat_store_get(store, self, hv, found);
    mmm_end_op();

    return ret;
}

void *
woolhat_put(woolhat_t *self, hatrack_hash_t *hv, void *item, bool *found)
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
woolhat_replace(woolhat_t *self, hatrack_hash_t *hv, void *item, bool *found)
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
woolhat_add(woolhat_t *self, hatrack_hash_t *hv, void *item)
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
woolhat_remove(woolhat_t *self, hatrack_hash_t *hv, bool *found)
{
    void            *ret;
    woolhat_store_t *store;

    mmm_start_basic_op();
    store = atomic_read(&self->store_current);
    ret   = woolhat_store_remove(store, self, hv, found, 0);
    mmm_end_op();

    return ret;
}

void
woolhat_delete(woolhat_t *self)
{
    woolhat_store_t   *store;
    woolhat_history_t *buckets;
    woolhat_history_t *p;
    woolhat_history_t *end;
    woolhat_record_t  *rec;

    store   = atomic_load(&self->store_current);
    buckets = store->hist_buckets;
    p       = buckets;
    end     = buckets + (store->last_slot + 1);

    while (p < end) {
        rec = atomic_load(&p->head);
        if (rec) {
            mmm_retire_unused(rec);
        }
        p++;
    }

    mmm_retire(store);
    free(self);
}

uint64_t
woolhat_len(woolhat_t *self)
{
    return self->item_count;
}

hatrack_view_t *
woolhat_view(woolhat_t *self, uint64_t *out_num, bool sort)
{
    woolhat_history_t *cur;
    woolhat_history_t *end;
    woolhat_store_t   *store;
    hatrack_view_t    *view;
    hatrack_view_t    *p;
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
        rec = hatrack_pflag_clear(atomic_read(&cur->head),
                                  WOOLHAT_F_MOVING | WOOLHAT_F_MOVED);

        // If there's a record, we need to ensure its epoch is updated
        // before we proceed.
        mmm_help_commit(rec);

        /* First, we find the top-most record that's older than (or
         * equal to) the linearization epoch.  At this point, we
         * happily will look under deletions; our goal is to just go
         * back in time until we find the right record.
         */
        while (rec) {
            sort_epoch = mmm_get_write_epoch(rec);
            if (sort_epoch <= epoch) {
                break;
            }
            rec = hatrack_pflag_clear(rec->next, WOOLHAT_F_USED);
        }

        /* If the sort_epoch is larger than the epoch, then no records
         * in this bucket are old enough to be part of the linearization.
         * Similarly, if the top record is a delete record, then the
         * bucket was empty at the linearization point.
         */
        if (!rec || sort_epoch > epoch
            || !hatrack_pflag_test(rec->next, WOOLHAT_F_USED)) {
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
        // Unordered buckets should be in random order, so quicksort
        // is a good option.
        qsort(view, num_items, sizeof(hatrack_view_t), hatrack_quicksort_cmp);
    }

    mmm_end_op();

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
woolhat_store_get(woolhat_store_t *self,
                  woolhat_t       *top,
                  hatrack_hash_t  *hv1,
                  bool            *found)
{
    uint64_t           bix;
    uint64_t           i;
    hatrack_hash_t     hv2;
    woolhat_history_t *bucket;
    woolhat_record_t  *head;

    bix = hatrack_bucket_index(hv1, self->last_slot);

    for (i = 0; i <= self->last_slot; i++) {
        bucket = &self->hist_buckets[bix];
        hv2    = atomic_read(&bucket->hv);
        if (hatrack_bucket_unreserved(&hv2)) {
            goto not_found;
        }
        if (!hatrack_hashes_eq(hv1, &hv2)) {
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
    head = hatrack_pflag_clear(atomic_read(&bucket->head),
                               WOOLHAT_F_MOVING | WOOLHAT_F_MOVED);
    if (head && hatrack_pflag_test(head->next, WOOLHAT_F_USED)) {
        if (found) {
            *found = true;
        }
        return head->item;
    }
    goto not_found;
}

static void *
woolhat_store_put(woolhat_store_t *self,
                  woolhat_t       *top,
                  hatrack_hash_t  *hv1,
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
    woolhat_record_t  *candidate;
    void              *ret;

    bix = hatrack_bucket_index(hv1, self->last_slot);

    for (i = 0; i < self->last_slot; i++) {
        bucket = &self->hist_buckets[bix];
        hv2    = atomic_read(&bucket->hv);
        if (hatrack_bucket_unreserved(&hv2)) {
            if (LCAS(&bucket->hv, &hv2, *hv1, WOOLHAT_CTR_BUCKET_ACQUIRE)) {
                used_count = atomic_fetch_add(&self->used_count, 1);
                if (used_count >= self->threshold) {
                    goto migrate_and_retry;
                }
                goto found_history_bucket;
            }
        }
        if (!hatrack_hashes_eq(hv1, &hv2)) {
            bix = (bix + 1) & self->last_slot;
            continue;
        }
        goto found_history_bucket;
    }

migrate_and_retry:
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
    head = atomic_read(&bucket->head);

    if (hatrack_pflag_test(head, WOOLHAT_F_MOVING)) {
        goto migrate_and_retry;
    }
    candidate       = mmm_alloc(sizeof(woolhat_record_t));
    candidate->next = hatrack_pflag_set(head, WOOLHAT_F_USED);
    candidate->item = item;

    if (head) {
        mmm_help_commit(head);
        if (hatrack_pflag_test(head->next, WOOLHAT_F_USED)) {
            mmm_copy_create_epoch(candidate, head);
        }
    }

    if (!LCAS(&bucket->head, &head, candidate, WOOLHAT_CTR_REC_INSTALL)) {
        mmm_retire_unused(candidate);

        if (hatrack_pflag_test(head, WOOLHAT_F_MOVING)) {
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

    if (!(hatrack_pflag_test(head->next, WOOLHAT_F_USED))) {
        goto not_overwriting;
    }
    if (found) {
        *found = true;
    }
    return head->item;
}

static void *
woolhat_store_replace(woolhat_store_t *self,
                      woolhat_t       *top,
                      hatrack_hash_t  *hv1,
                      void            *item,
                      bool            *found,
                      uint64_t         count)
{
    void              *ret;
    uint64_t           bix;
    uint64_t           i;
    hatrack_hash_t     hv2;
    woolhat_history_t *bucket;
    woolhat_record_t  *head;
    woolhat_record_t  *candidate;

    bix = hatrack_bucket_index(hv1, self->last_slot);

    for (i = 0; i < self->last_slot; i++) {
        bucket = &self->hist_buckets[bix];
        hv2    = atomic_read(&bucket->hv);
        if (hatrack_bucket_unreserved(&hv2)) {
            goto not_found;
        }
        if (!hatrack_hashes_eq(hv1, &hv2)) {
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
    head = atomic_read(&bucket->head);

    if (!head) {
        goto not_found;
    }

    if (hatrack_pflag_test(head, WOOLHAT_F_MOVING)) {
migrate_and_retry:
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

    if (!hatrack_pflag_test(head->next, WOOLHAT_F_USED)) {
        goto not_found;
    }

    candidate       = mmm_alloc(sizeof(woolhat_record_t));
    candidate->next = hatrack_pflag_set(head, WOOLHAT_F_USED);
    candidate->item = item;

    mmm_help_commit(head);
    mmm_copy_create_epoch(candidate, head);

    if (!LCAS(&bucket->head, &head, candidate, WOOLHAT_CTR_REC_INSTALL)) {
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
        mmm_retire_unused(candidate);

        if (hatrack_pflag_test(head, WOOLHAT_F_MOVING)) {
            goto migrate_and_retry;
        }
        if (found) {
            *found = true;
        }
        return item;
    }

    mmm_commit_write(candidate);
    mmm_retire(head);

    if (found) {
        *found = true;
    }

    return head->item;
}

static bool
woolhat_store_add(woolhat_store_t *self,
                  woolhat_t       *top,
                  hatrack_hash_t  *hv1,
                  void            *item,
                  uint64_t         count)
{
    uint64_t           bix;
    uint64_t           i;
    uint64_t           used_count;
    hatrack_hash_t     hv2;
    woolhat_history_t *bucket;
    woolhat_record_t  *head;
    woolhat_record_t  *candidate;

    bix = hatrack_bucket_index(hv1, self->last_slot);

    for (i = 0; i < self->last_slot; i++) {
        bucket = &self->hist_buckets[bix];
        hv2    = atomic_read(&bucket->hv);

        if (hatrack_bucket_unreserved(&hv2)) {
            if (LCAS(&bucket->hv, &hv2, *hv1, WOOLHAT_CTR_BUCKET_ACQUIRE)) {
                used_count = atomic_fetch_add(&self->used_count, 1);
                if (used_count >= self->threshold) {
                    goto migrate_and_retry;
                }
                goto found_history_bucket;
            }
        }
        if (!hatrack_hashes_eq(hv1, &hv2)) {
            bix = (bix + 1) & self->last_slot;
            continue;
        }
        goto found_history_bucket;
    }

migrate_and_retry:
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
    head = atomic_read(&bucket->head);

    if (hatrack_pflag_test(head, WOOLHAT_F_MOVING)) {
        goto migrate_and_retry;
    }

    if (head) {
        if (hatrack_pflag_test(head->next, WOOLHAT_F_USED)) {
            return false;
        }
    }

    candidate       = mmm_alloc(sizeof(woolhat_record_t));
    candidate->next = hatrack_pflag_set(head, WOOLHAT_F_USED);
    candidate->item = item;
    if (!LCAS(&bucket->head, &head, candidate, WOOLHAT_CTR_REC_INSTALL)) {
        mmm_retire_unused(candidate);

        if (hatrack_pflag_test(head, WOOLHAT_F_MOVING)) {
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

static void *
woolhat_store_remove(woolhat_store_t *self,
                     woolhat_t       *top,
                     hatrack_hash_t  *hv1,
                     bool            *found,
                     uint64_t         count)
{
    uint64_t           bix;
    uint64_t           i;
    hatrack_hash_t     hv2;
    woolhat_history_t *bucket;
    woolhat_record_t  *head;
    woolhat_record_t  *candidate;

    bix = hatrack_bucket_index(hv1, self->last_slot);

    for (i = 0; i < self->last_slot; i++) {
        bucket = &self->hist_buckets[bix];
        hv2    = atomic_read(&bucket->hv);
        if (hatrack_bucket_unreserved(&hv2)) {
            break;
        }
        if (!hatrack_hashes_eq(hv1, &hv2)) {
            bix = (bix + 1) & self->last_slot;
            continue;
        }
        if (!bucket->head) {
            // Bucket is empty.
            break;
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
    head = atomic_read(&bucket->head);

    if (hatrack_pflag_test(head, WOOLHAT_F_MOVING)) {
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

    if (!head || !(hatrack_pflag_test(head->next, WOOLHAT_F_USED))) {
        goto empty_bucket;
    }

    candidate       = mmm_alloc(sizeof(woolhat_record_t));
    candidate->next = NULL;
    candidate->item = NULL;
    if (!LCAS(&bucket->head, &head, candidate, WOOLHAT_CTR_DEL)) {
        mmm_retire_unused(candidate);

        // Moving flag got set before our CAS.
        if (hatrack_pflag_test(head, WOOLHAT_F_MOVING)) {
            goto migrate_and_retry;
        }
        if (!(hatrack_pflag_test(head->next, WOOLHAT_F_USED))) {
            // We got beat to the delete;
            goto empty_bucket;
        }
        if (found) {
            *found = true;
        }

        return NULL;
    }

    mmm_help_commit(head);
    mmm_commit_write(candidate);
    mmm_retire(head);

    if (found) {
        *found = true;
    }

    atomic_fetch_sub(&top->item_count, 1);
    return head->item;
}

static woolhat_store_t *
woolhat_store_migrate(woolhat_store_t *self, woolhat_t *top)
{
    woolhat_store_t   *new_store;
    woolhat_store_t   *candidate_store;
    uint64_t           new_size;
    woolhat_history_t *cur;
    woolhat_history_t *bucket;
    woolhat_record_t  *head;
    woolhat_record_t  *candidate;
    woolhat_record_t  *expected_head;
    hatrack_hash_t     hv;
    hatrack_hash_t     expected_hv;
    uint64_t           i, j;
    uint64_t           bix;
    uint64_t           new_used;
    uint64_t           expected_used;

    new_store = atomic_read(&top->store_current);

    if (new_store != self) {
        return new_store;
    }

    new_used = 0;

    /* Quickly run through every history bucket, and mark any bucket
     * that doesn't already have F_MOVING set.  Note that the CAS
     * could fail due to some other updater, so we keep CASing until
     * we know it was successful.
     */
    for (i = 0; i <= self->last_slot; i++) {
        cur  = &self->hist_buckets[i];
        head = atomic_read(&cur->head);

        do {
            if (hatrack_pflag_test(head, WOOLHAT_F_MOVING)) {
                break;
            }
        } while (!LCAS(&cur->head,
                       &head,
                       hatrack_pflag_set(head, WOOLHAT_F_MOVING),
                       WOOLHAT_CTR_F_MOVING));
        head = hatrack_pflag_clear(head, WOOLHAT_F_MOVING | WOOLHAT_F_MOVED);
        if (head && hatrack_pflag_test(head->next, WOOLHAT_F_USED)) {
            new_used++;
        }
    }

    new_store = atomic_read(&self->store_next);

    if (!new_store) {
        if (woolhat_need_to_help(top)) {
            new_size = (self->last_slot + 1) << 1;
        }
        else {
            new_size = hatrack_new_size(self->last_slot, new_used);
        }
        candidate_store = woolhat_store_new(new_size);

        if (!LCAS(&self->store_next,
                  &new_store,
                  candidate_store,
                  WOOLHAT_CTR_NEW_STORE)) {
            mmm_retire_unused(candidate_store);
        }
        else {
            new_store = candidate_store;
        }
    }

    for (i = 0; i <= self->last_slot; i++) {
        cur  = &self->hist_buckets[i];
        head = atomic_read(&cur->head);
        candidate
            = hatrack_pflag_clear(head, WOOLHAT_F_MOVING | WOOLHAT_F_MOVED);

        if (hatrack_pflag_test(head, WOOLHAT_F_MOVED)) {
            continue;
        }

        if (!candidate) {
            LCAS(&cur->head,
                 &head,
                 hatrack_pflag_set(head, WOOLHAT_F_MOVED),
                 WOOLHAT_CTR_F_MOVED1);
            continue;
        }

        if (!hatrack_pflag_test(candidate->next, WOOLHAT_F_USED)) {
            if (LCAS(&cur->head,
                     &head,
                     hatrack_pflag_set(head, WOOLHAT_F_MOVED),
                     WOOLHAT_CTR_F_MOVED2)) {
                mmm_help_commit(candidate);
                mmm_retire(candidate);
            }
            continue;
        }

        hv  = atomic_read(&cur->hv);
        bix = hatrack_bucket_index(&hv, new_store->last_slot);

        for (j = 0; j <= new_store->last_slot; j++) {
            bucket         = &new_store->hist_buckets[bix];
            expected_hv.w1 = 0;
            expected_hv.w2 = 0;
            if (!LCAS(&bucket->hv, &expected_hv, hv, WOOLHAT_CTR_MIGRATE_HV)) {
                if (!hatrack_hashes_eq(&expected_hv, &hv)) {
                    bix = (bix + 1) & new_store->last_slot;
                    continue;
                }
            }
            break;
        }

        expected_head = NULL;
        LCAS(&bucket->head, &expected_head, candidate, WOOLHAT_CTR_MIG_REC);
        LCAS(&cur->head,
             &head,
             hatrack_pflag_set(head, WOOLHAT_F_MOVED),
             WOOLHAT_CTR_F_MOVED3);
    }

    expected_used = 0;

    LCAS(&new_store->used_count,
         &expected_used,
         new_used,
         WOOLHAT_CTR_LEN_INSTALL);

    if (LCAS(&top->store_current,
             &self,
             new_store,
             WOOLHAT_CTR_STORE_INSTALL)) {
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
