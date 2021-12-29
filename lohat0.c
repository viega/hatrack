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
 *  Name:           lohat0.c
 *  Description:    Linearizeable, Ordered, Wait-free HAsh Table (LOHAT)
 *                  This version never orders, it just sorts when needed.
 *                  Views are fully consistent.
 *
 *  Author:         John Viega, john@zork.org
 */

#include "lohat0.h"

// clang-format off

static lohat0_store_t  *lohat0_store_new         (uint64_t);
static void             lohat0_retire_store      (lohat0_store_t *);
static void            *lohat0_store_get         (lohat0_store_t *, lohat0_t *,
						  hatrack_hash_t *, bool *);
static void            *lohat0_store_put         (lohat0_store_t *, lohat0_t *,
						  hatrack_hash_t *, void *,
						  bool *);
static bool             lohat0_store_put_if_empty(lohat0_store_t *,
						  lohat0_t *,
						  hatrack_hash_t *,
						  void *);
static void            *lohat0_store_remove      (lohat0_store_t *, lohat0_t *,
					          hatrack_hash_t *, bool *);
static lohat0_store_t  *lohat0_store_migrate     (lohat0_store_t *, lohat0_t *);

// clang-format on

void
lohat0_init(lohat0_t *self)
{
    lohat0_store_t *store = lohat0_store_new(HATRACK_MIN_SIZE);

    atomic_store(&self->store_current, store);
}

/* lohat0_get() returns whatever is stored in the item field.
 * Generally, we expect this to be two pointers, a key and a value.
 * Meaning, when the object is NOT in the table, the return value
 * will be the null pointer.
 *
 * When not using values (i.e., a set), it would be reasonable to
 * store values directly, instead of pointers. Thus, the extra
 * optional parameter to get() can tell us whether the item was
 * found or not.  Set it to NULL if you're not interested.
 */
void *
lohat0_get(lohat0_t *self, hatrack_hash_t *hv, bool *found)
{
    void           *ret;
    lohat0_store_t *store;

    mmm_start_basic_op();
    store = atomic_read(&self->store_current);
    ret   = lohat0_store_get(store, self, hv, found);
    mmm_end_op();

    return ret;
}

void *
lohat0_put(lohat0_t *self, hatrack_hash_t *hv, void *item, bool *found)
{
    void           *ret;
    lohat0_store_t *store;

    mmm_start_basic_op();
    store = atomic_read(&self->store_current);
    ret   = lohat0_store_put(store, self, hv, item, found);
    mmm_end_op();

    return ret;
}

bool
lohat0_put_if_empty(lohat0_t *self, hatrack_hash_t *hv, void *item)
{
    bool            ret;
    lohat0_store_t *store;

    mmm_start_basic_op();
    store = atomic_read(&self->store_current);
    ret   = lohat0_store_put_if_empty(store, self, hv, item);
    mmm_end_op();

    return ret;
}

void *
lohat0_remove(lohat0_t *self, hatrack_hash_t *hv, bool *found)
{
    void           *ret;
    lohat0_store_t *store;

    mmm_start_basic_op();
    store = atomic_read(&self->store_current);
    ret   = lohat0_store_remove(store, self, hv, found);
    mmm_end_op();

    return ret;
}

void
lohat0_delete(lohat0_t *self)
{
    lohat0_store_t   *store   = atomic_load(&self->store_current);
    lohat0_history_t *buckets = store->hist_buckets;
    lohat0_history_t *p       = buckets;
    lohat0_history_t *end     = buckets + (store->last_slot + 1);
    lohat_record_t   *rec;

    while (p < end) {
        rec = hatrack_pflag_clear(atomic_load(&p->head),
                                  LOHAT_F_MOVED | LOHAT_F_MOVING);
        if (rec) {
            mmm_retire_unused(rec);
        }
        p++;
    }

    lohat0_retire_store(store);
    free(self);
}

uint64_t
lohat0_len(lohat0_t *self)
{
    return self->store_current->used_count - self->store_current->del_count;
}

hatrack_view_t *
lohat0_view(lohat0_t *self, uint64_t *out_num, bool sort)
{
    lohat0_history_t *cur;
    lohat0_history_t *end;
    lohat0_store_t   *store;
    hatrack_view_t   *view;
    hatrack_view_t   *p;
    hatrack_hash_t    hv;
    lohat_record_t   *rec;
    uint64_t          epoch;
    uint64_t          sort_epoch;
    uint64_t          num_items;

    epoch = mmm_start_linearized_op();
    store = self->store_current;
    cur   = store->hist_buckets;
    end   = cur + (store->last_slot + 1);
    view  = (hatrack_view_t *)malloc(sizeof(hatrack_view_t) * (end - cur));
    p     = view;

    while (cur < end) {
        hv  = atomic_read(&cur->hv);
        rec = hatrack_pflag_clear(atomic_read(&cur->head),
                                  LOHAT_F_MOVING | LOHAT_F_MOVED);

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
            rec = hatrack_pflag_clear(rec->next, LOHAT_F_USED);
        }

        /* If the sort_epoch is larger than the epoch, then no records
         * in this bucket are old enough to be part of the linearization.
         * Similarly, if the top record is a delete record, then the
         * bucket was empty at the linearization point.
         */
        if (!rec || sort_epoch > epoch
            || !hatrack_pflag_test(rec->next, LOHAT_F_USED)) {
            cur++;
            continue;
        }

        p->hv         = hv;
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
        // Unordered buckets should be in random order, so quicksort
        // is a good option.
        qsort(view, num_items, sizeof(hatrack_view_t), hatrack_quicksort_cmp);
    }

    mmm_end_op();

    return view;
}

static lohat0_store_t *
lohat0_store_new(uint64_t size)
{
    lohat0_store_t *store;
    uint64_t        sz;

    sz    = sizeof(lohat0_store_t) + sizeof(lohat0_history_t) * size;
    store = (lohat0_store_t *)mmm_alloc_committed(sz);

    store->last_slot  = size - 1;
    store->threshold  = hatrack_compute_table_threshold(size);
    store->used_count = ATOMIC_VAR_INIT(0);
    store->del_count  = ATOMIC_VAR_INIT(0);
    store->store_next = ATOMIC_VAR_INIT(NULL);

    return store;
}

static void
lohat0_retire_unused_store(lohat0_store_t *self)
{
    mmm_retire_unused(self);
}

static void
lohat0_retire_store(lohat0_store_t *self)
{
    mmm_retire(self);
}

static void *
lohat0_store_get(lohat0_store_t *self,
                 lohat0_t       *top,
                 hatrack_hash_t *hv1,
                 bool           *found)
{
    uint64_t          bix;
    uint64_t          i;
    hatrack_hash_t    hv2;
    lohat0_history_t *bucket;
    lohat_record_t   *head;

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
                               LOHAT_F_MOVING | LOHAT_F_MOVED);
    if (head && hatrack_pflag_test(head->next, LOHAT_F_USED)) {
        if (found) {
            *found = true;
        }
        return head->item;
    }
    goto not_found;
}

static void *
lohat0_store_put(lohat0_store_t *self,
                 lohat0_t       *top,
                 hatrack_hash_t *hv1,
                 void           *item,
                 bool           *found)
{
    void             *ret;
    uint64_t          bix;
    uint64_t          i;
    hatrack_hash_t    hv2;
    lohat0_history_t *bucket;
    lohat_record_t   *head;
    lohat_record_t   *candidate;

    bix = hatrack_bucket_index(hv1, self->last_slot);

    for (i = 0; i < self->last_slot; i++) {
        bucket = &self->hist_buckets[bix];
        hv2    = atomic_read(&bucket->hv);
        if (hatrack_bucket_unreserved(&hv2)) {
            if (LCAS(&bucket->hv, &hv2, *hv1, LOHAT0_CTR_BUCKET_ACQUIRE)) {
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
    self = lohat0_store_migrate(self, top);
    return lohat0_store_put(self, top, hv1, item, found);

found_history_bucket:
    head = atomic_read(&bucket->head);

    if (hatrack_pflag_test(head, LOHAT_F_MOVING)) {
        goto migrate_and_retry;
    }
    candidate       = mmm_alloc(sizeof(lohat_record_t));
    candidate->next = hatrack_pflag_set(head, LOHAT_F_USED);
    candidate->item = item;

    /* Even if we're the winner, we need still to make sure that the
     * previous thread's write epoch got committed (since ours has to
     * be later than theirs). Then, we need to commit our write, and
     * return whatever value was there before, if any.
     *
     * Do this first, so we can attempt to set our create epoch
     * properly before we move our record into place.
     */
    if (head) {
        mmm_help_commit(head);
        if (hatrack_pflag_test(head->next, LOHAT_F_USED)) {
            mmm_copy_create_epoch(candidate, head);
        }
    }
    else {
        if (atomic_fetch_add(&self->used_count, 1) >= self->threshold) {
            mmm_retire_unused(candidate);
            goto migrate_and_retry;
        }
    }

    if (!LCAS(&bucket->head, &head, candidate, LOHAT0_CTR_REC_INSTALL)) {
        /* CAS failed. This is either because a flag got updated
         * (because of a table migration), or because a new record got
         * added first.  In the later case, we act like our write
         * happened, and that we got immediately overwritten, before
         * any read was possible.  We want the caller to delete the
         * item if appropriate, so when found is passed, we return
         * *found = true, and return the item passed in as a result.
         */
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
        if (found) {
            *found = false;
        }
        return NULL;
    }

    // If the previous record was a delete, then we bump down
    // del_count.
    if (!(hatrack_pflag_test(head->next, LOHAT_F_USED))) {
        atomic_fetch_sub(&self->del_count, 1);
        if (found) {
            *found = false;
        }

        ret = NULL;
    }
    else {
        if (found) {
            *found = true;
        }
        ret = head->item;
    }

    // Even though the write committment may have been serviced by
    // someone else, we're still responsible for retiring it ourselves,
    // since we are the ones that overwrote the record.
    mmm_retire(head);

    return ret;
}

static bool
lohat0_store_put_if_empty(lohat0_store_t *self,
                          lohat0_t       *top,
                          hatrack_hash_t *hv1,
                          void           *item)
{
    uint64_t          bix;
    uint64_t          i;
    hatrack_hash_t    hv2;
    lohat0_history_t *bucket;
    lohat_record_t   *head;
    lohat_record_t   *candidate;

    bix = hatrack_bucket_index(hv1, self->last_slot);

    for (i = 0; i < self->last_slot; i++) {
        bucket = &self->hist_buckets[bix];
        hv2    = atomic_read(&bucket->hv);

        if (hatrack_bucket_unreserved(&hv2)) {
            if (LCAS(&bucket->hv, &hv2, *hv1, LOHAT0_CTR_BUCKET_ACQUIRE)) {
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
    self = lohat0_store_migrate(self, top);
    return lohat0_store_put_if_empty(self, top, hv1, item);

found_history_bucket:
    head = atomic_read(&bucket->head);

    if (hatrack_pflag_test(head, LOHAT_F_MOVING)) {
        goto migrate_and_retry;
    }

    if (head) {
        // There's already something in this bucket, and the request was
        // to put only if the bucket is empty.
        if (hatrack_pflag_test(head->next, LOHAT_F_USED)) {
            return false;
        }
    }
    else {
        if (atomic_fetch_add(&self->used_count, 1) >= self->threshold) {
            goto migrate_and_retry;
        }
    }

    /* Right now there's nothing in the bucket, but there might be
     * something in the bucket before we add our item, in which case
     * the CAS will fail. Or, the CAS may fail if the migrating flag
     * got set.  If there is an item there, we return false; if we see
     * a migration in progress, we go off and do that instead.
     */
    candidate       = mmm_alloc(sizeof(lohat_record_t));
    candidate->next = hatrack_pflag_set(head, LOHAT_F_USED);
    candidate->item = item;
    if (!LCAS(&bucket->head, &head, candidate, LOHAT0_CTR_REC_INSTALL)) {
        mmm_retire_unused(candidate);

        if (hatrack_pflag_test(head, LOHAT_F_MOVING)) {
            goto migrate_and_retry;
        }
        return false;
    }

    atomic_fetch_add(&self->used_count, 1);

    if (head) {
        // If there's a previous record, it will be a "delete", so we need
        // still to make sure that the previous thread's write epoch got
        // committed before committing our write.
        atomic_fetch_sub(&self->del_count, 1);
        mmm_help_commit(head);

        mmm_commit_write(candidate);

        if (head) {
            mmm_retire(head);
        }
    }
    else {
        mmm_commit_write(candidate);
    }

    return true;
}

static void *
lohat0_store_remove(lohat0_store_t *self,
                    lohat0_t       *top,
                    hatrack_hash_t *hv1,
                    bool           *found)
{
    uint64_t          bix;
    uint64_t          i;
    hatrack_hash_t    hv2;
    lohat0_history_t *bucket;
    lohat_record_t   *head;
    lohat_record_t   *candidate;

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

    if (hatrack_pflag_test(head, LOHAT_F_MOVING)) {
migrate_and_retry:
        self = lohat0_store_migrate(self, top);
        return lohat0_store_remove(self, top, hv1, found);
    }

    // If !head, then some write hasn't finished.
    if (!head || !(hatrack_pflag_test(head->next, LOHAT_F_USED))) {
        goto empty_bucket;
    }

    /* At this moment, there's an item there to delete. Create a
     * deletion record, and try to add it on. If we "fail", we look at
     * the record that won. If it is itself a deletion, then that
     * record did the delete, and we act like we came in after it.  If
     * it's an overwrite, then the overwrite was responsible for
     * returning the old item for memory management purposes, so we
     * return NULL and set *found to false (if requested), to indicate
     * that there's no memory management work to do.
     */
    candidate       = mmm_alloc(sizeof(lohat_record_t));
    candidate->next = NULL;
    candidate->item = NULL;
    if (!LCAS(&bucket->head, &head, candidate, LOHAT0_CTR_DEL)) {
        mmm_retire_unused(candidate);

        // Moving flag got set before our CAS.
        if (hatrack_pflag_test(head, LOHAT_F_MOVING)) {
            goto migrate_and_retry;
        }
        if (!(hatrack_pflag_test(head->next, LOHAT_F_USED))) {
            // We got beat to the delete;
            goto empty_bucket;
        }
        if (found) {
            *found = true;
        }

        return NULL;
    }

    // Help finish the commit of anything we're overwriting, before we
    // fully commit our write, and then add its retirement epoch.

    mmm_help_commit(head);
    mmm_commit_write(candidate);
    mmm_retire(head);

    if (found) {
        *found = true;
    }

    atomic_fetch_add(&self->del_count, 1);
    return head->item;
}

static lohat0_store_t *
lohat0_store_migrate(lohat0_store_t *self, lohat0_t *top)
{
    lohat0_store_t   *new_store;
    lohat0_store_t   *candidate_store;
    uint64_t          new_size;
    lohat0_history_t *cur;
    lohat0_history_t *bucket;
    lohat_record_t   *head;
    lohat_record_t   *old_head;
    lohat_record_t   *deflagged;
    lohat_record_t   *expected_head;
    hatrack_hash_t    hv;
    hatrack_hash_t    expected_hv;
    uint64_t          i, j;
    uint64_t          bix;
    uint64_t          new_used      = 0;
    uint64_t          expected_used = ~0;

    /* Quickly run through every history bucket, and mark any bucket
     * that doesn't already have F_MOVING set.  Note that the CAS
     * could fail due to some other updater, so we keep CASing until
     * we know it was successful.
     */
    for (i = 0; i <= self->last_slot; i++) {
        cur  = &self->hist_buckets[i];
        head = atomic_read(&cur->head);

        do {
            if (hatrack_pflag_test(head, LOHAT_F_MOVING)) {
                break;
            }
        } while (!LCAS(&cur->head,
                       &head,
                       hatrack_pflag_set(head, LOHAT_F_MOVING),
                       LOHAT0_CTR_F_MOVING));
        deflagged = hatrack_pflag_clear(head, LOHAT_F_MOVING | LOHAT_F_MOVED);
        if (deflagged && hatrack_pflag_test(deflagged->next, LOHAT_F_USED)) {
            new_used++;
        }
    }

    new_store = atomic_read(&self->store_next);

    // If we couldn't acquire a store, try to install one. If we fail, free it.
    if (!new_store) {
        new_size        = hatrack_new_size(self->last_slot, new_used);
        candidate_store = lohat0_store_new(new_size);
        // This helps address a potential race condition, where
        // someone could drain the table after resize, having
        // us swap in the wrong length.
        atomic_store(&candidate_store->used_count, ~0);

        if (!LCAS(&self->store_next,
                  &new_store,
                  candidate_store,
                  LOHAT0_CTR_NEW_STORE)) {
            lohat0_retire_unused_store(candidate_store);
        }
        else {
            new_store = candidate_store;
        }
    }

    // At this point, we're sure that any late writers will help us
    // with the migration. Therefore, we can go through each item,
    // and, if it's not fully migrated, we can attempt to migrate it.
    for (i = 0; i <= self->last_slot; i++) {
        cur      = &self->hist_buckets[i];
        old_head = atomic_read(&cur->head);
        deflagged
            = hatrack_pflag_clear(old_head, LOHAT_F_MOVING | LOHAT_F_MOVED);

        if (!deflagged) {
            if (!(hatrack_pflag_test(old_head, LOHAT_F_MOVED))) {
                LCAS(&cur->head,
                     &old_head,
                     hatrack_pflag_set(old_head, LOHAT_F_MOVED),
                     LOHAT0_CTR_F_MOVED1);
            }
            continue;
        }

        if (hatrack_pflag_test(old_head, LOHAT_F_MOVED)) {
            continue;
        }

        if (!hatrack_pflag_test(deflagged->next, LOHAT_F_USED)) {
            if (LCAS(&cur->head,
                     &old_head,
                     hatrack_pflag_set(old_head, LOHAT_F_MOVED),
                     LOHAT0_CTR_F_MOVED2)) {
                mmm_retire(deflagged);
            }
            continue;
        }

        hv  = atomic_read(&cur->hv);
        bix = hatrack_bucket_index(&hv, new_store->last_slot);

        for (j = 0; j <= new_store->last_slot; j++) {
            bucket         = &new_store->hist_buckets[bix];
            expected_hv.w1 = 0;
            expected_hv.w2 = 0;
            if (!LCAS(&bucket->hv, &expected_hv, hv, LOHAT0_CTR_MIGRATE_HV)) {
                if (!hatrack_hashes_eq(&expected_hv, &hv)) {
                    bix = (bix + 1) & new_store->last_slot;
                    continue;
                }
            }
            break;
        }

        expected_head = NULL;
        LCAS(&bucket->head, &expected_head, deflagged, LOHAT0_CTR_MIG_REC);
        LCAS(&cur->head,
             &old_head,
             hatrack_pflag_set(old_head, LOHAT_F_MOVED),
             LOHAT0_CTR_F_MOVED3);
    }

    LCAS(&new_store->used_count,
         &expected_used,
         new_used,
         LOHAT0_CTR_LEN_INSTALL);

    if (LCAS(&top->store_current, &self, new_store, LOHAT0_CTR_STORE_INSTALL)) {
        lohat0_retire_store(self);
    }

    return new_store;
}
