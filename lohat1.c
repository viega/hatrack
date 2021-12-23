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
 *  Name:           lohat1.c
 *  Description:    Linearizeable, Ordered HAsh Table (LOHAT)
 *                  This version keeps two tables, for partial ordering.
 *
 *  Author:         John Viega, john@zork.org
 *
 */

#include "lohat1.h"

// clang-format off

static lohat1_store_t *lohat1_store_new(uint64_t);
static void             lohat1_retire_store(lohat1_store_t *);
static void             lohat1_retire_unused_store(lohat1_store_t *);
static void            *lohat1_store_get(lohat1_store_t *, lohat1_t *,
					  hatrack_hash_t *, bool *);
static void            *lohat1_store_put(lohat1_store_t *, lohat1_t *,
					  hatrack_hash_t *, void *, bool *);
static bool             lohat1_store_put_if_empty(lohat1_store_t *,
						   lohat1_t *,
						   hatrack_hash_t *, void *);
static void            *lohat1_store_remove(lohat1_store_t *, lohat1_t *,
					     hatrack_hash_t *, bool *);
static lohat1_store_t *lohat1_store_migrate(lohat1_store_t *, lohat1_t *);
static hatrack_view_t  *lohat1_store_view(lohat1_store_t *, lohat1_t *,
					   uint64_t, uint64_t *);

#if !defined(HATRACK_DONT_SORT) && !defined(HATRACK_ALWAYS_USE_QSORT)
static void             lohat1_insertion_sort(hatrack_view_t *, uint64_t);
#endif

// clang-format on

void
lohat1_init(lohat1_t *self)
{
    lohat1_store_t *store = lohat1_store_new(HATRACK_MIN_SIZE);

    atomic_store(&self->store_current, store);
}

/* lohat1_get() returns whatever is stored in the item field.
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
lohat1_get(lohat1_t *self, hatrack_hash_t *hv, bool *found)
{
    void *ret;

    mmm_start_basic_op();
    ret = lohat1_store_get(self->store_current, self, hv, found);
    mmm_end_op();

    return ret;
}

void *
lohat1_put(lohat1_t       *self,
           hatrack_hash_t *hv,
           void           *item,
           bool            ifempty,
           bool           *found)
{
    void *ret;
    bool  bool_ret;

    mmm_start_basic_op();
    if (ifempty) {
        bool_ret
            = lohat1_store_put_if_empty(self->store_current, self, hv, item);
        mmm_end_op();

        return (void *)bool_ret;
    }

    ret = lohat1_store_put(self->store_current, self, hv, item, found);
    mmm_end_op();

    return ret;
}

void *
lohat1_remove(lohat1_t *self, hatrack_hash_t *hv, bool *found)
{
    void *ret;

    mmm_start_basic_op();
    ret = lohat1_store_remove(self->store_current, self, hv, found);
    mmm_end_op();

    return ret;
}

void
lohat1_delete(lohat1_t *self)
{
    lohat1_store_t   *store   = atomic_load(&self->store_current);
    lohat1_history_t *buckets = store->hist_buckets;
    lohat1_history_t *p       = buckets;
    lohat1_history_t *end     = store->hist_end;
    lohat_record_t   *rec;

    while (p < end) {
        rec = hatrack_pflag_clear(atomic_load(&p->head),
                                  LOHAT_F_MOVED | LOHAT_F_MOVING);
        if (rec) {
            mmm_retire_unused(rec);
        }
        p++;
    }

    lohat1_retire_store(store);
    free(self);
}

uint64_t
lohat1_len(lohat1_t *self)
{
    uint64_t diff;

    /* Remember that pointer subtraction implicity divides
     * the result by the number of objects, so this
     * gives us the number of buckets in the history store
     * that have been claimed.
     */
    diff = atomic_load(&self->store_current->hist_next)
         - self->store_current->hist_buckets;

    // Subtract out buckets known to be empty.
    return diff - atomic_load(&self->store_current->del_count);
}

hatrack_view_t *
lohat1_view(lohat1_t *self, uint64_t *num_items)
{
    hatrack_view_t *ret;
    uint64_t        epoch;

    epoch = mmm_start_linearized_op();
    ret   = lohat1_store_view(self->store_current, self, epoch, num_items);
    mmm_end_op();

    return ret;
}

static lohat1_store_t *
lohat1_store_new(uint64_t size)
{
    lohat1_store_t *store;

    store = (lohat1_store_t *)mmm_alloc_committed(sizeof(lohat1_store_t));

    store->last_slot    = size - 1;
    store->threshold    = hatrack_compute_table_threshold(size);
    store->del_count    = ATOMIC_VAR_INIT(0);
    store->hist_buckets = (lohat1_history_t *)mmm_alloc_committed(
        sizeof(lohat1_history_t) * size);
    store->store_next  = ATOMIC_VAR_INIT(NULL);
    store->ptr_buckets = (lohat1_indirect_t *)mmm_alloc_committed(
        sizeof(lohat1_indirect_t) * size);
    store->hist_end
        = store->hist_buckets + hatrack_compute_table_threshold(size);
    store->hist_next = store->hist_buckets;

    return store;
}

static void
lohat1_retire_store(lohat1_store_t *self)
{
    mmm_retire(self->ptr_buckets);
    mmm_retire(self->hist_buckets);
    mmm_retire(self);
}

static void
lohat1_retire_unused_store(lohat1_store_t *self)
{
    mmm_retire_unused(self->ptr_buckets);
    mmm_retire_unused(self->hist_buckets);
    mmm_retire_unused(self);
}

static void *
lohat1_store_get(lohat1_store_t *self,
                 lohat1_t       *top,
                 hatrack_hash_t *hv1,
                 bool           *found)
{
    uint64_t           bix = hatrack_bucket_index(hv1, self->last_slot);
    uint64_t           i;
    hatrack_hash_t     hv2;
    lohat1_history_t  *bucket;
    lohat_record_t    *head;
    lohat1_indirect_t *ptrbucket;

    for (i = 0; i <= self->last_slot; i++) {
        ptrbucket = &self->ptr_buckets[bix];
        hv2       = atomic_load(&ptrbucket->hv);
        if (hatrack_bucket_unreserved(&hv2)) {
not_found:
            if (found) {
                *found = false;
            }
            return NULL;
        }
        if (!hatrack_hashes_eq(hv1, &hv2)) {
            bix = (bix + 1) & self->last_slot;
            continue;
        }

        bucket = atomic_load(&ptrbucket->ptr);
        // It's possible that another thread has started a write but
        // has not finished it yet.
        if (!bucket) {
            goto not_found;
        }
        goto found_history_bucket;
    }
    goto not_found;

found_history_bucket:
    head = hatrack_pflag_clear(atomic_load(&bucket->head),
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
lohat1_store_put(lohat1_store_t *self,
                 lohat1_t       *top,
                 hatrack_hash_t *hv1,
                 void           *item,
                 bool           *found)
{
    void              *ret;
    uint64_t           bix = hatrack_bucket_index(hv1, self->last_slot);
    uint64_t           i;
    hatrack_hash_t     hv2;
    lohat1_history_t  *bucket;
    lohat1_history_t  *new_bucket;
    lohat_record_t    *head;
    lohat_record_t    *candidate;
    lohat1_indirect_t *ptrbucket;

    for (i = 0; i < self->last_slot; i++) {
        ptrbucket = &self->ptr_buckets[bix];
        hv2.w1    = 0;
        hv2.w2    = 0;
        if (!LCAS(&ptrbucket->hv, &hv2, *hv1, LOHAT1_CTR_BUCKET_ACQUIRE)) {
            if (!hatrack_hashes_eq(hv1, &hv2)) {
                bix = (bix + 1) & self->last_slot;
                continue;
            }
        }
        /* If we are the first writer, or if there's a writer ahead of
         * us who was slow, both the ptr value and the hash value in
         * the history record may not be set.  For the ptr field, we
         * check to see if it's not set, before trying to "help",
         * because we don't want to waste space in the second
         * array. However, with the hv, we always just try to write
         * it.
         */
        bucket = atomic_load(&ptrbucket->ptr);
        if (!bucket) {
            new_bucket = atomic_fetch_add(&self->hist_next, 1);
            if (new_bucket >= self->hist_end) {
                return lohat1_store_put(lohat1_store_migrate(self, top),
                                        top,
                                        hv1,
                                        item,
                                        found);
            }
            // If someone else installed ptr before we did, then its
            // value will be in bucket.  Otherwise, it will be in
            // new_bucket.
            if (LCAS(&ptrbucket->ptr,
                     &bucket,
                     new_bucket,
                     LOHAT1_CTR_PTR_INSTALL)) {
                bucket = new_bucket;
            }
            else {
                atomic_fetch_add(&self->del_count, 1);
            }
        }
        // Now try to write out the hash value, without bothering to
        // check if it needs to be written.
        hv2.w1 = 0;
        hv2.w2 = 0;
        LCAS(&bucket->hv, &hv2, *hv1, LOHAT1_CTR_HIST_HASH);

        goto found_history_bucket;
    }
    return lohat1_store_put(lohat1_store_migrate(self, top),
                            top,
                            hv1,
                            item,
                            found);

found_history_bucket:
    head = atomic_load(&bucket->head);

    if (hatrack_pflag_test(head, LOHAT_F_MOVING)) {
        return lohat1_store_put(lohat1_store_migrate(self, top),
                                top,
                                hv1,
                                item,
                                found);
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
            mmm_set_create_epoch(candidate, mmm_get_create_epoch(head));
        }
    }

    if (!LCAS(&bucket->head, &head, candidate, LOHAT1_CTR_REC_INSTALL)) {
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
            return lohat1_store_put(lohat1_store_migrate(self, top),
                                    top,
                                    hv1,
                                    item,
                                    found);
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
lohat1_store_put_if_empty(lohat1_store_t *self,
                          lohat1_t       *top,
                          hatrack_hash_t *hv1,
                          void           *item)
{
    uint64_t           bix = hatrack_bucket_index(hv1, self->last_slot);
    uint64_t           i;
    hatrack_hash_t     hv2;
    lohat1_history_t  *bucket;
    lohat1_history_t  *new_bucket;
    lohat_record_t    *head;
    lohat_record_t    *candidate;
    lohat1_indirect_t *ptrbucket;

    for (i = 0; i < self->last_slot; i++) {
        ptrbucket = &self->ptr_buckets[bix];
        hv2.w1    = 0;
        hv2.w2    = 0;
        if (!LCAS(&ptrbucket->hv, &hv2, *hv1, LOHAT1_CTR_BUCKET_ACQUIRE)) {
            if (!hatrack_hashes_eq(hv1, &hv2)) {
                bix = (bix + 1) & self->last_slot;
                continue;
            }
        }
        /* If we are the first writer, or if there's a writer ahead of
         * us who was slow, both the ptr value and the hash value in
         * the history record may not be set.  For the ptr field, we
         * check to see if it's not set, before trying to "help",
         * because we don't want to waste space in the second
         * array. However, with the hv, we always just try to write
         * it.
         */
        bucket = atomic_load(&ptrbucket->ptr);
        if (!bucket) {
            new_bucket = atomic_fetch_add(&self->hist_next, 1);
            if (new_bucket >= self->hist_end) {
                return lohat1_store_put_if_empty(
                    lohat1_store_migrate(self, top),
                    top,
                    hv1,
                    item);
            }
            // If someone else installed ptr before we did, then
            // its value will be in bucket.  Otherwise, it will
            // be in new_bucket.
            if (LCAS(&ptrbucket->ptr,
                     &bucket,
                     new_bucket,
                     LOHAT1_CTR_PTR_INSTALL)) {
                bucket = new_bucket;
            }
        }
        // Now try to write out the hash value, without bothering to
        // check if it needs to be written.
        hv2.w1 = 0;
        hv2.w2 = 0;
        LCAS(&bucket->hv, &hv2, *hv1, LOHAT1_CTR_HIST_HASH);

        goto found_history_bucket;
    }
    return lohat1_store_put_if_empty(lohat1_store_migrate(self, top),
                                     top,
                                     hv1,
                                     item);

found_history_bucket:
    head = atomic_load(&bucket->head);

    if (hatrack_pflag_test(head, LOHAT_F_MOVING)) {
        return lohat1_store_put_if_empty(lohat1_store_migrate(self, top),
                                         top,
                                         hv1,
                                         item);
    }

    // There's already something in this bucket, and the request was
    // to put only if the bucket is empty.
    if (hatrack_pflag_test(head->next, LOHAT_F_USED)) {
        return false;
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
    if (!LCAS(&bucket->head, &head, candidate, LOHAT1_CTR_REC_INSTALL)) {
        mmm_retire_unused(candidate);

        if (hatrack_pflag_test(head, LOHAT_F_MOVING)) {
            return lohat1_store_put_if_empty(lohat1_store_migrate(self, top),
                                             top,
                                             hv1,
                                             item);
        }
        return false;
    }

    if (head) {
        // If there's a previous record, it will be a "delete", so we need
        // still to make sure that the previous thread's write epoch got
        // committed before committing our write.
        atomic_fetch_sub(&self->del_count, 1);
        mmm_help_commit(head);

        mmm_commit_write(candidate);

        if (head->next) {
            mmm_retire(head);
        }
    }
    else {
        mmm_commit_write(candidate);
    }

    return true;
}

static void *
lohat1_store_remove(lohat1_store_t *self,
                    lohat1_t       *top,
                    hatrack_hash_t *hv1,
                    bool           *found)
{
    uint64_t           bix = hatrack_bucket_index(hv1, self->last_slot);
    uint64_t           i;
    hatrack_hash_t     hv2;
    lohat1_history_t  *bucket;
    lohat_record_t    *head;
    lohat_record_t    *candidate;
    lohat1_indirect_t *ptrbucket;

    for (i = 0; i < self->last_slot; i++) {
        ptrbucket = &self->ptr_buckets[bix];
        hv2       = atomic_load(&ptrbucket->hv);
        if (hatrack_bucket_unreserved(&hv2)) {
            break;
        }

        if (!hatrack_hashes_eq(hv1, &hv2)) {
            bix = (bix + 1) & self->last_slot;
            continue;
        }

        bucket = atomic_load(&ptrbucket->ptr);
        if (!bucket) {
            break;
        }
        // Now try to write out the hash value, without bothering to
        // check if it needs to be written.
        hv2.w1 = 0;
        hv2.w2 = 0;
        LCAS(&bucket->hv, &hv2, *hv1, LOHAT1_CTR_BUCKET_ACQUIRE);

        goto found_history_bucket;
    }
    // If run off the loop, or break out of it, the item was not present.
empty_bucket:
    if (found) {
        *found = false;
    }
    return NULL;

found_history_bucket:
    head = atomic_load(&bucket->head);

    if (hatrack_pflag_test(head, LOHAT_F_MOVING)) {
        return lohat1_store_remove(lohat1_store_migrate(self, top),
                                   top,
                                   hv1,
                                   found);
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
    if (!LCAS(&bucket->head, &head, candidate, LOHAT1_CTR_DEL)) {
        mmm_retire_unused(candidate);

        // Moving flag got set before our CAS.
        if (hatrack_pflag_test(head, LOHAT_F_MOVING)) {
            return lohat1_store_remove(lohat1_store_migrate(self, top),
                                       top,
                                       hv1,
                                       found);
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

static lohat1_store_t *
lohat1_store_migrate(lohat1_store_t *self, lohat1_t *top)
{
    lohat1_store_t    *new_store;
    lohat1_store_t    *candidate;
    uint64_t           new_size;
    lohat1_history_t  *cur;
    lohat1_history_t  *target;
    lohat1_history_t  *store_end;
    lohat_record_t    *old_head;
    lohat_record_t    *deflagged;     // Head w/ migration flags removed.
    lohat1_indirect_t *ptr_bucket;    // New unordered bucket location.
    hatrack_hash_t     cur_hv;        // Hash value of the record to migrate.
    lohat_record_t    *expected_head; // Expected values in new table are NULL.
    hatrack_hash_t     expected_hv;
    lohat1_history_t  *expected_ptr;
    uint64_t           i;
    uint64_t           bix;
    uint64_t           new_used = 0;

    /* Quickly run through every history bucket, and mark any bucket
     * that doesn't already have F_MOVING set.  Note that the CAS
     * could fail due to some other updater, so we keep CASing until
     * we know it was successful.
     */
    cur       = self->hist_buckets;
    store_end = self->hist_end;

    while (cur < store_end) {
        old_head = atomic_load(&cur->head);
        do {
            if (hatrack_pflag_test(old_head, LOHAT_F_MOVING)) {
                break;
            }
        } while (!LCAS(&cur->head,
                       &old_head,
                       hatrack_pflag_set(old_head, LOHAT_F_MOVING),
                       LOHAT1_CTR_F_MOVING));
        deflagged
            = hatrack_pflag_clear(old_head, LOHAT_F_MOVING | LOHAT_F_MOVED);
        if (deflagged && hatrack_pflag_test(deflagged->next, LOHAT_F_USED)) {
            new_used++;
        }

        cur++;
    }

    new_store = atomic_load(&self->store_next);

    // If we couldn't acquire a store, try to install one. If we fail, free it.
    if (!new_store) {
        new_size  = hatrack_new_size(self->last_slot, new_used);
        candidate = lohat1_store_new(new_size);

        if (!LCAS(&self->store_next,
                  &new_store,
                  candidate,
                  LOHAT1_CTR_NEW_STORE)) {
            lohat1_retire_unused_store(candidate);
        }
        else {
            new_store = candidate;
        }
    }

    // At this point, we're sure that any late writers will help us
    // with the migration. Therefore, we can go through each item,
    // and, if it's not fully migrated, we can attempt to migrate it.

    cur    = self->hist_buckets;
    target = new_store->hist_buckets;

    while (cur < store_end) {
        old_head = atomic_load(&cur->head);
        deflagged
            = hatrack_pflag_clear(old_head, LOHAT_F_MOVING | LOHAT_F_MOVED);

        // If there was not an old record in this bucket, then we race
        // to set LOHAT_F_MOVED and go on.
        if (!deflagged) {
            if (!(hatrack_pflag_test(old_head, LOHAT_F_MOVED))) {
                LCAS(&cur->head,
                     &old_head,
                     hatrack_pflag_set(old_head, LOHAT_F_MOVED),
                     LOHAT1_CTR_F_MOVED1);
            }
            cur++;
            continue;
        }

        // If someone beat us to the move, and there was an item present,
        // we need to update our index into both bucket lists.
        if (hatrack_pflag_test(old_head, LOHAT_F_MOVED)) {
            if (hatrack_pflag_test(deflagged->next, LOHAT_F_USED)) {
                target++;
            }
            cur++;
            continue;
        }

        // If the record is a delete record, then try to set the moved
        // flag. If we win, then we retire the old delete record.
        if (!hatrack_pflag_test(deflagged->next, LOHAT_F_USED)) {
            if (LCAS(&cur->head,
                     &old_head,
                     hatrack_pflag_set(old_head, LOHAT_F_MOVED),
                     LOHAT1_CTR_F_MOVED2)) {
                mmm_retire(deflagged);
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
         * New array starts off zero-initialized. If there's anything else
         * after any specific swap, it means we lost a race.
         */
        expected_hv.w1 = 0;
        expected_hv.w2 = 0;
        expected_head  = NULL;

        cur_hv = atomic_load(&cur->hv);

        LCAS(&target->hv, &expected_hv, cur_hv, LOHAT1_CTR_MIGRATE_HV);
        LCAS(&target->head, &expected_head, deflagged, LOHAT1_CTR_MIG_REC);

        // The history records are now successfully migrated.  But we
        // still have to claim a bucket in the indirection array, and
        // point it into the ordered array.

        bix = hatrack_bucket_index(&cur_hv, new_store->last_slot);

        for (i = 0; i <= new_store->last_slot; i++) {
            ptr_bucket     = &new_store->ptr_buckets[bix];
            expected_hv.w1 = 0;
            expected_hv.w2 = 0;
            if (!LCAS(&ptr_bucket->hv,
                      &expected_hv,
                      cur_hv,
                      LOHAT1_CTR_MV_IH)) {
                if (!hatrack_hashes_eq(&expected_hv, &cur_hv)) {
                    bix = (bix + 1) & new_store->last_slot;
                    // Someone else has this bucket; Need to keep probing.
                    continue;
                }
            }
            break; // Bucket is claimed.
        }
        /* Attempt to install the pointer that points from the hashed
         * array, so that it points to target.  The only reason this
         * might fail is if another thread helping with the migration
         * succeeded.
         */
        expected_ptr = NULL;
        LCAS(&ptr_bucket->ptr, &expected_ptr, target, LOHAT1_CTR_NEW_PTR);

        // Okay, this bucket is properly set up in the destination
        // table.  We need to make sure the old bucket is updated
        // properly, by trying to add LOHAT_F_MOVED.
        LCAS(&cur->head,
             &old_head,
             hatrack_pflag_set(old_head, LOHAT_F_MOVED),
             LOHAT1_CTR_F_MOVED3);
        target++;
        cur++;
    }

    // Now that we've gone through every bucket in the old store, we
    // try to set hist_end in the new store (again, if nobody beat us
    // to it).

    expected_ptr = new_store->hist_buckets;
    LCAS(&new_store->hist_next, &expected_ptr, target, LOHAT1_CTR_F_HIST);

    if (LCAS(&top->store_current, &self, new_store, LOHAT1_CTR_STORE_INSTALL)) {
        lohat1_retire_store(self);
    }

    return new_store;
}

static hatrack_view_t *
lohat1_store_view(lohat1_store_t *self,
                  lohat1_t       *top,
                  uint64_t        epoch,
                  uint64_t       *num)
{
    lohat1_history_t *cur = self->hist_buckets;
    lohat1_history_t *end;
    hatrack_view_t   *view;
    hatrack_view_t   *p;
    hatrack_hash_t    hv;
    lohat_record_t   *rec;
    uint64_t          sort_epoch;
    uint64_t          num_items;

    end = atomic_load(&self->hist_next);

    if (self->hist_end < end) {
        end = self->hist_end;
    }

    view = (hatrack_view_t *)malloc(sizeof(hatrack_view_t) * (end - cur));
    p    = view;

    while (cur < end) {
        hv  = atomic_load(&cur->hv);
        rec = hatrack_pflag_clear(atomic_load(&cur->head),
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
    *num      = num_items;

    if (!num_items) {
        free(view);
        return NULL;
    }

    view = realloc(view, *num * sizeof(hatrack_view_t));

    // Unordered buckets should be in random order, so quicksort is a
    // good option.  Otherwise, we should use an insertion sort.
#ifdef HATRACK_QSORT_THRESHOLD
    if (num_items >= HATRACK_QSORT_THRESHOLD) {
        qsort(view, num_items, sizeof(hatrack_view_t), hatrack_quicksort_cmp);
    }
    else {
        lohat1_insertion_sort(view, num_items);
    }
#elif defined(HATRACK_ALWAYS_USE_QSORT)
    qsort(view, num_items, sizeof(hatrack_view_t), hatrack_quicksort_cmp);
#elif !defined(HATRACK_DONT_SORT)
    lohat1_insertion_sort(view, num_items);
#endif

    return view;
}

#if !defined(HATRACK_DONT_SORT) && !defined(HATRACK_ALWAYS_USE_QSORT)
static inline void
lohat1_insertion_sort(hatrack_view_t *view, uint64_t num_items)
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
}
#endif
