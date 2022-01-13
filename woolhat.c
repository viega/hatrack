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
					      hatrack_hash_t, bool *);
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

// clang-format on

void
woolhat_init(woolhat_t *self)
{
    woolhat_store_t *store;

    store = woolhat_store_new(HATRACK_MIN_SIZE);

    atomic_store(&self->help_needed, 0);
    atomic_store(&self->item_count, 0);
    atomic_store(&self->store_current, store);

    return;
}

void *
woolhat_get(woolhat_t *self, hatrack_hash_t hv, bool *found)
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

    return;
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
                  hatrack_hash_t   hv1,
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
    head = atomic_read(&bucket->head);
    head = hatrack_pflag_clear(head, WOOLHAT_F_MOVING | WOOLHAT_F_MOVED);

    if (head && !head->deleted) {
        if (found) {
            *found = true;
        }
        return head->item;
    }
not_found:
    if (found) {
        *found = false;
    }

    return NULL;
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
    woolhat_record_t  *candidate;
    void              *ret;

    bix = hatrack_bucket_index(hv1, self->last_slot);

    for (i = 0; i < self->last_slot; i++) {
        bucket = &self->hist_buckets[bix];
        hv2    = atomic_read(&bucket->hv);
        if (hatrack_bucket_unreserved(hv2)) {
            if (LCAS(&bucket->hv, &hv2, hv1, WOOLHAT_CTR_BUCKET_ACQUIRE)) {
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
    head = atomic_read(&bucket->head);

    if (hatrack_pflag_test(head, WOOLHAT_F_MOVING)) {
        goto migrate_and_retry;
    }
    candidate       = mmm_alloc(sizeof(woolhat_record_t));
    candidate->next = head;
    candidate->item = item;

    if (head) {
        mmm_help_commit(head);
        if (!head->deleted) {
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

    if (head->deleted) {
        goto not_overwriting;
    }
    if (found) {
        *found = true;
    }

    /* In woolhat, whenever we successfully overwrite a value, we need
     * to help migrate, if a migration is in process.  See
     * woolhat_store_migrate() for a bit more detailed an
     * explaination, but doing this helps make woolhat_store_migrate()
     * wait free.
     */
    if (atomic_read(&self->used_count) >= self->threshold) {
        woolhat_store_migrate(self, top);
    }
    return head->item;
}

static void *
woolhat_store_replace(woolhat_store_t *self,
                      woolhat_t       *top,
                      hatrack_hash_t   hv1,
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
    head = atomic_read(&bucket->head);

    if (!head) {
        goto not_found;
    }

    if (hatrack_pflag_test(head, WOOLHAT_F_MOVING)) {
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

    if (head->deleted) {
        goto not_found;
    }

    candidate       = mmm_alloc(sizeof(woolhat_record_t));
    candidate->next = head;
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

    /* In woolhat, whenever we successfully overwrite a value, we need
     * to help migrate, if a migration is in process.  See
     * woolhat_store_migrate() for a bit more detailed an
     * explaination, but doing this helps make woolhat_store_migrate()
     * wait free.
     */
    if (atomic_read(&self->used_count) >= self->threshold) {
        woolhat_store_migrate(self, top);
    }

    return head->item;
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
    woolhat_record_t  *candidate;

    bix = hatrack_bucket_index(hv1, self->last_slot);

    for (i = 0; i < self->last_slot; i++) {
        bucket = &self->hist_buckets[bix];
        hv2    = atomic_read(&bucket->hv);

        if (hatrack_bucket_unreserved(hv2)) {
            if (LCAS(&bucket->hv, &hv2, hv1, WOOLHAT_CTR_BUCKET_ACQUIRE)) {
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
    head = atomic_read(&bucket->head);

    if (hatrack_pflag_test(head, WOOLHAT_F_MOVING)) {
        goto migrate_and_retry;
    }

    if (head && !head->deleted) {
        return false;
    }

    candidate       = mmm_alloc(sizeof(woolhat_record_t));
    candidate->next = head;
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
                     hatrack_hash_t   hv1,
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
        if (hatrack_bucket_unreserved(hv2)) {
            break;
        }
        if (!hatrack_hashes_eq(hv1, hv2)) {
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

    if (!head || head->deleted) {
        goto empty_bucket;
    }

    candidate          = mmm_alloc(sizeof(woolhat_record_t));
    candidate->next    = head;
    candidate->item    = NULL;
    candidate->deleted = true;
    if (!LCAS(&bucket->head, &head, candidate, WOOLHAT_CTR_DEL)) {
        mmm_retire_unused(candidate);

        // Moving flag got set before our CAS.
        if (hatrack_pflag_test(head, WOOLHAT_F_MOVING)) {
            goto migrate_and_retry;
        }
        if (head->deleted) {
            // We got beat to the delete;
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

    /* In woolhat, whenever we successfully overwrite a value, we need
     * to help migrate, if a migration is in process.  See
     * woolhat_store_migrate() for a bit more detailed an
     * explaination, but doing this helps make woolhat_store_migrate()
     * wait free.
     */
    if (atomic_read(&self->used_count) >= self->threshold) {
        woolhat_store_migrate(self, top);
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

    for (i = 0; i <= self->last_slot; i++) {
        cur  = &self->hist_buckets[i];
        head = atomic_read(&cur->head);

        /* This loop is the final place where lohat is only
         * lock-free-- it's possible that we could spin forever
         * waiting to lock a bucket, telling other writers to migrate.
         * For instance, we might be the only thread adding new
         * content, and other threads might all be trying to update
         * the same bucket with new values as fast as they can. It's
         * possible (though not likely in practice, due to fair
         * scheduling), that we'd effectively get starved, spinning
         * here forever.
         *
         * Woolhat addresses this problem by having writers who
         * MODIFY the contents of a bucket help migrate, but only
         * after their modification operation succeeds. This prevents
         * any one thread from unbounded starving others in this loop
         * due to modifications-- the upper bound is the number of
         * threads performing updates.
         *
         * Note that it's only necessary to do that check on updates
         * of existing values (including removes), not on inserts.
         */
        do {
            if (hatrack_pflag_test(head, WOOLHAT_F_MOVING)) {
                goto didnt_win;
            }
            if (head && !head->deleted) {
                candidate = hatrack_pflag_set(head, WOOLHAT_F_MOVING);
            }
            else {
                candidate
                    = hatrack_pflag_set(head,
                                        WOOLHAT_F_MOVING | WOOLHAT_F_MOVED);
            }
        } while (!LCAS(&cur->head, &head, candidate, WOOLHAT_CTR_F_MOVING));
        if (head && hatrack_pflag_test(candidate, WOOLHAT_F_MOVED)) {
            mmm_help_commit(head);
            mmm_retire(head);
            continue;
        }

didnt_win:
        head = hatrack_pflag_clear(head, WOOLHAT_F_MOVING | WOOLHAT_F_MOVED);
        if (head && !head->deleted) {
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

        hv  = atomic_read(&cur->hv);
        bix = hatrack_bucket_index(hv, new_store->last_slot);

        for (j = 0; j <= new_store->last_slot; j++) {
            bucket         = &new_store->hist_buckets[bix];
            expected_hv.w1 = 0;
            expected_hv.w2 = 0;
            if (!LCAS(&bucket->hv, &expected_hv, hv, WOOLHAT_CTR_MIGRATE_HV)) {
                if (!hatrack_hashes_eq(expected_hv, hv)) {
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
