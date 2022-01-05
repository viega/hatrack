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
 *  Name:           lohat.c
 *  Description:    Linearizeable, Ordered, Wait-free HAsh Table (LOHAT)
 *                  This version never orders, it just sorts when needed.
 *                  Views are fully consistent.
 *
 *                  PLEASE READ THE COMMENT AT THE TOP OF lohat.h FOR
 *                  AN OVERVIEW OF THIS ALGORITHM.
 *
 *  Author:         John Viega, john@zork.org
 *
 */

#include "lohat.h"

// clang-format off

static lohat_store_t  *lohat_store_new    (uint64_t);
static void           *lohat_store_get    (lohat_store_t *, lohat_t *,
					   hatrack_hash_t *, bool *);
static void           *lohat_store_put    (lohat_store_t *, lohat_t *,
					   hatrack_hash_t *, void *, bool *);
static void           *lohat_store_replace(lohat_store_t *, lohat_t *,
					   hatrack_hash_t *, void *, bool *);
static bool            lohat_store_add    (lohat_store_t *, lohat_t *,
					   hatrack_hash_t *, void *);
static void           *lohat_store_remove (lohat_store_t *, lohat_t *,
					   hatrack_hash_t *, bool *);
static lohat_store_t  *lohat_store_migrate(lohat_store_t *, lohat_t *);

// clang-format on

// While mmm zeros out the memory it allocates, the system allocator may
// not, so explicitly zero out item_count to be cautious.
void
lohat_init(lohat_t *self)
{
    lohat_store_t *store = lohat_store_new(HATRACK_MIN_SIZE);

    atomic_store(&self->store_current, store);
    atomic_store(&self->item_count, 0);
}

/* lohat_get, _put, _replace, _add, _remove
 *
 * As discussed in our .h file, all of these operations use MMM to
 * protect our access to memory objects that other threads want to
 * deallocate.
 *
 * And, mmm_start_basic_op() is a lower bound of the linearized time
 * on any operation, below. As discussed above, the get operation will
 * get the most recent value in the store that it acquires... which
 * might not be completely up to date at the time the read happens.
 *
 * On the other hand, the write operations all happen during the epoch
 * in which the operation is committed.
 *
 *
 * NOTE:
 *
 * The basic interface for the top-level functions here works the same
 * way as with all our other hash tables. Please see refhat.c for a
 * thorough overview of the semantics.
 *
 * Here, we'll focus our comments here to what's going on in
 * lower-level (per-store) operations.
 */
void *
lohat_get(lohat_t *self, hatrack_hash_t *hv, bool *found)
{
    void          *ret;
    lohat_store_t *store;

    mmm_start_basic_op();
    store = atomic_read(&self->store_current);
    ret   = lohat_store_get(store, self, hv, found);
    mmm_end_op();

    return ret;
}

void *
lohat_put(lohat_t *self, hatrack_hash_t *hv, void *item, bool *found)
{
    void          *ret;
    lohat_store_t *store;

    mmm_start_basic_op();
    store = atomic_read(&self->store_current);
    ret   = lohat_store_put(store, self, hv, item, found);
    mmm_end_op();

    return ret;
}

void *
lohat_replace(lohat_t *self, hatrack_hash_t *hv, void *item, bool *found)
{
    void          *ret;
    lohat_store_t *store;

    mmm_start_basic_op();
    store = atomic_read(&self->store_current);
    ret   = lohat_store_replace(store, self, hv, item, found);
    mmm_end_op();

    return ret;
}

bool
lohat_add(lohat_t *self, hatrack_hash_t *hv, void *item)
{
    bool           ret;
    lohat_store_t *store;

    mmm_start_basic_op();
    store = atomic_read(&self->store_current);
    ret   = lohat_store_add(store, self, hv, item);
    mmm_end_op();

    return ret;
}

void *
lohat_remove(lohat_t *self, hatrack_hash_t *hv, bool *found)
{
    void          *ret;
    lohat_store_t *store;

    mmm_start_basic_op();
    store = atomic_read(&self->store_current);
    ret   = lohat_store_remove(store, self, hv, found);
    mmm_end_op();

    return ret;
}

/*
 * lohat_delete()
 *
 * Deletes a lohat object. Generally, you should be confident that
 * all threads except the one from which you're calling this have
 * stopped using the table (generally meaning they no longer hold a
 * reference to the store).
 *
 * Note that this function assumes the lohat object was allocated
 * via the default malloc. If it wasn't, don't call this directly, but
 * do note that the stores were created via mmm_alloc(), and the most
 * recent store will need to be retired via mmm_retire().
 *
 * Note that, the most current store will also most likely have
 * buckets that have not been retired. We could have a store register
 * a callback with MMM to go through once it's time to retire the
 * toplevel store.
 *
 * However, since no threads can be active here, we can just go
 * through all the buckets of the top-level store, and free them all.
 * Again, its important that this happen AFTER all threads are done
 * with the table.
 *
 * It means there will be no migration in process (no sub-store to
 * delete), and that we can immediately retire any record that's
 * currently in the table.
 *
 */
void
lohat_delete(lohat_t *self)
{
    lohat_store_t   *store   = atomic_load(&self->store_current);
    lohat_history_t *buckets = store->hist_buckets;
    lohat_history_t *p       = buckets;
    lohat_history_t *end     = buckets + (store->last_slot + 1);
    lohat_record_t  *rec;

    while (p < end) {
        rec = atomic_load(&p->head);
        if (rec) {
            mmm_retire_unused(rec);
        }
        p++;
    }

    mmm_retire_unused(store);
    free(self);
}

/* lohat_len()
 *
 * Returns the approximate number of items currently in the
 * table. Note that we strongly discourage using this call, since it
 * is close to meaningless in multi-threaded programs, as the value at
 * the time of check could be dramatically different by the time of
 * use.
 */
uint64_t
lohat_len(lohat_t *self)
{
    return self->item_count;
}

/* lohat_view()
 *
 * Conceptually, we are doing the same basic thing as with many of our
 * other hash tables-- going through every bucket, extracting the
 * item, hash value and timestamp for each, and then sorting based on
 * timestamp, if desired.
 *
 * However, our job is a little bit harder, because we're getting a
 * consistent view. We start by selecting an epoch to use for the sake
 * of linearization.
 *
 * This means, for every single bucket, we'll copy out the state of
 * that bucket, as it was, in the epoch of our linearization.
 *
 * To do that, we need to start by calling mmm_start_linearized_op(),
 * which, unlike mmm_start_op(), returns an epoch for the sake of
 * linearization.
 *
 * Getting an epoch for linearization actually has some subtleties to
 * it, especially to do it in a wait-free manner (which we do). See
 * mmm.h for the details on that, if you're interested.
 *
 * Then, as we visit buckets, we need to sift through the record stack
 * to find the "right" record, taking care not to traverse into
 * records that have been retired before our linearization epoch.
 *
 * Note that, if a table migration occurs after we grab the store
 * pointer, we can safely ignore it... every record from our
 * linearization epoch is necessarily in the old table.  We only need
 * to make sure to mask out the appropriate state bits (we use the
 * macro hatrack_pflag_clear from hatrack_common.h, which hides some
 * ugly casting for us).
 */
hatrack_view_t *
lohat_view(lohat_t *self, uint64_t *out_num, bool sort)
{
    lohat_history_t *cur;
    lohat_history_t *end;
    lohat_store_t   *store;
    hatrack_view_t  *view;
    hatrack_view_t  *p;
    hatrack_hash_t   hv;
    lohat_record_t  *rec;
    uint64_t         epoch;
    uint64_t         sort_epoch;
    uint64_t         num_items;

    epoch = mmm_start_linearized_op();
    store = self->store_current;
    cur   = store->hist_buckets;
    end   = cur + (store->last_slot + 1);
    view  = (hatrack_view_t *)malloc(sizeof(hatrack_view_t) * (end - cur));
    p     = view;

    while (cur < end) {
        hv  = atomic_read(&cur->hv);
        /* Clear the HEAD flags, to be able to dereference the
         * original record if a migration is in progres.
         */
        rec = hatrack_pflag_clear(atomic_read(&cur->head),
                                  LOHAT_F_MOVING | LOHAT_F_MOVED);

        /* If there's a record in this bucket, we need to ensure its
         * epoch is updated before we proceed! If the epoch isn't
         * committed by the time we're helping, the epoch that DOES
         * commit will end up greater than our linearization epoch,
         * naturally.
         */
        mmm_help_commit(rec);

        /* First, we find the top-most record we can find that's older
         * than (or equal to) the linearization epoch, based on the
         * time the record was committed to the table.  At this point,
         * we happily will look under deletions; our goal is to just
         * go back in time until we find the right record for the
         * linearization epoch we're using, which we then store in the
         * variable 'rec'.
         *
         * Note that when we walk records' next pointer, we need to
         * clear the USED bit (if set), in order to be able to walk
         * it.
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

        /* We found the right record via its write commit time, but for
         * sorting purposes, we want to go back to the create epoch.
         */
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

    // Size down to the actual used size.
    view = realloc(view, num_items * sizeof(hatrack_view_t));

    if (sort) {
        // Unordered buckets should be in random order, so quicksort
        // is a good option.
        qsort(view, num_items, sizeof(hatrack_view_t), hatrack_quicksort_cmp);
    }

    mmm_end_op();

    return view;
}

/* mmm_alloc() will zero out any stores we allocate, so we do not
 * bother to initialize null or zero-valued variables.
 */
static lohat_store_t *
lohat_store_new(uint64_t size)
{
    lohat_store_t *store;
    uint64_t       alloc_len;

    alloc_len = sizeof(lohat_store_t) + sizeof(lohat_history_t) * size;
    store     = (lohat_store_t *)mmm_alloc_committed(alloc_len);

    store->last_slot = size - 1;
    store->threshold = hatrack_compute_table_threshold(size);

    return store;
}

/* The basic read and write operations all look similar to hihat2,
 * with the exception of the way we use pointer flags, since we are
 * always going to use the top-most record for these operations.
 *
 * I'll give an overview here about how we're using these flags (see
 * lohat_common.h for their definition, and more commentary on them).
 *
 * But for the general mechanics of acquiring a bucket, and the other
 * accounting we do, see hihat.c, which again is very similar.
 *
 * Remember that this implementation keeps a linked list of
 * modification records on a per-bucket basis. The head of that linked
 * list is in stored in the 'head' field.  Each record (declared in
 * lohat_common.h) consists of two fields: a pointer to the item
 * associated with that record (the item field), and a pointer to the
 * record beneath it in the stack.
 *
 * When a viewer is looking through record history, they need to know
 * whether the item in the record is a live record, or if the record
 * is deleted (a null item could be a null pointer, but it could also
 * be the stored value 0).
 *
 * The flag LOHAT_F_USED gets set on the 'next' pointer in a record,
 * to make that indication. Of course, as you can see above, to
 * traverse down the linked list, we first need to clear the
 * pointer. That's the only bit we still from the record itself.
 *
 * We do steal two bits out of the 'head' field though... Again, the
 * head field holds a pointer to the first record, or a NULL value if
 * there isn't one. But the two bottom bits are used to record
 * migration status of the bucket (LOWHAT_F_MOVING and LOWHAT_F_MOVED).
 *
 * Naturally, these pointers need to be cleared every time we want to
 * access the top record in a bucket.
 *
 * The pointer stealing doesn't take up too many cycles. On the other
 * hand, the fact that any record we want to access always requires a
 * pointer indirection (one which will generally result in a cache
 * miss), DOES have an impact on performance, which is why our hash
 * tables with linearization are slower than the ones that don't
 * (except for oldhat, which also requires pointer indirection, and is
 * in the same speed class).
 *
 * Still, most applications do plenty of pointer indirections, so the
 * bit of extra here should be way, way in the noise for nearly all
 * applications.
 */
static void *
lohat_store_get(lohat_store_t  *self,
                lohat_t        *top,
                hatrack_hash_t *hv1,
                bool           *found)
{
    uint64_t         bix;
    uint64_t         i;
    hatrack_hash_t   hv2;
    lohat_history_t *bucket;
    lohat_record_t  *head;

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
    /* The reader doesn't care if we're migrating; it needs
     * to access the record, and cleans out the migration
     * bits so it can reference the record.
     */
    head = hatrack_pflag_clear(atomic_read(&bucket->head),
                               LOHAT_F_MOVING | LOHAT_F_MOVED);
    /* We can't look at the item field to tell if the record is
     * in use; instead we look at the pointer to the next record,
     * which stores that information.
     */
    if (head && hatrack_pflag_test(head->next, LOHAT_F_USED)) {
        if (found) {
            *found = true;
        }
        return head->item;
    }
    goto not_found;
}

static void *
lohat_store_put(lohat_store_t  *self,
                lohat_t        *top,
                hatrack_hash_t *hv1,
                void           *item,
                bool           *found)
{
    void            *ret;
    uint64_t         bix;
    uint64_t         i;
    hatrack_hash_t   hv2;
    lohat_history_t *bucket;
    lohat_record_t  *head;
    lohat_record_t  *candidate;

    bix = hatrack_bucket_index(hv1, self->last_slot);

    for (i = 0; i < self->last_slot; i++) {
        bucket = &self->hist_buckets[bix];
        hv2    = atomic_read(&bucket->hv);
        if (hatrack_bucket_unreserved(&hv2)) {
            if (LCAS(&bucket->hv, &hv2, *hv1, LOHAT_CTR_BUCKET_ACQUIRE)) {
                if (atomic_fetch_add(&self->used_count, 1) >= self->threshold) {
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
    self = lohat_store_migrate(self, top);
    return lohat_store_put(self, top, hv1, item, found);

found_history_bucket:
    head = atomic_read(&bucket->head);

    if (hatrack_pflag_test(head, LOHAT_F_MOVING)) {
        goto migrate_and_retry;
    }
    candidate       = mmm_alloc(sizeof(lohat_record_t));
    candidate->next = hatrack_pflag_set(head, LOHAT_F_USED);
    candidate->item = item;

    /* Even if we're the winner, we need still to make sure that the
     * previous thread's write epoch got committed (since our epoch
     * needs to be later than theirs). Then, we need to commit our
     * write, and return whatever value was there before, if any.
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

    if (!LCAS(&bucket->head, &head, candidate, LOHAT_CTR_REC_INSTALL)) {
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

    /* If we're here, the record install was successful.  head won't
     * have any flags set, otherwise we'd be migrating instead.
     * If there is no head, or if the previous record was a delete,
     * then we also need to bump up the item count.
     */
    if (!head || !(hatrack_pflag_test(head->next, LOHAT_F_USED))) {
        atomic_fetch_add(&top->item_count, 1);
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

    // Since we were successful at installing the record, we are
    // respondible for retiring the old record.
    if (head) {
        mmm_retire(head);
    }

    return ret;
}

static void *
lohat_store_replace(lohat_store_t  *self,
                    lohat_t        *top,
                    hatrack_hash_t *hv1,
                    void           *item,
                    bool           *found)
{
    uint64_t         bix;
    uint64_t         i;
    hatrack_hash_t   hv2;
    lohat_history_t *bucket;
    lohat_record_t  *head;
    lohat_record_t  *candidate;

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

    if (hatrack_pflag_test(head, LOHAT_F_MOVING)) {
migrate_and_retry:
        self = lohat_store_migrate(self, top);
        return lohat_store_replace(self, top, hv1, item, found);
    }
    candidate       = mmm_alloc(sizeof(lohat_record_t));
    candidate->next = hatrack_pflag_set(head, LOHAT_F_USED);
    candidate->item = item;

    /* This CAS-loop makes our replace operation lock free, not
     * wait-free.  When there's contention, as long as the new record
     * is an item, instead of a delete record, we will keep trying,
     * either until we succeed, or until we notice we need to migrate.
     *
     * Note that every time through the loop, re-set our create epoch
     * based on the new record. However, if we wanted, we could leave
     * it alone, and instead bail if the create epoch of the record
     * we're trying to replace ever changes-- as it means at some
     * point from where we started, until now, there was a deletion
     * and a re-insertion.
     *
     * Woolhat is a wait-free version of this algorithm, which does
     * not keep going if there's contention.
     */
    do {
        if (!hatrack_pflag_test(head->next, LOHAT_F_USED)) {
            mmm_retire_unused(candidate);
            goto not_found;
        }
        if (hatrack_pflag_test(head, LOHAT_F_MOVING)) {
            mmm_retire_unused(candidate);
            goto migrate_and_retry;
        }
        mmm_help_commit(head);
        mmm_copy_create_epoch(candidate, head);
    } while (!LCAS(&bucket->head, &head, candidate, LOHAT_CTR_REC_INSTALL));

    mmm_commit_write(candidate);
    mmm_retire(head);

    if (found) {
        *found = true;
    }

    return head->item;
}

static bool
lohat_store_add(lohat_store_t  *self,
                lohat_t        *top,
                hatrack_hash_t *hv1,
                void           *item)
{
    uint64_t         bix;
    uint64_t         i;
    uint64_t         used_count;
    hatrack_hash_t   hv2;
    lohat_history_t *bucket;
    lohat_record_t  *head;
    lohat_record_t  *candidate;

    bix = hatrack_bucket_index(hv1, self->last_slot);

    for (i = 0; i < self->last_slot; i++) {
        bucket = &self->hist_buckets[bix];
        hv2    = atomic_read(&bucket->hv);

        if (hatrack_bucket_unreserved(&hv2)) {
            if (LCAS(&bucket->hv, &hv2, *hv1, LOHAT_CTR_BUCKET_ACQUIRE)) {
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
    self = lohat_store_migrate(self, top);
    return lohat_store_add(self, top, hv1, item);

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

    /* Right now there's nothing in the bucket, but there might be
     * something in the bucket before we add our item, in which case
     * the CAS will fail. Or, the CAS may fail if the migrating flag
     * got set.  If there is an item there, we return false; if we see
     * a migration in progress, we go off and do that instead.
     */
    candidate       = mmm_alloc(sizeof(lohat_record_t));
    candidate->next = hatrack_pflag_set(head, LOHAT_F_USED);
    candidate->item = item;
    if (!LCAS(&bucket->head, &head, candidate, LOHAT_CTR_REC_INSTALL)) {
        mmm_retire_unused(candidate);

        if (hatrack_pflag_test(head, LOHAT_F_MOVING)) {
            goto migrate_and_retry;
        }
        return false;
    }

    atomic_fetch_add(&top->item_count, 1);

    if (head) {
        /* If there's a previous record, it will be a "delete", so we need
         * still to make sure that the previous thread's write epoch got
         * committed before committing our write.
         */
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
lohat_store_remove(lohat_store_t  *self,
                   lohat_t        *top,
                   hatrack_hash_t *hv1,
                   bool           *found)
{
    uint64_t         bix;
    uint64_t         i;
    hatrack_hash_t   hv2;
    lohat_history_t *bucket;
    lohat_record_t  *head;
    lohat_record_t  *candidate;

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
        self = lohat_store_migrate(self, top);
        return lohat_store_remove(self, top, hv1, found);
    }

    // If !head, then some write hasn't finished, and we consider
    // the bucket empty.
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
    if (!LCAS(&bucket->head, &head, candidate, LOHAT_CTR_DEL)) {
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

    atomic_fetch_sub(&top->item_count, 1);
    return head->item;
}

static lohat_store_t *
lohat_store_migrate(lohat_store_t *self, lohat_t *top)
{
    lohat_store_t   *new_store;
    lohat_store_t   *candidate_store;
    uint64_t         new_size;
    lohat_history_t *cur;
    lohat_history_t *bucket;
    lohat_record_t  *head;
    lohat_record_t  *candidate;
    lohat_record_t  *expected_head;
    hatrack_hash_t   hv;
    hatrack_hash_t   expected_hv;
    uint64_t         i, j;
    uint64_t         bix;
    uint64_t         new_used;
    uint64_t         expected_used;

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
            if (hatrack_pflag_test(head, LOHAT_F_MOVING)) {
                break;
            }
        } while (!LCAS(&cur->head,
                       &head,
                       hatrack_pflag_set(head, LOHAT_F_MOVING),
                       LOHAT_CTR_F_MOVING));
        head = hatrack_pflag_clear(head, LOHAT_F_MOVING | LOHAT_F_MOVED);
        if (head && hatrack_pflag_test(head->next, LOHAT_F_USED)) {
            new_used++;
        }
    }

    new_store = atomic_read(&self->store_next);

    if (!new_store) {
        new_size        = hatrack_new_size(self->last_slot, new_used);
        candidate_store = lohat_store_new(new_size);

        if (!LCAS(&self->store_next,
                  &new_store,
                  candidate_store,
                  LOHAT_CTR_NEW_STORE)) {
            mmm_retire_unused(candidate_store);
        }
        else {
            new_store = candidate_store;
        }
    }

    for (i = 0; i <= self->last_slot; i++) {
        cur       = &self->hist_buckets[i];
        head      = atomic_read(&cur->head);
        candidate = hatrack_pflag_clear(head, LOHAT_F_MOVING | LOHAT_F_MOVED);

        if (!candidate) {
            if (!(hatrack_pflag_test(head, LOHAT_F_MOVED))) {
                LCAS(&cur->head,
                     &head,
                     hatrack_pflag_set(head, LOHAT_F_MOVED),
                     LOHAT_CTR_F_MOVED1);
            }
            continue;
        }

        if (hatrack_pflag_test(head, LOHAT_F_MOVED)) {
            continue;
        }

        if (!hatrack_pflag_test(candidate->next, LOHAT_F_USED)) {
            if (LCAS(&cur->head,
                     &head,
                     hatrack_pflag_set(head, LOHAT_F_MOVED),
                     LOHAT_CTR_F_MOVED2)) {
                // Need to not mmm_retire() something without a write epoch
                // when something is still referencing it.
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
            if (!LCAS(&bucket->hv, &expected_hv, hv, LOHAT_CTR_MIGRATE_HV)) {
                if (!hatrack_hashes_eq(&expected_hv, &hv)) {
                    bix = (bix + 1) & new_store->last_slot;
                    continue;
                }
            }
            break;
        }

        expected_head = NULL;
        LCAS(&bucket->head, &expected_head, candidate, LOHAT_CTR_MIG_REC);
        LCAS(&cur->head,
             &head,
             hatrack_pflag_set(head, LOHAT_F_MOVED),
             LOHAT_CTR_F_MOVED3);
    }

    expected_used = 0;

    LCAS(&new_store->used_count,
         &expected_used,
         new_used,
         LOHAT_CTR_LEN_INSTALL);

    if (LCAS(&top->store_current, &self, new_store, LOHAT_CTR_STORE_INSTALL)) {
        mmm_retire(self);
    }

    return top->store_current;
}
