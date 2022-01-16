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

#include <hatrack.h>

// clang-format off

static lohat_store_t  *lohat_store_new    (uint64_t);
static void           *lohat_store_get    (lohat_store_t *, hatrack_hash_t,
					   bool *);
static void           *lohat_store_put    (lohat_store_t *, lohat_t *,
					   hatrack_hash_t, void *, bool *);
static void           *lohat_store_replace(lohat_store_t *, lohat_t *,
					   hatrack_hash_t, void *, bool *);
static bool            lohat_store_add    (lohat_store_t *, lohat_t *,
					   hatrack_hash_t, void *);
static void           *lohat_store_remove (lohat_store_t *, lohat_t *,
					   hatrack_hash_t, bool *);
static lohat_store_t  *lohat_store_migrate(lohat_store_t *, lohat_t *);

// clang-format on

// While mmm zeros out the memory it allocates, the system allocator may
// not, so explicitly zero out item_count to be cautious.
void
lohat_init(lohat_t *self)
{
    lohat_store_t *store;

    store = lohat_store_new(HATRACK_MIN_SIZE);

    atomic_store(&self->item_count, 0);
    atomic_store(&self->store_current, store);

    return;
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
lohat_get(lohat_t *self, hatrack_hash_t hv, bool *found)
{
    void *         ret;
    lohat_store_t *store;

    mmm_start_basic_op();

    store = atomic_read(&self->store_current);
    ret   = lohat_store_get(store, hv, found);

    mmm_end_op();

    return ret;
}

void *
lohat_put(lohat_t *self, hatrack_hash_t hv, void *item, bool *found)
{
    void *         ret;
    lohat_store_t *store;

    mmm_start_basic_op();

    store = atomic_read(&self->store_current);
    ret   = lohat_store_put(store, self, hv, item, found);

    mmm_end_op();

    return ret;
}

void *
lohat_replace(lohat_t *self, hatrack_hash_t hv, void *item, bool *found)
{
    void *         ret;
    lohat_store_t *store;

    mmm_start_basic_op();

    store = atomic_read(&self->store_current);
    ret   = lohat_store_replace(store, self, hv, item, found);

    mmm_end_op();

    return ret;
}

bool
lohat_add(lohat_t *self, hatrack_hash_t hv, void *item)
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
lohat_remove(lohat_t *self, hatrack_hash_t hv, bool *found)
{
    void *         ret;
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
    lohat_store_t *  store;
    lohat_history_t *buckets;
    lohat_history_t *p;
    lohat_history_t *end;
    lohat_record_t * rec;

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
    return atomic_read(&self->item_count);
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
    lohat_store_t *  store;
    hatrack_view_t * view;
    hatrack_view_t * p;
    lohat_record_t * rec;
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

        if (rec) {
            mmm_help_commit(rec);
        }

        /* First, we find the top-most record we can find that's older
         * than (or equal to) the linearization epoch, based on the
         * time the record was committed to the table.  At this point,
         * we happily will look under deletions; our goal is to just
         * go back in time until we find the right record for the
         * linearization epoch we're using, which we then store in the
         * variable 'rec'.
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
        if (!rec || sort_epoch > epoch || rec->deleted) {
            cur++;
            continue;
        }

        /* We found the right record via its write commit time, but for
         * sorting purposes, we want to go back to the create epoch.
         */
        p->item       = rec->item;
        p->sort_epoch = mmm_get_create_epoch(rec);

        p++;
        cur++;
    }

    num_items = p - view;
    *out_num  = num_items;

    // If there are no items, instead of realloc'ing down, free the
    // memory and return NULL.
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
 * be the stored value 0). That's why remove explicitly adds deletion
 * records.
 *
 * We do steal two bits out of the 'head' field ... Again, the head
 * field holds a pointer to the first record, or a NULL value if there
 * isn't one. But the two bottom bits are used to record migration
 * status of the bucket (LOWHAT_F_MOVING and LOWHAT_F_MOVED).
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
lohat_store_get(lohat_store_t *self, hatrack_hash_t hv1, bool *found)
{
    uint64_t         bix;
    uint64_t         i;
    hatrack_hash_t   hv2;
    lohat_history_t *bucket;
    lohat_record_t * head;

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
    if (head && !head->deleted) {
        if (found) {
            *found = true;
        }

        return head->item;
    }
    goto not_found;
}

static void *
lohat_store_put(lohat_store_t *self,
                lohat_t *      top,
                hatrack_hash_t hv1,
                void *         item,
                bool *         found)
{
    uint64_t         bix;
    uint64_t         i;
    uint64_t         used_count;
    hatrack_hash_t   hv2;
    lohat_history_t *bucket;
    lohat_record_t * head;
    lohat_record_t * candidate;

    bix = hatrack_bucket_index(hv1, self->last_slot);

    for (i = 0; i < self->last_slot; i++) {
        bucket = &self->hist_buckets[bix];
        hv2    = atomic_read(&bucket->hv);

        if (hatrack_bucket_unreserved(hv2)) {
            if (LCAS(&bucket->hv, &hv2, hv1, LOHAT_CTR_BUCKET_ACQUIRE)) {
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
    self = lohat_store_migrate(self, top);

    return lohat_store_put(self, top, hv1, item, found);

found_history_bucket:
    head = atomic_read(&bucket->head);

    if (hatrack_pflag_test(head, LOHAT_F_MOVING)) {
        goto migrate_and_retry;
    }

    candidate       = mmm_alloc(sizeof(lohat_record_t));
    candidate->next = head;
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

        if (!head->deleted) {
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

    /* If we're here, the record install was successful.  Now we need
     * to commit our write operation, so that our record will get a
     * write epoch recorded for it, which will be the linearization
     * point for this operation.
     *
     * Remember that if our thread gets suspended after the record
     * install, but before this commit completes, no other write
     * thread will use this record, nor will viewers, until it is
     * committed; they call mmm_help_commit() before accessing.
     */
    mmm_commit_write(candidate);

    /* The variable 'head' won't have any flags set here, otherwise
     * we'd be migrating the store.  If there is no head, or if the
     * previous record was a delete, then we also need to bump up the
     * item count.
     */

    if (!head) {
not_overwriting:
        atomic_fetch_add(&top->item_count, 1);

        if (found) {
            *found = false;
        }

        return NULL;
    }

    /* At this point, we've definitely installed a new record over
     * the old record, and therefore we are responsible for retiring
     * that record via mmm_retire().  If the record we're retiring
     * was a deletion record, then jump up above to bump the
     * item count and return NULL.
     *
     * Otherwise, return the item from the old record.

     * Remember that mmm_retire() never frees immediately; it's safe
     * to continue to use the pointer until the end of our operation.
     */

    mmm_retire(head);

    if (head->deleted) {
        goto not_overwriting;
    }

    if (found) {
        *found = true;
    }

    return head->item;
}

static void *
lohat_store_replace(lohat_store_t *self,
                    lohat_t *      top,
                    hatrack_hash_t hv1,
                    void *         item,
                    bool *         found)
{
    uint64_t         bix;
    uint64_t         i;
    hatrack_hash_t   hv2;
    lohat_history_t *bucket;
    lohat_record_t * head;
    lohat_record_t * candidate;

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

    if (hatrack_pflag_test(head, LOHAT_F_MOVING)) {
migrate_and_retry:
        self = lohat_store_migrate(self, top);

        return lohat_store_replace(self, top, hv1, item, found);
    }

    candidate       = mmm_alloc(sizeof(lohat_record_t));
    candidate->next = head;
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
    } while (!LCAS(&bucket->head, &head, candidate, LOHAT_CTR_REC_INSTALL));

    mmm_commit_write(candidate);
    mmm_retire(head);

    if (found) {
        *found = true;
    }

    return head->item;
}

static bool
lohat_store_add(lohat_store_t *self,
                lohat_t *      top,
                hatrack_hash_t hv1,
                void *         item)
{
    uint64_t         bix;
    uint64_t         i;
    uint64_t         used_count;
    hatrack_hash_t   hv2;
    lohat_history_t *bucket;
    lohat_record_t * head;
    lohat_record_t * candidate;

    bix = hatrack_bucket_index(hv1, self->last_slot);

    for (i = 0; i < self->last_slot; i++) {
        bucket = &self->hist_buckets[bix];
        hv2    = atomic_read(&bucket->hv);

        if (hatrack_bucket_unreserved(hv2)) {
            if (LCAS(&bucket->hv, &hv2, hv1, LOHAT_CTR_BUCKET_ACQUIRE)) {
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
        if (!head->deleted) {
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
    candidate->next = head;
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
lohat_store_remove(lohat_store_t *self,
                   lohat_t *      top,
                   hatrack_hash_t hv1,
                   bool *         found)
{
    uint64_t         bix;
    uint64_t         i;
    hatrack_hash_t   hv2;
    lohat_history_t *bucket;
    lohat_record_t * head;
    lohat_record_t * candidate;

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

    if (hatrack_pflag_test(head, LOHAT_F_MOVING)) {
migrate_and_retry:
        self = lohat_store_migrate(self, top);

        return lohat_store_remove(self, top, hv1, found);
    }

    // If !head, then some write hasn't finished, and we consider
    // the bucket empty.
    if (!head || head->deleted) {
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
    candidate          = mmm_alloc(sizeof(lohat_record_t));
    candidate->next    = head;
    candidate->item    = NULL;
    candidate->deleted = true;

    if (!LCAS(&bucket->head, &head, candidate, LOHAT_CTR_DEL)) {
        mmm_retire_unused(candidate);

        // Moving flag got set before our CAS.
        if (hatrack_pflag_test(head, LOHAT_F_MOVING)) {
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

    // Help finish the commit of anything we're overwriting, before we
    // fully commit our write, and then add its retirement epoch.

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

static lohat_store_t *
lohat_store_migrate(lohat_store_t *self, lohat_t *top)
{
    lohat_store_t *  new_store;
    lohat_store_t *  candidate_store;
    uint64_t         new_size;
    lohat_history_t *cur;
    lohat_history_t *bucket;
    lohat_record_t * head;
    lohat_record_t * candidate;
    lohat_record_t * expected_head;
    hatrack_hash_t   hv;
    hatrack_hash_t   expected_hv;
    uint64_t         i, j;
    uint64_t         bix;
    uint64_t         new_used;
    uint64_t         expected_used;

    /* If we're a late-enough writer, let's just double check to see
     * if we need to help at all.
     */
    new_store = atomic_read(&top->store_current);

    if (new_store != self) {
        return new_store;
    }

    new_used = 0;
    /* Quickly run through every history bucket, and mark any bucket
     * that doesn't already have F_MOVING set.  Note that the CAS
     * could fail due to some other updater, so we keep CASing until
     * we know some thread was successful (either we succeeded, or we
     * see LOHAT_F_MOVING).
     *
     * Note that this makes our migration algorithm lock-free, not
     * wait free, because other threads could keep causing us to fail
     * by re-writing this bucket continually.
     *
     * We solve that problem in woolhat, to make this part wait-free.
     */
    for (i = 0; i <= self->last_slot; i++) {
        cur  = &self->hist_buckets[i];
        head = atomic_read(&cur->head);

        do {
            if (hatrack_pflag_test(head, LOHAT_F_MOVING)) {
                goto didnt_win;
            }
            /* If the head pointer is a deletion record, we can also
             * set LOHAT_F_MOVED, since there's no actual work to do
             * to migrate. However, there we need to know if we won
             * the CAS, because if we do, we're responsible for the
             * memory management. That's why the exit from this loop
             * above is is a GOTO... we need to be able to do the
             * memory management and just move on to the nex item if
             * we've got a successful CAS on a delete record.
             */
            if (head && !head->deleted) {
                candidate = hatrack_pflag_set(head, LOHAT_F_MOVING);
            }
            else {
                candidate
                    = hatrack_pflag_set(head, LOHAT_F_MOVING | LOHAT_F_MOVED);
            }
        } while (!LCAS(&cur->head, &head, candidate, LOHAT_CTR_F_MOVING));

        if (head && hatrack_pflag_test(candidate, LOHAT_F_MOVED)) {
            mmm_help_commit(head);
            mmm_retire(head);
            continue;
        }

didnt_win:
        head = hatrack_pflag_clear(head, LOHAT_F_MOVING | LOHAT_F_MOVED);
        if (head && !head->deleted) {
            new_used++;
        }
    }

    new_store = atomic_read(&self->store_next);
    /* If there's still not a store in place by now, we'll allocate
     * one, and try to install it ourselves. If we can't install ours,
     * that means someone raced us, installing one after our read
     * completed (well, could have been during or even before our
     * read, since our read doesn't use a memory barrier, only the
     * swap used to install does), so we free ours and use it.
     *
     * Note that, in large tables, this could be a big allocation, and
     * there could be lots of concurrent threads attempting to do the
     * allocation at the same time. And, we are zero-initializing the
     * memory we grab, too (using calloc), making it straightforward
     * for us to tell the status of a bucket migration in the target
     * store.
     *
     * However, this isn't generally much of a concern-- most
     * reasonable calloc implementations on any reasonable
     * architecture will grab virtual memory that maps to a read-only
     * page of all zeros, and then copy-on-write, once we start
     * mutating the array.
     *
     * As a result, we may have a bunch of simultaneous maps into the
     * same page, but only the winning store should result in new
     * memory usage.
     */
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

    /* At this point, we're sure that any late writers will help us
     * with the migration. Therefore, we can go through each item,
     * and, if it's not fully migrated, we can attempt to migrate it.
     *
     * Of course, other migrating threads may race ahead of us.
     *
     * Note that, since the table may be resizing, not just migrating,
     * the bucket index our hash value maps to may change, and there
     * may be new collisions. Plus, we may no longer 'collide' with
     * keys that have since been deleted, which could put us in a
     * different place, even if the table is NOT resizing.
     *
     * That all means, we can't just copy a record to the same index
     * in the new table; we need to go through the bucket allocation
     * process again (including the linear probing).
     */
    for (i = 0; i <= self->last_slot; i++) {
        cur       = &self->hist_buckets[i];
        head      = atomic_read(&cur->head);
        candidate = hatrack_pflag_clear(head, LOHAT_F_MOVING | LOHAT_F_MOVED);

        if (hatrack_pflag_test(head, LOHAT_F_MOVED)) {
            continue;
        }

        // If it hasn't been moved, there's definitely an item in it,
        // as empty buckets got MOVED set in the first loop.
        hv  = atomic_read(&cur->hv);
        bix = hatrack_bucket_index(hv, new_store->last_slot);

        for (j = 0; j <= new_store->last_slot; j++) {
            bucket         = &new_store->hist_buckets[bix];
            expected_hv.w1 = 0;
            expected_hv.w2 = 0;

            if (!LCAS(&bucket->hv, &expected_hv, hv, LOHAT_CTR_MIGRATE_HV)) {
                if (!hatrack_hashes_eq(expected_hv, hv)) {
                    bix = (bix + 1) & new_store->last_slot;
                    continue;
                }
            }
            break;
        }

        expected_head = NULL;

        // The only way this can fail is if some other thread succeeded,
        // so we don't need to concern ourselves with the return value.
        LCAS(&bucket->head, &expected_head, candidate, LOHAT_CTR_MIG_REC);

        // Whether we won or not, assume the winner might have
        // stalled.  Every thread attempts to update the source
        // bucket, to denote that the move was successful.
        LCAS(&cur->head,
             &head,
             hatrack_pflag_set(head, LOHAT_F_MOVED),
             LOHAT_CTR_F_MOVED3);
    }

    /* All buckets are migrated. Attempt to write to the new table how
     * many buckets are currently used. Note that, it's possible if
     * the source table was drained, that this value might be zero, so
     * the value in the target array will already be right.  If we're
     * a late-comer, and the new store has already been re-opened for
     * writing, there's still no failure case-- if new writers write
     * to the new store, used_count only ever increases in the
     * context, since bucket reservations only get wiped out when
     * we do our migration.
     */
    expected_used = 0;

    LCAS(&new_store->used_count,
         &expected_used,
         new_used,
         LOHAT_CTR_LEN_INSTALL);

    /* Now that the new store is fully set up, with all its buckets in
     * place, and all other data correct, we can 'turn it on' for
     * writes, by overwriting the top-level object's store pointer.
     *
     * Of course, multiple threads might be trying to do this. If we
     * fail, it's because someone else succeeded, and we move on.
     *
     * However, if we succeed, then we are responsible for the memory
     * management of the old store. We use mmm_retire(), to make sure
     * that we don't free the store before all threads currently using
     * the store are done with it.
     */
    if (LCAS(&top->store_current, &self, new_store, LOHAT_CTR_STORE_INSTALL)) {
        mmm_retire(self);
    }

    /* Instead of returning new_store here, we accept that we might
     * have been suspended for a while at some point, and there might
     * be an even later migration. So we grab the top-most store, even
     * though we expect that it's generally going to be the same as
     * next_store.
     */
    return top->store_current;
}
