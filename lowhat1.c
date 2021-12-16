#include "lowhat1.h"
#include <assert.h>

// clang-format off
static lowhat_store_t *lowhat1_store_new(uint64_t);
static void            lowhat1_delete_store(lowhat_store_t *);
static void           *lowhat1_store_get(lowhat_store_t *, lowhat_t *,
					 lowhat_hash_t *, bool *);
static void           *lowhat1_store_put(lowhat_store_t *, lowhat_t *,
					 lowhat_hash_t *, void *, bool *);
static bool            lowhat1_store_put_if_empty(lowhat_store_t *, lowhat_t *,
						  lowhat_hash_t *, void *);
static void           *lowhat1_store_remove(lowhat_store_t *, lowhat_t *,
					    lowhat_hash_t *, bool *);
static lowhat_store_t *lowhat1_store_migrate(lowhat_store_t *, lowhat_t *);
static lowhat_view_t  *lowhat1_store_view(lowhat_store_t *, lowhat_t *,
					  uint64_t, uint64_t *);
static inline void     lowhat1_do_migration(lowhat_store_t *, lowhat_store_t *);

#if defined(LOWHAT_QSORT_THRESHOLD) || defined(LOWHAT_ALWAYS_USE_QSORT)
static int             lowhat1_quicksort_cmp(const void *, const void *);
#endif

#if !defined(LOWHAT_DONT_SORT) && !defined(LOWHAT_ALWAYS_USE_QSORT)
static void            lowhat1_insertion_sort(lowhat_view_t *, uint64_t);
#endif


const lowhat_vtable_t lowhat1_vtable = {
    .init   = lowhat1_init,
    .get    = lowhat1_get,
    .put    = lowhat1_put,
    .remove = lowhat1_remove,
    .delete = lowhat1_delete,
    .len    = lowhat1_len,
    .view   = lowhat1_view
};
// clang-format on

void
lowhat1_init(lowhat_t *self)
{
    lowhat_store_t *store = lowhat1_store_new(1 << LOWHAT_MIN_SIZE_LOG);

    mmm_commit_write(store);
    atomic_store(&self->store_current, store);
}

// lowhat1_get() returns whatever is stored in the item field.
// Generally, we expect this to be two pointers, a key and a value.
// Meaning, when the object is NOT in the table, the return value
// will be the null pointer.
//
// When not using values (i.e., a set), it would be reasonable to
// store values directly, instead of pointers. Thus, the extra
// optional parameter to get() can tell us whether the item was
// found or not.  Set it to NULL if you're not interested.

void *
lowhat1_get(lowhat_t *self, lowhat_hash_t *hv, bool *found)
{
    void *ret;

    mmm_start_basic_op();
    ret = lowhat1_store_get(self->store_current, self, hv, found);
    mmm_end_op();

    return ret;
}

void *
lowhat1_put(lowhat_t      *self,
            lowhat_hash_t *hv,
            void          *item,
            bool           ifempty,
            bool          *found)
{
    void *ret;
    bool  bool_ret;

    mmm_start_basic_op();
    if (ifempty) {
        bool_ret
            = lowhat1_store_put_if_empty(self->store_current, self, hv, item);
        mmm_end_op();

        return (void *)bool_ret;
    }

    ret = lowhat1_store_put(self->store_current, self, hv, item, found);
    mmm_end_op();

    return ret;
}

void *
lowhat1_remove(lowhat_t *self, lowhat_hash_t *hv, bool *found)
{
    void *ret;

    mmm_start_basic_op();
    ret = lowhat1_store_remove(self->store_current, self, hv, found);
    mmm_end_op();

    return ret;
}

void
lowhat1_delete(lowhat_t *self)
{
    lowhat_store_t   *store   = atomic_load(&self->store_current);
    lowhat_history_t *buckets = store->hist_buckets;
    lowhat_history_t *p       = buckets;
    lowhat_history_t *end     = store->hist_end;
    lowhat_record_t  *rec;

    while (p < end) {
        rec = lowhat_pflag_clear(atomic_load(&p->head),
                                 LOWHAT_F_MOVED | LOWHAT_F_MOVING);
        if (rec) {
            mmm_retire_unused(rec);
        }
        p++;
    }

    lowhat1_delete_store(store);
    free(self);
}

uint64_t
lowhat1_len(lowhat_t *self)
{
    uint64_t diff;

    // Remember that pointer subtraction implicity divides
    // the result by the number of objects, so this
    // gives us the number of buckets in the history store
    // that have been claimed.
    diff = atomic_load(&self->store_current->hist_next)
         - self->store_current->hist_buckets;

    // Subtract out buckets known to be empty.
    return diff - atomic_load(&self->store_current->del_count);
}

lowhat_view_t *
lowhat1_view(lowhat_t *self, uint64_t *num_items)
{
    lowhat_view_t *ret;
    uint64_t       epoch;

    epoch = mmm_start_linearized_op();
    ret   = lowhat1_store_view(self->store_current, self, epoch, num_items);
    mmm_end_op();

    return ret;
}

static lowhat_store_t *
lowhat1_store_new(uint64_t size)
{
    lowhat_store_t *store = (lowhat_store_t *)mmm_alloc(sizeof(lowhat_store_t));

    store->last_slot = size - 1;
    store->threshold = lowhat_compute_table_threshold(size);
    store->del_count = ATOMIC_VAR_INIT(0);
    store->hist_buckets
        = (lowhat_history_t *)mmm_alloc(sizeof(lowhat_history_t) * size);
    store->store_next = ATOMIC_VAR_INIT(NULL);
    store->ptr_buckets
        = (lowhat_indirect_t *)mmm_alloc(sizeof(lowhat_indirect_t) * size);
    store->hist_end
        = store->hist_buckets + lowhat_compute_table_threshold(size);
    store->hist_next = store->hist_buckets;

    mmm_commit_write(store->hist_buckets);
    mmm_commit_write(store->ptr_buckets);

    return store;
}

static void
lowhat1_delete_store(lowhat_store_t *self)
{
    mmm_retire_unused(self->ptr_buckets);
    mmm_retire_unused(self->hist_buckets);
    mmm_retire_unused(self);
}

static void
lowhat1_retire_store(lowhat_store_t *self)
{
    mmm_retire(self->ptr_buckets);
    mmm_retire(self->hist_buckets);
    mmm_retire(self);
}

static void *
lowhat1_store_get(lowhat_store_t *self,
                  lowhat_t       *top,
                  lowhat_hash_t  *hv1,
                  bool           *found)
{
    uint64_t           bix = lowhat_bucket_index(hv1, self->last_slot);
    uint64_t           i;
    lowhat_hash_t      hv2;
    lowhat_history_t  *bucket;
    lowhat_record_t   *head;
    lowhat_indirect_t *ptrbucket;

    for (i = 0; i <= self->last_slot; i++) {
        ptrbucket = &self->ptr_buckets[bix];
        hv2       = atomic_load(&ptrbucket->hv);
        if (lowhat_bucket_unreserved(&hv2)) {
not_found:
            if (found) {
                *found = false;
            }
            return NULL;
        }
        if (!lowhat_hashes_eq(hv1, &hv2)) {
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
    head = lowhat_pflag_clear(atomic_load(&bucket->head),
                              LOWHAT_F_MOVING | LOWHAT_F_MOVED);
    if (head && lowhat_pflag_test(head->next, LOWHAT_F_USED)) {
        if (found) {
            *found = true;
        }
        return head->item;
    }
    goto not_found;
}

static void *
lowhat1_store_put(lowhat_store_t *self,
                  lowhat_t       *top,
                  lowhat_hash_t  *hv1,
                  void           *item,
                  bool           *found)
{
    void              *ret;
    uint64_t           bix = lowhat_bucket_index(hv1, self->last_slot);
    uint64_t           i;
    lowhat_hash_t      hv2;
    lowhat_history_t  *bucket;
    lowhat_history_t  *new_bucket;
    lowhat_record_t   *head;
    lowhat_record_t   *candidate;
    lowhat_indirect_t *ptrbucket;

    for (i = 0; i < self->last_slot; i++) {
        ptrbucket = &self->ptr_buckets[bix];
        hv2.w1    = 0;
        hv2.w2    = 0;
        if (!CAS(&ptrbucket->hv, &hv2, *hv1)) {
            if (!lowhat_hashes_eq(hv1, &hv2)) {
                bix = (bix + 1) & self->last_slot;
                continue;
            }
        }
        // If we are the first writer, or if there's a writer ahead of
        // us who was slow, both the ptr value and the hash value in
        // the history record may not be set.  For the ptr field, we
        // check to see if it's not set, before trying to "help",
        // because we don't want to waste space in the second
        // array. However, with the hv, we always just try to write
        // it.
        bucket = atomic_load(&ptrbucket->ptr);
        if (!bucket) {
            new_bucket = atomic_fetch_add(&self->hist_next, 1);
            if (new_bucket >= self->hist_end) {
                return lowhat1_store_put(lowhat1_store_migrate(self, top),
                                         top,
                                         hv1,
                                         item,
                                         found);
            }
            // If someone else installed ptr before we did, then its
            // value will be in bucket.  Otherwise, it will be in
            // new_bucket.
            if (CAS(&ptrbucket->ptr, &bucket, new_bucket)) {
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
        CAS(&bucket->hv, &hv2, *hv1);

        goto found_history_bucket;
    }
    return lowhat1_store_put(lowhat1_store_migrate(self, top),
                             top,
                             hv1,
                             item,
                             found);

found_history_bucket:
    head = atomic_load(&bucket->head);

    if (lowhat_pflag_test(head, LOWHAT_F_MOVING)) {
        return lowhat1_store_put(lowhat1_store_migrate(self, top),
                                 top,
                                 hv1,
                                 item,
                                 found);
    }
    candidate       = mmm_alloc(sizeof(lowhat_record_t));
    candidate->next = lowhat_pflag_set(head, LOWHAT_F_USED);
    candidate->item = item;

    // Even if we're the winner, we need still to make sure that the
    // previous thread's write epoch got committed (since ours has to
    // be later than theirs). Then, we need to commit our write, and
    // return whatever value was there before, if any.
    //
    // Do this first, so we can attempt to set our create epoch
    // properly before we move our record into place.

    if (head) {
        mmm_help_commit(head);
        if (lowhat_pflag_test(head->next, LOWHAT_F_USED)) {
            mmm_set_create_epoch(candidate, mmm_get_create_epoch(head));
        }
    }

    if (!CAS(&bucket->head, &head, candidate)) {
        // CAS failed. This is either because a flag got updated
        // (because of a table migration), or because a new record got
        // added first.  In the later case, we act like our write
        // happened, and that we got immediately overwritten, before
        // any read was possible.  We want the caller to delete the
        // item if appropriate, so when found is passed, we return
        // *found = true, and return the item passed in as a result.
        mmm_retire_unused(candidate);

        if (lowhat_pflag_test(head, LOWHAT_F_MOVING)) {
            return lowhat1_store_put(lowhat1_store_migrate(self, top),
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
    if (!(lowhat_pflag_test(head->next, LOWHAT_F_USED))) {
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
lowhat1_store_put_if_empty(lowhat_store_t *self,
                           lowhat_t       *top,
                           lowhat_hash_t  *hv1,
                           void           *item)
{
    uint64_t           bix = lowhat_bucket_index(hv1, self->last_slot);
    uint64_t           i;
    lowhat_hash_t      hv2;
    lowhat_history_t  *bucket;
    lowhat_history_t  *new_bucket;
    lowhat_record_t   *head;
    lowhat_record_t   *candidate;
    lowhat_indirect_t *ptrbucket;

    for (i = 0; i < self->last_slot; i++) {
        ptrbucket = &self->ptr_buckets[bix];
        hv2.w1    = 0;
        hv2.w2    = 0;
        if (!CAS(&ptrbucket->hv, &hv2, *hv1)) {
            if (!lowhat_hashes_eq(hv1, &hv2)) {
                bix = (bix + 1) & self->last_slot;
                continue;
            }
        }
        // If we are the first writer, or if there's a writer ahead of
        // us who was slow, both the ptr value and the hash value in
        // the history record may not be set.  For the ptr field, we
        // check to see if it's not set, before trying to "help",
        // because we don't want to waste space in the second
        // array. However, with the hv, we always just try to write
        // it.
        bucket = atomic_load(&ptrbucket->ptr);
        if (!bucket) {
            new_bucket = atomic_fetch_add(&self->hist_next, 1);
            if (new_bucket >= self->hist_end) {
                return lowhat1_store_put_if_empty(
                    lowhat1_store_migrate(self, top),
                    top,
                    hv1,
                    item);
            }
            // If someone else installed ptr before we did, then
            // its value will be in bucket.  Otherwise, it will
            // be in new_bucket.
            if (CAS(&ptrbucket->ptr, &bucket, new_bucket)) {
                bucket = new_bucket;
            }
        }
        // Now try to write out the hash value, without bothering to
        // check if it needs to be written.
        hv2.w1 = 0;
        hv2.w2 = 0;
        CAS(&bucket->hv, &hv2, *hv1);

        goto found_history_bucket;
    }
    return lowhat1_store_put_if_empty(lowhat1_store_migrate(self, top),
                                      top,
                                      hv1,
                                      item);

found_history_bucket:
    head = atomic_load(&bucket->head);

    if (lowhat_pflag_test(head, LOWHAT_F_MOVING)) {
        return lowhat1_store_put_if_empty(lowhat1_store_migrate(self, top),
                                          top,
                                          hv1,
                                          item);
    }

    // There's already something in this bucket, and the request was
    // to put only if the bucket is empty.
    if (lowhat_pflag_test(head->next, LOWHAT_F_USED)) {
        return false;
    }

    // Right now there's nothing in the bucket, but there might be
    // something in the bucket before we add our item, in which case
    // the CAS will fail. Or, the CAS may fail if the migrating flag
    // got set.  If there is an item there, we return false; if we see
    // a migration in progress, we go off and do that instead.

    candidate       = mmm_alloc(sizeof(lowhat_record_t));
    candidate->next = lowhat_pflag_set(head, LOWHAT_F_USED);
    candidate->item = item;
    if (!CAS(&bucket->head, &head, candidate)) {
        mmm_retire_unused(candidate);

        if (lowhat_pflag_test(head, LOWHAT_F_MOVING)) {
            return lowhat1_store_put_if_empty(lowhat1_store_migrate(self, top),
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
lowhat1_store_remove(lowhat_store_t *self,
                     lowhat_t       *top,
                     lowhat_hash_t  *hv1,
                     bool           *found)
{
    uint64_t           bix = lowhat_bucket_index(hv1, self->last_slot);
    uint64_t           i;
    lowhat_hash_t      hv2;
    lowhat_history_t  *bucket;
    lowhat_record_t   *head;
    lowhat_record_t   *candidate;
    lowhat_indirect_t *ptrbucket;

    for (i = 0; i < self->last_slot; i++) {
        ptrbucket = &self->ptr_buckets[bix];
        hv2       = atomic_load(&ptrbucket->hv);
        if (lowhat_bucket_unreserved(&hv2)) {
            break;
        }

        if (!lowhat_hashes_eq(hv1, &hv2)) {
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
        CAS(&bucket->hv, &hv2, *hv1);

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

    if (lowhat_pflag_test(head, LOWHAT_F_MOVING)) {
        return lowhat1_store_remove(lowhat1_store_migrate(self, top),
                                    top,
                                    hv1,
                                    found);
    }

    // If !head, then some write hasn't finished.
    if (!head || !(lowhat_pflag_test(head->next, LOWHAT_F_USED))) {
        goto empty_bucket;
    }

    // At this moment, there's an item there to delete. Create a
    // deletion record, and try to add it on. If we "fail", we look at
    // the record that won. If it is itself a deletion, then that
    // record did the delete, and we act like we came in after it.  If
    // it's an overwrite, then the overwrite was responsible for
    // returning the old item for memory management purposes, so we
    // return NULL and set *found to false (if requested), to indicate
    // that there's no memory management work to do.

    candidate       = mmm_alloc(sizeof(lowhat_record_t));
    candidate->next = NULL;
    candidate->item = NULL;
    if (!CAS(&bucket->head, &head, candidate)) {
        mmm_retire_unused(candidate);

        // Moving flag got set before our CAS.
        if (lowhat_pflag_test(head, LOWHAT_F_MOVING)) {
            return lowhat1_store_remove(lowhat1_store_migrate(self, top),
                                        top,
                                        hv1,
                                        found);
        }
        if (!(lowhat_pflag_test(head->next, LOWHAT_F_USED))) {
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

static lowhat_store_t *
lowhat1_store_migrate(lowhat_store_t *self, lowhat_t *top)
{
    lowhat_store_t *new_store = atomic_load(&self->store_next);
    lowhat_store_t *candidate;
    uint64_t        approx_len;
    uint64_t        new_size;

    // If we couldn't acquire a store, try to install one. If we fail, free it.

    if (!new_store) {
        approx_len = atomic_load(&self->hist_next) - self->hist_buckets;

        // The new size start off the same as the old size.
        new_size = self->last_slot + 1;

        // If the current table seems to be more than 50% full for
        // real, then double the table size.
        if (approx_len > new_size / 2) {
            new_size <<= 1;
        }

        candidate = lowhat1_store_new(new_size);
        mmm_commit_write(candidate);
        if (!CAS(&self->store_next, &new_store, candidate)) {
            lowhat1_delete_store(candidate);
        }
        else {
            new_store = candidate;
        }
    }

    lowhat1_do_migration(self, new_store);

    if (CAS(&top->store_current, &self, new_store)) {
        lowhat1_retire_store(self);
    }

    return new_store;
}

static inline void
lowhat1_do_migration(lowhat_store_t *old, lowhat_store_t *new)
{
    // cur is a pointer into the history bucket we're processing in the old
    // table. target is the bucket we're writing to in the new table.
    lowhat_history_t  *cur       = old->hist_buckets;
    lowhat_history_t  *target    = new->hist_buckets;
    lowhat_history_t  *store_end = old->hist_end;
    lowhat_record_t   *old_head;
    lowhat_record_t   *old_record;    // Head w/ migration flags removed.
    lowhat_indirect_t *ptr_bucket;    // New unordered bucket location.
    lowhat_hash_t      cur_hv;        // Hash value of the record to migrate.
    lowhat_record_t   *expected_head; // Expected values in new table are NULL.
    lowhat_hash_t      expected_hv;
    lowhat_history_t  *expected_ptr;
    uint64_t           i;
    uint64_t           bix;

    // Quickly run through every history bucket, and mark any bucket
    // that doesn't already have F_MOVING set.  Note that the CAS
    // could fail due to some other updater, so we keep CASing until
    // we know it was successful.
    while (cur < store_end) {
        old_head = atomic_load(&cur->head);
        do {
            if (lowhat_pflag_test(old_head, LOWHAT_F_MOVING)) {
                break;
            }
        } while (!CAS(&cur->head,
                      &old_head,
                      lowhat_pflag_set(old_head, LOWHAT_F_MOVING)));

        cur++;
    }

    // At this point, we're sure that any late writers will help us
    // with the migration. Therefore, we can go through each item,
    // and, if it's not fully migrated, we can attempt to migrate it.

    cur = old->hist_buckets;

    while (cur < store_end) {
        old_head = atomic_load(&cur->head);
        old_record
            = lowhat_pflag_clear(old_head, LOWHAT_F_MOVING | LOWHAT_F_MOVED);

        // If there was not an old record in this bucket, then we race
        // to set LOWHAT_F_MOVED and go on.
        if (!old_record) {
            if (!(lowhat_pflag_test(old_head, LOWHAT_F_MOVED))) {
                CAS(&cur->head,
                    &old_head,
                    lowhat_pflag_set(old_head, LOWHAT_F_MOVED));
            }
            cur++;
            continue;
        }

        // If someone beat us to the move, and there was an item present,
        // we need to update our index into both bucket lists.
        if (lowhat_pflag_test(old_head, LOWHAT_F_MOVED)) {
            if (lowhat_pflag_test(old_record->next, LOWHAT_F_USED)) {
                target++;
            }
            cur++;
            continue;
        }

        // If the record is a delete record, then try to set the moved
        // flag. If we win, then we retire the old delete record.
        if (!lowhat_pflag_test(old_record->next, LOWHAT_F_USED)) {
            if (CAS(&cur->head,
                    &old_head,
                    lowhat_pflag_set(old_head, LOWHAT_F_MOVED))) {
                mmm_retire(old_record);
            }
            cur++;
            continue;
        }

        // At this point, there's something to move, and no thread has
        // finished moving it. So we'll go through all the steps
        // necessary to move it, even though other threads might beat
        // us to any particular step. We do this, because other
        // threads may get suspended, and we want to ensure progress.
        //
        // New array starts off zero-initialized. If there's anything else
        // after any specific swap, it means we lost a race.
        expected_hv.w1 = 0;
        expected_hv.w2 = 0;
        expected_head  = NULL;

        cur_hv = atomic_load(&cur->hv);

        CAS(&target->hv, &expected_hv, cur_hv);
        CAS(&target->head, &expected_head, old_record);

        // The history records are now successfully migrated.  But we
        // still have to claim a bucket in the indirection array, and
        // point it into the ordered array.

        bix = lowhat_bucket_index(&cur_hv, new->last_slot);

        for (i = 0; i <= new->last_slot; i++) {
            ptr_bucket     = &new->ptr_buckets[bix];
            expected_hv.w1 = 0;
            expected_hv.w2 = 0;
            if (!CAS(&ptr_bucket->hv, &expected_hv, cur_hv)) {
                if (!lowhat_hashes_eq(&expected_hv, &cur_hv)) {
                    bix = (bix + 1) & new->last_slot;
                    // Someone else has this bucket; Need to keep probing.
                    continue;
                }
            }
            break; // Bucket is claimed.
        }
        // Attempt to install the pointer that points from the hashed
        // array, so that it points to target.  The only reason this
        // might fail is if another thread helping with the migration
        // succeeded.
        //
        expected_ptr = NULL;
        CAS(&ptr_bucket->ptr, &expected_ptr, target);

        // Okay, this bucket is properly set up in the destination
        // table.  We need to make sure the old bucket is updated
        // properly, by trying to add LOWHAT_F_MOVED.
        CAS(&cur->head, &old_head, lowhat_pflag_set(old_head, LOWHAT_F_MOVED));
        target++;
        cur++;
    }

    // Now that we've gone through every bucket in the old store, we
    // try to set hist_end in the new store (again, if nobody beat us
    // to it).

    expected_ptr = new->hist_buckets;
    CAS(&new->hist_next, &expected_ptr, target);

    // Now, we can swap out the top store, which is done in the caller.
}

static lowhat_view_t *
lowhat1_store_view(lowhat_store_t *self,
                   lowhat_t       *top,
                   uint64_t        epoch,
                   uint64_t       *num)
{
    lowhat_history_t *cur = self->hist_buckets;
    lowhat_history_t *end;
    lowhat_view_t    *view;
    lowhat_view_t    *p;
    lowhat_hash_t     hv;
    lowhat_record_t  *rec;
    uint64_t          sort_epoch;
    uint64_t          num_items;

    end  = atomic_load(&self->hist_next);
    view = (lowhat_view_t *)malloc(sizeof(lowhat_view_t) * (end - cur));
    p    = view;

    while (cur < end) {
        hv  = atomic_load(&cur->hv);
        rec = lowhat_pflag_clear(atomic_load(&cur->head),
                                 LOWHAT_F_MOVING | LOWHAT_F_MOVED);

        // If there's a record, we need to ensure its epoch is updated
        // before we proceed.
        mmm_help_commit(rec);

        // First, we find the top-most record that's older than (or
        // equal to) the linearization epoch.  At this point, we
        // happily will look under deletions; our goal is to just go
        // back in time until we find the right record.

        while (rec) {
            sort_epoch = mmm_get_write_epoch(rec);
            if (sort_epoch <= epoch) {
                break;
            }
            rec = lowhat_pflag_clear(rec->next, LOWHAT_F_USED);
        }

        // If the sort_epoch is larger than the epoch, then no records
        // in this bucket are old enough to be part of the linearization.
        // Similarly, if the top record is a delete record, then the
        // bucket was empty at the linearization point.
        if (!rec || sort_epoch > epoch
            || !lowhat_pflag_test(rec->next, LOWHAT_F_USED)) {
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
    view      = realloc(view, *num * sizeof(lowhat_view_t));

    // Unordered buckets should be in random order, so quicksort is a
    // good option.  Otherwise, we should use an insertion sort.
#ifdef LOWHAT_QSORT_THRESHOLD
    if (num_items >= LOWHAT_QSORT_THRESHOLD) {
        qsort(view, num_items, sizeof(lowhat_view_t), lowhat1_quicksort_cmp);
    }
    else {
        lowhat1_insertion_sort(view, num_items);
    }
#elif defined(LOWHAT_ALWAYS_USE_QSORT)
    qsort(view, num_items, sizeof(lowhat_view_t), lowhat1_quicksort_cmp);
#elif !defined(LOWHAT_DONT_SORT)
    lowhat1_insertion_sort(view, num_items);
#endif

    return view;
}

#if defined(LOWHAT_QSORT_THRESHOLD) || defined(LOWHAT_ALWAYS_USE_QSORT)
static int
lowhat1_quicksort_cmp(const void *bucket1, const void *bucket2)
{
    lowhat_view_t *item1 = (lowhat_view_t *)bucket1;
    lowhat_view_t *item2 = (lowhat_view_t *)bucket2;

    return item1->sort_epoch - item2->sort_epoch;
}
#endif

#if !defined(LOWHAT_DONT_SORT) && !defined(LOWHAT_ALWAYS_USE_QSORT)
static inline void
lowhat1_insertion_sort(lowhat_view_t *view, uint64_t num_items)
{
    uint64_t      i, j;
    lowhat_view_t swap;

    for (i = 1; i < num_items; i++) {
        swap = view[i];
        for (j = i; j > 0 && swap.sort_epoch < view[j - 1].sort_epoch; j--) {
            view[j] = view[j - 1];
        }
        view[j] = swap;
    }
}
#endif
