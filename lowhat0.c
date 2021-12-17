#include "lowhat0.h"
#include <stdio.h>

// clang-format off
static lowhat0_store_t *lowhat0_store_new(uint64_t);
static void             lowhat0_delete_store(lowhat0_store_t *);
static void            *lowhat0_store_get(lowhat0_store_t *, lowhat0_t *,
					  lowhat_hash_t *, bool *);
static void            *lowhat0_store_put(lowhat0_store_t *, lowhat0_t *,
					  lowhat_hash_t *, void *, bool *);
static bool             lowhat0_store_put_if_empty(lowhat0_store_t *,
						   lowhat0_t *,
						   lowhat_hash_t *,
						   void *);
static void            *lowhat0_store_remove(lowhat0_store_t *, lowhat0_t *,
					     lowhat_hash_t *, bool *);
static lowhat0_store_t *lowhat0_store_migrate(lowhat0_store_t *, lowhat0_t *);
static lowhat_view_t   *lowhat0_store_view(lowhat0_store_t *, lowhat0_t *,
					   uint64_t, uint64_t *);
static inline void      lowhat0_do_migration(lowhat0_store_t *,
					     lowhat0_store_t *);
static int              lowhat0_quicksort_cmp(const void *, const void *);

const lowhat_vtable_t lowhat0_vtable = {
    .init   = (lowhat_init_func)lowhat0_init,
    .get    = (lowhat_get_func)lowhat0_get,
    .put    = (lowhat_put_func)lowhat0_put,
    .remove = (lowhat_remove_func)lowhat0_remove,
    .delete = (lowhat_delete_func)lowhat0_delete,
    .len    = (lowhat_len_func)lowhat0_len,
    .view   = (lowhat_view_func)lowhat0_view
};
// clang-format on

void
lowhat0_init(lowhat0_t *self)
{
    lowhat0_store_t *store = lowhat0_store_new(1 << LOWHAT_MIN_SIZE_LOG);

    mmm_commit_write(store);
    atomic_store(&self->store_current, store);
}

// lowhat0_get() returns whatever is stored in the item field.
// Generally, we expect this to be two pointers, a key and a value.
// Meaning, when the object is NOT in the table, the return value
// will be the null pointer.
//
// When not using values (i.e., a set), it would be reasonable to
// store values directly, instead of pointers. Thus, the extra
// optional parameter to get() can tell us whether the item was
// found or not.  Set it to NULL if you're not interested.

void *
lowhat0_get(lowhat0_t *self, lowhat_hash_t *hv, bool *found)
{
    void *ret;

    mmm_start_basic_op();
    ret = lowhat0_store_get(self->store_current, self, hv, found);
    mmm_end_op();

    return ret;
}

void *
lowhat0_put(lowhat0_t     *self,
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
            = lowhat0_store_put_if_empty(self->store_current, self, hv, item);
        mmm_end_op();

        return (void *)bool_ret;
    }

    ret = lowhat0_store_put(self->store_current, self, hv, item, found);
    mmm_end_op();

    return ret;
}

void *
lowhat0_remove(lowhat0_t *self, lowhat_hash_t *hv, bool *found)
{
    void *ret;

    mmm_start_basic_op();
    ret = lowhat0_store_remove(self->store_current, self, hv, found);
    mmm_end_op();

    return ret;
}

void
lowhat0_delete(lowhat0_t *self)
{
    lowhat0_store_t   *store   = atomic_load(&self->store_current);
    lowhat0_history_t *buckets = store->hist_buckets;
    lowhat0_history_t *p       = buckets;
    lowhat0_history_t *end     = buckets + (store->last_slot + 1);
    lowhat_record_t   *rec;

    while (p < end) {
        rec = lowhat_pflag_clear(atomic_load(&p->head),
                                 LOWHAT_F_MOVED | LOWHAT_F_MOVING);
        if (rec) {
            mmm_retire_unused(rec);
        }
        p++;
    }

    lowhat0_delete_store(store);
    free(self);
}

uint64_t
lowhat0_len(lowhat0_t *self)
{
    return self->store_current->used_count - self->store_current->del_count;
}

lowhat_view_t *
lowhat0_view(lowhat0_t *self, uint64_t *num_items)
{
    lowhat_view_t *ret;
    uint64_t       epoch;

    epoch = mmm_start_linearized_op();
    ret   = lowhat0_store_view(self->store_current, self, epoch, num_items);
    mmm_end_op();

    return ret;
}

static lowhat0_store_t *
lowhat0_store_new(uint64_t size)
{
    lowhat0_store_t *store
        = (lowhat0_store_t *)mmm_alloc(sizeof(lowhat0_store_t));

    store->last_slot  = size - 1;
    store->threshold  = lowhat_compute_table_threshold(size);
    store->used_count = ATOMIC_VAR_INIT(0);
    store->del_count  = ATOMIC_VAR_INIT(0);
    store->hist_buckets
        = (lowhat0_history_t *)mmm_alloc(sizeof(lowhat0_history_t) * size);

    mmm_commit_write(store->hist_buckets);

    return store;
}

static void
lowhat0_delete_store(lowhat0_store_t *self)
{
    mmm_retire_unused(self->hist_buckets);
    mmm_retire_unused(self);
}

static void
lowhat0_retire_store(lowhat0_store_t *self)
{
    mmm_retire(self->hist_buckets);
    mmm_retire(self);
}

static void *
lowhat0_store_get(lowhat0_store_t *self,
                  lowhat0_t       *top,
                  lowhat_hash_t   *hv1,
                  bool            *found)
{
    uint64_t           bix = lowhat_bucket_index(hv1, self->last_slot);
    uint64_t           i;
    lowhat_hash_t      hv2;
    lowhat0_history_t *bucket;
    lowhat_record_t   *head;

    for (i = 0; i <= self->last_slot; i++) {
        bucket = &self->hist_buckets[bix];
        hv2    = atomic_load(&bucket->hv);
        if (lowhat_bucket_unreserved(&hv2)) {
            goto not_found;
        }
        if (!lowhat_hashes_eq(hv1, &hv2)) {
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
lowhat0_store_put(lowhat0_store_t *self,
                  lowhat0_t       *top,
                  lowhat_hash_t   *hv1,
                  void            *item,
                  bool            *found)
{
    void              *ret;
    uint64_t           bix = lowhat_bucket_index(hv1, self->last_slot);
    uint64_t           i;
    lowhat_hash_t      hv2;
    lowhat0_history_t *bucket;
    lowhat_record_t   *head;
    lowhat_record_t   *candidate;

    for (i = 0; i < self->last_slot; i++) {
        bucket = &self->hist_buckets[bix];
        hv2.w1 = 0;
        hv2.w2 = 0;
        if (!CAS(&bucket->hv, &hv2, *hv1)) {
            if (!lowhat_hashes_eq(hv1, &hv2)) {
                bix = (bix + 1) & self->last_slot;
                continue;
            }
        }
        goto found_history_bucket;
    }
    return lowhat0_store_put(lowhat0_store_migrate(self, top),
                             top,
                             hv1,
                             item,
                             found);

found_history_bucket:
    head = atomic_load(&bucket->head);

    if (lowhat_pflag_test(head, LOWHAT_F_MOVING)) {
        return lowhat0_store_put(lowhat0_store_migrate(self, top),
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
    else {
        if (atomic_fetch_add(&self->used_count, 1) >= self->threshold) {
            mmm_retire_unused(candidate);
            return lowhat0_store_put(lowhat0_store_migrate(self, top),
                                     top,
                                     hv1,
                                     item,
                                     found);
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
            return lowhat0_store_put(lowhat0_store_migrate(self, top),
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
lowhat0_store_put_if_empty(lowhat0_store_t *self,
                           lowhat0_t       *top,
                           lowhat_hash_t   *hv1,
                           void            *item)
{
    uint64_t           bix = lowhat_bucket_index(hv1, self->last_slot);
    uint64_t           i;
    lowhat_hash_t      hv2;
    lowhat0_history_t *bucket;
    lowhat_record_t   *head;
    lowhat_record_t   *candidate;

    for (i = 0; i < self->last_slot; i++) {
        bucket = &self->hist_buckets[bix];
        hv2.w1 = 0;
        hv2.w2 = 0;
        if (!CAS(&bucket->hv, &hv2, *hv1)) {
            if (!lowhat_hashes_eq(hv1, &hv2)) {
                bix = (bix + 1) & self->last_slot;
                continue;
            }
        }
        goto found_history_bucket;
    }
    return lowhat0_store_put_if_empty(lowhat0_store_migrate(self, top),
                                      top,
                                      hv1,
                                      item);

found_history_bucket:
    head = atomic_load(&bucket->head);

    if (lowhat_pflag_test(head, LOWHAT_F_MOVING)) {
        return lowhat0_store_put_if_empty(lowhat0_store_migrate(self, top),
                                          top,
                                          hv1,
                                          item);
    }

    if (head) {
        // There's already something in this bucket, and the request was
        // to put only if the bucket is empty.
        if (lowhat_pflag_test(head->next, LOWHAT_F_USED)) {
            return false;
        }
    }
    else {
        if (atomic_fetch_add(&self->used_count, 1) >= self->threshold) {
            return lowhat0_store_put_if_empty(lowhat0_store_migrate(self, top),
                                              top,
                                              hv1,
                                              item);
        }
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
            return lowhat0_store_put_if_empty(lowhat0_store_migrate(self, top),
                                              top,
                                              hv1,
                                              item);
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
lowhat0_store_remove(lowhat0_store_t *self,
                     lowhat0_t       *top,
                     lowhat_hash_t   *hv1,
                     bool            *found)
{
    uint64_t           bix = lowhat_bucket_index(hv1, self->last_slot);
    uint64_t           i;
    lowhat_hash_t      hv2;
    lowhat0_history_t *bucket;
    lowhat_record_t   *head;
    lowhat_record_t   *candidate;

    for (i = 0; i < self->last_slot; i++) {
        bucket = &self->hist_buckets[bix];
        hv2    = atomic_load(&bucket->hv);
        if (lowhat_bucket_unreserved(&hv2)) {
            break;
        }

        if (!lowhat_hashes_eq(hv1, &hv2)) {
            bix = (bix + 1) & self->last_slot;
            continue;
        }
        // Bucket is empty.
        if (!bucket->head) {
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
    head = atomic_load(&bucket->head);

    if (lowhat_pflag_test(head, LOWHAT_F_MOVING)) {
        return lowhat0_store_remove(lowhat0_store_migrate(self, top),
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
            return lowhat0_store_remove(lowhat0_store_migrate(self, top),
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

static lowhat0_store_t *
lowhat0_store_migrate(lowhat0_store_t *self, lowhat0_t *top)
{
    lowhat0_store_t *new_store = atomic_load(&self->store_next);
    lowhat0_store_t *candidate;
    uint64_t         approx_len;
    uint64_t         new_size;

    // If we couldn't acquire a store, try to install one. If we fail, free it.

    if (!new_store) {
        approx_len = self->used_count - self->del_count;

        // The new size start off the same as the old size.
        new_size = self->last_slot + 1;

        // If the current table seems to be more than 50% full for
        // real, then double the table size.
        if (approx_len > new_size / 2) {
            new_size <<= 1;
        }

        candidate = lowhat0_store_new(new_size);
        // This helps address a potential race condition, where
        // someone could drain the table after resize, having
        // us swap in the wrong length.
        atomic_store(&candidate->used_count, ~0);
        mmm_commit_write(candidate);
        if (!CAS(&self->store_next, &new_store, candidate)) {
            lowhat0_delete_store(candidate);
        }
        else {
            new_store = candidate;
        }
    }

    lowhat0_do_migration(self, new_store);

    if (CAS(&top->store_current, &self, new_store)) {
        lowhat0_retire_store(self);
    }

    return new_store;
}

static inline void
lowhat0_do_migration(lowhat0_store_t *old, lowhat0_store_t *new)
{
    lowhat0_history_t *cur;
    lowhat0_history_t *bucket;
    lowhat_record_t   *head;
    lowhat_record_t   *old_head;
    lowhat_record_t   *old_record;
    lowhat_record_t   *expected_head;
    lowhat_hash_t      hv;
    lowhat_hash_t      expected_hv;
    uint64_t           i, j;
    uint64_t           bix;
    uint64_t           new_used      = 0;
    uint64_t           expected_used = ~0;

    // Quickly run through every history bucket, and mark any bucket
    // that doesn't already have F_MOVING set.  Note that the CAS
    // could fail due to some other updater, so we keep CASing until
    // we know it was successful.
    for (i = 0; i <= old->last_slot; i++) {
        cur  = &old->hist_buckets[i];
        head = atomic_load(&cur->head);

        do {
            if (lowhat_pflag_test(head, LOWHAT_F_MOVING)) {
                break;
            }
        } while (
            !CAS(&cur->head, &head, lowhat_pflag_set(head, LOWHAT_F_MOVING)));
    }

    // At this point, we're sure that any late writers will help us
    // with the migration. Therefore, we can go through each item,
    // and, if it's not fully migrated, we can attempt to migrate it.

    for (i = 0; i <= old->last_slot; i++) {
        cur      = &old->hist_buckets[i];
        old_head = atomic_load(&cur->head);
        old_record
            = lowhat_pflag_clear(old_head, LOWHAT_F_MOVING | LOWHAT_F_MOVED);

        if (!old_record) {
            if (!(lowhat_pflag_test(old_head, LOWHAT_F_MOVED))) {
                CAS(&cur->head,
                    &old_head,
                    lowhat_pflag_set(old_head, LOWHAT_F_MOVED));
            }
            continue;
        }

        if (lowhat_pflag_test(old_head, LOWHAT_F_MOVED)) {
            if (lowhat_pflag_test(old_record->next, LOWHAT_F_USED)) {
                new_used++;
            }
            continue;
        }

        if (!lowhat_pflag_test(old_record->next, LOWHAT_F_USED)) {
            if (CAS(&cur->head,
                    &old_head,
                    lowhat_pflag_set(old_head, LOWHAT_F_MOVED))) {
                mmm_retire(old_record);
            }
            continue;
        }

        hv  = atomic_load(&cur->hv);
        bix = lowhat_bucket_index(&hv, new->last_slot);
        new_used++;

        for (j = 0; j <= new->last_slot; j++) {
            bucket         = &new->hist_buckets[bix];
            expected_hv.w1 = 0;
            expected_hv.w2 = 0;
            if (!CAS(&bucket->hv, &expected_hv, hv)) {
                if (!lowhat_hashes_eq(&expected_hv, &hv)) {
                    bix = (bix + 1) & new->last_slot;
                    continue;
                }
            }
            break;
        }

        expected_head = NULL;
        CAS(&bucket->head, &expected_head, old_record);
        CAS(&cur->head, &old_head, lowhat_pflag_set(old_head, LOWHAT_F_MOVED));
    }

    CAS(&new->used_count, &expected_used, new_used);

    // Now, we can swap out the top store, which is done in the caller.
}

static lowhat_view_t *
lowhat0_store_view(lowhat0_store_t *self,
                   lowhat0_t       *top,
                   uint64_t         epoch,
                   uint64_t        *num)
{
    lowhat0_history_t *cur = self->hist_buckets;
    lowhat0_history_t *end;
    lowhat_view_t     *view;
    lowhat_view_t     *p;
    lowhat_hash_t      hv;
    lowhat_record_t   *rec;
    uint64_t           sort_epoch;
    uint64_t           num_items;

    end  = self->hist_buckets + (self->last_slot + 1);
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
    // good option.
    qsort(view, num_items, sizeof(lowhat_view_t), lowhat0_quicksort_cmp);

    return view;
}

static int
lowhat0_quicksort_cmp(const void *bucket1, const void *bucket2)
{
    lowhat_view_t *item1 = (lowhat_view_t *)bucket1;
    lowhat_view_t *item2 = (lowhat_view_t *)bucket2;

    return item1->sort_epoch - item2->sort_epoch;
}
