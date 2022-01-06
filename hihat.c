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
 *  Name:           hihat.c
 *  Description:    Half-Interesting HAsh Table.
 *                  This is a lock-free hash table, with wait-free
 *                  read operations. This table allows for you to
 *                  recover the approximate order when getting a view,
 *                  but does not guarantee the consistency of that view.
 *
 *  Author:         John Viega, john@zork.org
 *
 */

#include "hihat.h"

// clang-format off
static hihat_store_t *hihat_store_new    (uint64_t);
static void          *hihat_store_get    (hihat_store_t *, hihat_t *,
					  hatrack_hash_t *, bool *);
static void          *hihat_store_put    (hihat_store_t *, hihat_t *,
					  hatrack_hash_t *, void *, bool *);
static void          *hihat_store_replace(hihat_store_t *, hihat_t *,
					  hatrack_hash_t *, void *, bool *);
static bool           hihat_store_add    (hihat_store_t *, hihat_t *,
					  hatrack_hash_t *, void *);
static void          *hihat_store_remove (hihat_store_t *, hihat_t *,
					  hatrack_hash_t *, bool *);
static hihat_store_t *hihat_store_migrate(hihat_store_t *, hihat_t *);

/* hihat_init()
 *
 * For the definition of HATRACK_MIN_SIZE, this is computed in
 * config.h, since we require hash table buckets to always be sized to
 * a power of two. To set the size, you instead set the preprocessor
 * variable HATRACK_MIN_SIZE_LOG.
 */
void
hihat_init(hihat_t *self)
{
    hihat_store_t *store;

    store            = hihat_store_new(HATRACK_MIN_SIZE);
    self->next_epoch = 1; // 0 is reserved for empty buckets.
    
    atomic_store(&self->store_current, store);
    atomic_store(&self->item_count, 0);
}

/* hihat_get(), _put(), _replace(), _add(), _remove()
 *
 * Our core operations need to safely acquire a reference to the current
 * store, before looking for the hash value in the store, to make sure
 * they don't end up dereferencing a pointer to memory that's been
 * freed. We do so, by using our memory management implementation,
 * MMM.
 *
 * Specifically, for the sake of hihat, MMM makes sure that, when
 * stores are "retired", they are not freed until all threads are done
 * with them.
 *
 * This works by the thread registering a "reservation" via
 * mmm_start_basic_op().  This essentially tells the memory management
 * system that any record created by MMM that is CURRENTLY alive might
 * be held by this thread, along with any future record created.
 *
 * As long as the thread holds a reservation, nothing allocated via
 * mmm will get freed out from under it.  It does need to be a good
 * citizen, and let go of the reservation when it's done with the
 * operation.
 *
 * This has the same impact as hazard pointers, but is faster, and
 * less error-prone. We primarily need to make sure to always
 * register/de-register, and to not "retire" things before we've
 * installed a replacement.
 *
 * A bit more detail, for the interested: 
 *
 * MMM keeps a global, atomically updated counter of memory
 * "epochs". Each write operation starts a new epoch. Each memory
 * object records its "write" epoch, as well as its "retire" epoch,
 * meaning the epoch in which mmm_retire() was called.
 *
 * The way mmm protects from freeing data that might be in use by
 * parallel threads, is as follows:
 *
 * 1) All threads "register" by writing the current epoch into a
 *    special array, when they start an operation.  This happens when
 *    calling mmm_start_basic_op(), which is inlined and defined in
 *    mmm.h. Again, the algorithm will ensure that, if a thread has
 *    registered for an epoch, no values from that epoch onward will
 *    be deleted, while that reservation is held. It does this by
 *    walking the reservations before beginning to free things, and
 *    only freeing those things older than any current registration.
 *
 * 2) When the operation is done, they "unregister", via mmm_end_op().
 *    Obviously, if this is forgotten, you can end up leaking records
 *    that never get cleaned up (they're not technically leaked, but
 *    they're still live, and taking up memory that will never again
 *    get used).
 *
 * 3) When mmm_retire() is called on a pointer, the "retire" epoch is
 *    stored (in a hidden header). The cell is placed on a thread
 *    specific list, and is never immediately freed.
 *
 * 4) Periodically, each thread goes through its retirement list,
 *    looking at the retirement epoch.  If there are no threads that
 *    have registered an epoch requiring the pointer to be alive, then
 *    the value can be safely freed.
 *
 * There are more options with mmm, that we don't use in this
 * algorithm. See mmm.h and mmm.c for more details on the algorithm,
 * and options (start with the .h file).
 *
 * Once the reference is required, we delegate to the function
 * hihat_store_<whatever_is_appropriate>() to do the work. Note that
 * the API (including use of the found parameter) works as with every
 * other hash table; see refhat.c or swimcap.c for more details on the
 * parameters, if needed.
 */
void *
hihat_get(hihat_t *self, hatrack_hash_t *hv, bool *found)
{
    void           *ret;
    hihat_store_t *store;

    mmm_start_basic_op();
    store = atomic_read(&self->store_current);
    ret   = hihat_store_get(store, self, hv, found);
    mmm_end_op();

    return ret;
}

void *
hihat_put(hihat_t *self, hatrack_hash_t *hv, void *item, bool *found)
{
    void           *ret;
    hihat_store_t *store;

    mmm_start_basic_op();
    store = atomic_read(&self->store_current);
    ret   = hihat_store_put(store, self, hv, item, found);
    mmm_end_op();

    return ret;
}

void *
hihat_replace(hihat_t *self, hatrack_hash_t *hv, void *item, bool *found)
{
    void           *ret;
    hihat_store_t *store;

    mmm_start_basic_op();
    store = atomic_read(&self->store_current);
    ret   = hihat_store_replace(store, self, hv, item, found);
    mmm_end_op();

    return ret;
}

bool
hihat_add(hihat_t *self, hatrack_hash_t *hv, void *item)
{
    bool            ret;
    hihat_store_t *store;

    mmm_start_basic_op();
    store = atomic_read(&self->store_current);    
    ret   = hihat_store_add(store, self, hv, item);
    mmm_end_op();

    return ret;
}

void *
hihat_remove(hihat_t *self, hatrack_hash_t *hv, bool *found)
{
    void           *ret;
    hihat_store_t *store;

    mmm_start_basic_op();
    store = atomic_read(&self->store_current);    
    ret   = hihat_store_remove(store, self, hv, found);
    mmm_end_op();

    return ret;
}

/*
 * hihat_delete()
 *
 * Deletes a hihat object. Generally, you should be confident that
 * all threads except the one from which you're calling this have
 * stopped using the table (generally meaning they no longer hold a
 * reference to the store).
 *
 * Note that this function assumes the hihat object was allocated
 * via the default malloc. If it wasn't, don't call this directly, but
 * do note that the stores were created via mmm_alloc(), and the most
 * recent store will need to be retired via mmm_retire(). 
 */
void
hihat_delete(hihat_t *self)
{
    mmm_retire(atomic_load(&self->store_current));
    free(self);
}

/* hihat_len()
 *
 * Returns the approximate number of items currently in the
 * table. Note that we strongly discourage using this call, since it
 * is close to meaningless in multi-threaded programs, as the value at
 * the time of check could be dramatically different by the time of
 * use.
 */
uint64_t
hihat_len(hihat_t *self)
{
    return atomic_read(&self->item_count);
}

/* hihat_view()
 *
 * This returns an array of hatrack_view_t items, representing all of
 * the items in the hash table, for the purposes of iterating over the
 * items, for any reason. The number of items in the view will be
 * stored in the memory address pointed to by the second parameter,
 * num. If the third parameter (sort) is set to true, then quicksort
 * will be used to sort the items in the view, based on the insertion
 * order.
 *
 * Note that the view provided here could be an "inconsistent" view,
 * meaning that it might not capture the state of the hash table at a
 * given moment in time. In the case of heavy writes, it probably will
 * not.
 *
 * The contents of individual buckets will always be independently
 * consistent; we will see them atomically.  But relative to each
 * other, there can be issues.
 *
 * For instance, imagine there are two threads, one writing, and one
 * creating a view.
 *
 * The writing thread might do the following ordered operations:
 *
 * 1) Add item A, giving us the state:    { A }
 * 2) Add item B, giving us the state:    { A, B }
 * 3) Remove item A, giving us the state: { B }
 * 4) Add item C, giving us the state:    { B, C }
 *
 * The viewing thread, going through the bucket in parallel, might
 * experience the following:
 *
 * 1) It reads A, sometime after write event 1, but before event 2.
 * 2) It reads the bucket B will end up in, before B gets there.
 * 3) The viewer going slowly, B gets written, then C gets written.
 * 4) The viewer reads C.
 *
 * The resulting view is { A, C }, which was never the state of the 
 * hash table in any logical sense.
 *
 * Similarly, we could end up with { A, B, C }, another incorrect
 * view.
 *
 * Many applications won't care about this problem, but some will (for
 * instance, when one needs to do set operations atomically). For such
 * applications, see the lohat and woolhat implementations, where we
 * solve that problem, and provide full consistency.
 */
hatrack_view_t *
hihat_view(hihat_t *self, uint64_t *num, bool sort)
{
    hatrack_view_t  *view;
    hatrack_view_t  *p;
    hihat_bucket_t *cur;
    hihat_bucket_t *end;
    hihat_record_t  record;
    uint64_t         num_items;
    uint64_t         alloc_len;
    uint64_t         record_epoch;
    hihat_store_t  *store;

    /* Again, we need to do this before grabbing our pointer to the
     * store, to make sure it doesn't get deallocated out from under
     * us. We also have to call mmm_end_op() when done.
     *
     * That is to say, a migration could be in progress at any time
     * during this view operation.  As a reader, we can safely ignore
     * the migration in all cases, and just work from the current
     * store.
     */
    mmm_start_basic_op();

    store     = atomic_read(&self->store_current);
    alloc_len = sizeof(hatrack_view_t) * (store->last_slot + 1);
    view      = (hatrack_view_t *)malloc(alloc_len);
    p         = view;
    cur       = store->buckets;
    end       = cur + (store->last_slot + 1);

    while (cur < end) {
        record       = atomic_read(&cur->record);
        record_epoch = record.info & HIHAT_EPOCH_MASK;

	// If there's no epoch in the record, then the bucket was
	// empty, and we skip it.
	if (!record_epoch) {
	    cur++;
	    continue;
	}

	p->sort_epoch = record_epoch;
        p->item       = record.item;
	
        p++;
        cur++;
    }

    num_items = p - view;
    *num      = num_items;

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

/* New stores get allocated with mmm_alloc_committed. As a result, the
 * underlying memory is zeroed out, so we only need to initialize
 * non-zero items.
 */
static hihat_store_t *
hihat_store_new(uint64_t size)
{
    hihat_store_t *store;
    uint64_t        alloc_len;

    alloc_len = sizeof(hihat_store_t) + sizeof(hihat_bucket_t) * size;
    store     = (hihat_store_t *)mmm_alloc_committed(alloc_len);

    store->last_slot  = size - 1;
    store->threshold  = hatrack_compute_table_threshold(size);

    return store;
}

/*
 * Getting is really straightforward, especially since our caller
 * deals with the mmm registration / deregistration.
 *
 * We can go directly to buckets and read their contents, and not have
 * to worry about other readers or writers, at all.
 *
 * Note that atomic_read() is a macro that calls the C11 atomic_ API
 * to load memory, using "relaxed" memory ordering. That means we only
 * care about the contents being in a consistent state (i.e., that
 * they are not half-updated). 
 *
 * But, if there are parallel store operations happening, we really do
 * not care about the order in which they land, relative to our read.
 *
 * That is, we are explicitly loading a variable declared _Atomic,
 * without any memory barriers. atomic_load(), in constrast, provides
 * a full memory barrier.
 *
 * To understand why we can do with such relaxed ordering, consider
 * each of the two things we might care about in a bucket, the hash
 * value, and the associated record.
 *
 * If we are racing the thread that ends up writing out the bucket's
 * hash value, we might read an empty bucket, in which case we came
 * before the bucket got reserved, and we consider the associated hash
 * value as not present in the table.
 *
 * If, on the other hand, we successfully read the hash value, great!
 * We'll move on to reading the contents, where again, we might be
 * racing, either with the first write, or with subsequent writes.
 *
 * If we beat the first writer, that's the same to us as a miss... we
 * were earlier, so we should see nothing.  After that, we don't
 * really care which value we get in a race; we conceptually order our
 * read on whichever side of the write makes sense, depending on the
 * value we got.
 */
static void *
hihat_store_get(hihat_store_t *self,
                 hihat_t       *top,
                 hatrack_hash_t *hv1,
                 bool           *found)
{
    uint64_t         bix;
    uint64_t         i;
    hatrack_hash_t   hv2;
    hihat_bucket_t *bucket;
    hihat_record_t  record;

    bix = hatrack_bucket_index(hv1, self->last_slot);
    
    for (i = 0; i <= self->last_slot; i++) {
        bucket = &self->buckets[bix];
        hv2    = atomic_read(&bucket->hv);
        if (hatrack_bucket_unreserved(&hv2)) {
            goto not_found;
        }
        if (!hatrack_hashes_eq(hv1, &hv2)) {
            bix = (bix + 1) & self->last_slot;
            continue;
        }

        record = atomic_read(&bucket->record);
        if (record.info & HIHAT_EPOCH_MASK) {
            if (found) {
                *found = true;
            }
            return record.item;
        }
        break;
    }
not_found:
    if (found) {
        *found = false;
    }
    return NULL;
}

/*
 * Write operations are a little less straightforward without locks.
 *
 * Once hash values are written out, they don't change, but it's
 * certainly possible for two different threads to try to stake a
 * claim on a bucket in parallel.
 *
 * As a result, if we try to claim a bucket by writing in the hash,
 * and we fail, we need to check to see if the value that DID get
 * written is our value. If it is, we proceed to write into the
 * bucket.
 *
 * Similarly, when we go to write out a value, we might have other
 * threads trying to write out the result of some operation on the
 * same key.
 *
 * In that case, we have two options:
 *
 * 1) We can keep trying to write, until we succeed.
 *
 * 2) We can conceptually order our operation BEFORE the operation that
 *    succeeded, and give up, considering ourselves overwritten.
 *
 * We implement option #2 here. Option #1 is more expensive, and,
 * without additional effort, ensures that the operation cannot be
 * wait free (in hihat, if a put operation does not involve a resize,
 * then it is wait-free).
 *
 * Note that the biggest oddity of this approach is how we handle
 * memory management for the old value. When there is no contention,
 * this function returns the previous value (if any), so that the
 * caller can be responsible for freeing it.
 * 
 * In this case, we need the caller to free the item we passed in, so
 * we claim success in writing, but return our input for the sake of
 * memory management.
 *
 * Note that we use gotos a bit liberally here, to help clearly
 * separate bucket acquisition from the writing, and keep the pieces
 * smaller and more easily digestable.
 */
static void *
hihat_store_put(hihat_store_t *self,
                 hihat_t       *top,
                 hatrack_hash_t *hv1,
                 void           *item,
                 bool           *found)
{
    void            *old_item;
    bool             new_item;
    uint64_t         bix;
    uint64_t         i;
    hatrack_hash_t   hv2;
    hihat_bucket_t *bucket;
    hihat_record_t  record;
    hihat_record_t  candidate;

    bix = hatrack_bucket_index(hv1, self->last_slot);
    
    for (i = 0; i <= self->last_slot; i++) {
        bucket = &self->buckets[bix];
	/* We load the current hash value and check it, making sure
	 * it's empty before we attempt to write out out value.
	 * We *could* assume the bucket is empty, and attempt to 
	 * swap into it, then just move on if the swap fails. 
	 *
	 * In my testing, it's generally significantly more performant
	 * to do the extra check, and NOT CAS the hash value every
	 * time.  The alternative involves a load anyway, since we
	 * have to load 0's into the expected value (hv2), in that
	 * case.
	 */
	hv2    = atomic_read(&bucket->hv);
	if (hatrack_bucket_unreserved(&hv2)) {
	    /* Note that our compare-and-swap macro is defined in hatomic.h.
	     * It includes a facility where, if we have counters turned on,
	     * we can keep track of how often individual operations succed
	     * or fail. 
	     *
	     * When those facilities are off, the macros expand
	     * directly to a C11 atomic_compare_exchange_strong()
	     * operation.
	     */
	    if (LCAS(&bucket->hv, &hv2, *hv1, HIHAT_CTR_BUCKET_ACQUIRE)) {
		/* Our resize metric. If this insert puts us at the
		 * 75% mark, then we need to resize. If this CAS
		 * fails, we're not contributing to filling up the
		 * table, because we're going to try to write over an
		 * old record.  So we don't need to check on fail.
		 */
		if (atomic_fetch_add(&self->used_count, 1) >= self->threshold) {
		    goto migrate_and_retry;
		}
		goto found_bucket;
	    }
	}
	if (!hatrack_hashes_eq(hv1, &hv2)) {
	    bix = (bix + 1) & self->last_slot;
	    continue;
	}
        goto found_bucket;
    }
    /* If we manage to visit every bucket, without finding a place to
     * put things, then we have multiple competing writes, and the
     * table got full, fast. We need to resize, to try to make room,
     * and then try again via a recursive call.
     */
 migrate_and_retry:
    self = hihat_store_migrate(self, top);
    return hihat_store_put(self, top, hv1, item, found);

 found_bucket:
    /* Once we've found the right bucket, we are not quite ready to
     * try to write to it. First, we need to check to make sure that
     * this bucket isn't marked for migration.  If it is so marked,
     * then we need to help with the resizing, then try the operation
     * again.
     */
    record = atomic_read(&bucket->record);
    if (record.info & HIHAT_F_MOVING) {
	goto migrate_and_retry;
    }

    /*
     * The way we tell if there's an actual item in this bucket is by
     * extracting the "epoch" (essentially a timestamp), and seeing if
     * it is non-zero.
      */
    if (record.info & HIHAT_EPOCH_MASK) {
	if (found) {
	    *found = true;
	}
	/* We need to remember for later a couple of things:
	 * 1) The old item, so that we can return it, but only
         *    if our CAS is successful, and
	 *
	 * 2) Whether or not this is a 'new item', If our CAS is
	 *    successful. The thread that successfully adds the new
	 *    item becomes responsible for updating the approximate
	 *    item count.
	 *
	 * Plus, when setting up the record we want to swap in, since
	 * there's already a value, we want to preserve its write
	 * epoch for the purposes of sorting by insertion time.
	 */
	old_item       = record.item;
	new_item       = false;
	candidate.info = record.info;
    }
    else {
	if (found) {
	    *found = false;
	}
	/* In this branch, we grab a new epoch for our (hopefully) 
	 * new item. The epoch will go unused if we don't win, and
	 * that's just fine. 
	 * 
	 * On the flip side, multiple threads are creating epochs in
	 * parallel, without ensuring atomicity. That means that more
	 * than one item could end up with the same epoch.
	 *
	 * Since we aren't requiring full consistency for our views in
	 * hihat, that doesn't matter to us. If we care about
	 * consistency, lohat (and woolhat) solve this problem.
	 */
	old_item       = NULL;
	new_item       = true;
	candidate.info = top->next_epoch++;
    }

    candidate.item = item;

    if (LCAS(&bucket->record, &record, candidate, HIHAT_CTR_REC_INSTALL)) {
        if (new_item) {
            atomic_fetch_add(&top->item_count, 1);
        }
        return old_item;
    }

    /* Above, we said if we lose the race to swap in a new value, we
     * pretend that we failed. However, we could lose the race not
     * because we failed, but because a migration has started-- some
     * other thread might have changed our bucket to signal the
     * migration happening. In that case, we are obliged to help with
     * the migration, before retrying our operation.
     */
    if (record.info & HIHAT_F_MOVING) {
	goto migrate_and_retry;
    }

    return item;
}

/*
 * Our replace operation will look incredibly similar to our put
 * operation, with the obvious exception that we bail if we ever find
 * that the bucket is empty.  See comments above on hihat_store_put
 * for an explaination of the general algorithm; here we limit
 * ourselves to concerns (somewhat) specific to a replace operation.
 */
static void *
hihat_store_replace(hihat_store_t *self,
		     hihat_t       *top,
		     hatrack_hash_t *hv1,
		     void           *item,
		     bool           *found)
{
    void            *old_item;
    uint64_t         bix;
    uint64_t         i;
    hatrack_hash_t   hv2;
    hihat_bucket_t *bucket;
    hihat_record_t  record;
    hihat_record_t  candidate;

    bix = hatrack_bucket_index(hv1, self->last_slot);
    
    for (i = 0; i <= self->last_slot; i++) {
        bucket = &self->buckets[bix];
	hv2    = atomic_read(&bucket->hv);
	if (hatrack_bucket_unreserved(&hv2)) {
	    goto not_found;
	}
	if (!hatrack_hashes_eq(hv1, &hv2)) {
	    bix = (bix + 1) & self->last_slot;
	    continue;
	}
        goto found_bucket;
    }

 not_found:
    if (found) {
	*found = false;
    }
    return NULL;

 found_bucket:
    record = atomic_read(&bucket->record);
    if (record.info & HIHAT_F_MOVING) {
    migrate_and_retry:
	self = hihat_store_migrate(self, top);
	return hihat_store_replace(self, top, hv1, item, found);
    }

    if (!record.info) {
	goto not_found;
    }

    old_item       = record.item;
    candidate.item = item;
    candidate.info = record.info;

    /* If we lose the compare and swap (and since we're not storing
     * history data, as we will see in other algorithms), we have two
     * options; we can pretend we overwrote something, and return
     * ourself as the thing we overwrote for the sake of memory
     * management, or we can loop here, until the CAS succeeds, and
     * decide what to do once the CAS succeeds.  We could also take
     * the stance, in case of a failure, that there was nothing to
     * overwrite; from a linearization perspective, it's as if the
     * write that beat us was a remove followed by a write, and our
     * operation came in between the two.
     *
     * In this implementation, we'll do the CAS loop, since we don't
     * have history, and we want the results to make some intuitive
     * sense.
     *
     * When implementing this method for wait-free hash tables, we
     * choose a different approach, for the sake of making it less
     * complicated to make the algorithm wait-free. See witchhat.c;
     * this issue is one of the two real differences between hihat
     * and witchhat.
     */    
    while(!LCAS(&bucket->record, &record, candidate, HIHAT_CTR_REC_INSTALL)) {
	if (record.info & HIHAT_F_MOVING) {
	    goto migrate_and_retry;
	}
	if (!record.info) {
	    goto not_found;
	}
    }
    
    if (found) {
	*found = true;
    }

    return record.item;
}

/*
 * Again, see hihat_store_put() for the algorithmic info; this is ony
 * different in that it only puts an item in the table if there is
 * nothing in the bucket at the time. The second we see something is
 * definitely in the bucket (and not deleted), we can bail.
 *
 * Since we never replace anything, there's never any memory
 * management for the caller to do, and we can directly return whether
 * we were successful or not, instead of relying on a 'found'
 * parameter being passed.
 */
static bool
hihat_store_add(hihat_store_t *self,
		 hihat_t       *top,
		 hatrack_hash_t *hv1,
		 void           *item)
{
    uint64_t         bix;
    uint64_t         i;
    hatrack_hash_t   hv2;
    hihat_bucket_t *bucket;
    hihat_record_t  record;
    hihat_record_t  candidate;

    bix = hatrack_bucket_index(hv1, self->last_slot);
    
    for (i = 0; i <= self->last_slot; i++) {
        bucket = &self->buckets[bix];
	hv2    = atomic_read(&bucket->hv);
	if (hatrack_bucket_unreserved(&hv2)) {
	    if (LCAS(&bucket->hv, &hv2, *hv1, HIHAT_CTR_BUCKET_ACQUIRE)) {
		if (atomic_fetch_add(&self->used_count, 1) >= self->threshold) {
		    goto migrate_and_retry;
		}
		goto found_bucket;
	    }
	}
	if (!hatrack_hashes_eq(hv1, &hv2)) {
	    bix = (bix + 1) & self->last_slot;
	    continue;
	}
	
        goto found_bucket;
    }

 migrate_and_retry:
    self = hihat_store_migrate(self, top);
    return hihat_store_add(self, top, hv1, item);

found_bucket:
    record = atomic_read(&bucket->record);
    if (record.info & HIHAT_F_MOVING) {
	goto migrate_and_retry;
    }
    if (record.info) {
        return false;
    }

    candidate.item = item;
    candidate.info = top->next_epoch++;

    if (LCAS(&bucket->record, &record, candidate, HIHAT_CTR_REC_INSTALL)) {
	atomic_fetch_add(&top->item_count, 1);
        return true;
    } 
    if (record.info & HIHAT_F_MOVING) {
	goto migrate_and_retry;
    }

    // Since we don't allow double deletions, at some point
    // since we loaded record, there was an item there, so we
    // order our add appropriately, and fail.
    return false;
}

/*
 * The logic for bucket acquisition is the same as with a get(), since
 * we don't need to go any farther the second we see a bucket is
 * empty.
 *
 * The write logic is the same as if we were writing out an actual
 * value. And, as with put and replace operations, we return any
 * previous value for the sake of memory management.
 */
static void *
hihat_store_remove(hihat_store_t *self,
                    hihat_t       *top,
                    hatrack_hash_t *hv1,
                    bool           *found)
{
    void            *old_item;
    uint64_t         bix;
    uint64_t         i;
    hatrack_hash_t   hv2;
    hihat_bucket_t *bucket;
    hihat_record_t  record;
    hihat_record_t  candidate;

    bix = hatrack_bucket_index(hv1, self->last_slot);
    
    for (i = 0; i <= self->last_slot; i++) {
        bucket = &self->buckets[bix];
        hv2    = atomic_read(&bucket->hv);
        if (hatrack_bucket_unreserved(&hv2)) {
            break;
        }
        if (!hatrack_hashes_eq(hv1, &hv2)) {
            bix = (bix + 1) & self->last_slot;
            continue;
        }

        goto found_bucket;
    }

    if (found) {
        *found = false;
    }

    return NULL;

found_bucket:
    record = atomic_read(&bucket->record);
    if (record.info & HIHAT_F_MOVING) {
    migrate_and_retry:
	self = hihat_store_migrate(self, top);
	return hihat_store_remove(self, top, hv1, found);
    }
    if (!record.info) {
        if (found) {
            *found = false;
        }

        return NULL;
    }

    old_item       = record.item;
    candidate.item = NULL;
    candidate.info = 0;

    if (LCAS(&bucket->record, &record, candidate, HIHAT_CTR_DEL)) {
        atomic_fetch_sub(&top->item_count, 1);

        if (found) {
            *found = true;
        }
        return old_item;
    }
    
    if (record.info & HIHAT_F_MOVING) {
	goto migrate_and_retry;
    }

    if (found) {
        *found = false;
    }
    return NULL;
}

/* 
 * This function gets called whenever a thread notices that a
 * migration is necessary.
 *
 * Some threads may come here, because they see we are using
 * approx. 75% of the available buckets in the table.
 *
 * But, in writer threads that are overwriting an item, we don't want
 * them changing the table state after we've started migrating items,
 * so our first order of business is to visit each bucket, and signal
 * that a migration is in progress, by setting the flag
 * HIHAT_F_MOVING.
 *
 * The flag effectively locks all writes, but instead of sitting
 * around and waiting for the migration to occur, writer threads
 * attempt to help with the migration.
 *
 * We have all writer threads help with the migration because threads
 * can stall due to the scheduler, and we don't want any thread to be
 * dependent on any other thread's progress. Therefore, every thread
 * that notices that a migration is in progress, goes through the
 * ENTIRE process of attempting to migrate. If they see that some
 * other thread completed some work, they abandon that particular
 * piece of the migration, and move on to the next piece.
 *
 * In practice (in my testing), when multiple threads are writing
 * simultaneously, they generally do end up all contributing. Let's
 * say that one thread gets a head start, and migrates 10% of the
 * buckets, before another writer comes along. The late writer
 * checking those 10% of the buckets to see if there's work is fast
 * compared to the migration of a bucket, so the late writer tends to
 * catch up, then the two threads play a bit of leapfrog, each
 * migrating approximately their share of the buckets (with some
 * significant variance depending on thread scheudling).
 *
 * Anyway, once we prevent all writes by writing HIHAT_F_MOVING to
 * each bucket, migraters can properly cycle back through all buckets,
 * and attempt to migrate them. Again, the only reason they might fail
 * is if another writer is being successful.
 *
 * As a result of this approach, the migration itself is lock-free.
 * The only reason we might "spin" on a CAS loop is if late writers
 * manage to update the value of a bucket, while we're waiting to
 * write HIHAT_CTR_F_MOVING.  While the number of threads may be
 * bounded, if threads keep writing out the same value over and over,
 * it's conceptually possible for them to never notice that a
 * migration is happening.
 *
 * In practice, this doesn't happen -- I can't even make it happen
 * when designing workloads explcitily designed to trigger the
 * condition (the most attempts I've ever seen to write this value out
 * is 6). Still, we will address this issue in witchhat, using a
 * 'helping' mechanism to make the algorithm truly wait-free.
 *
 * Note that, for those familiar with the Cliff Click lock-free hash
 * table, in his table, writes can happen in parallel to a
 * migration-in-progress, resulting in nested migrations (where, for
 * example, during the migration from store 1 to store 2, the
 * migration from store 2 to store 3 begins). In our table, that is
 * not possible -- no thread will write to a bucket that causes the
 * table to hit the 75% threshold in either table, until the migration
 * is confirmed complete (though, it is possible for a thread to
 * suspend during the migration of store 1, and not wake up until the
 * some subsequent migration is in progress -- in that case, MMM
 * ensures the old buckets are still readable. But the thread will
 * quickly determine there was no work, and move on to the topmost
 * table.
 * 
 * A major difference here is that, when there are many simultaneous
 * writers, Click only has late writers help a bit, then go work in
 * the new table. That's valid, but is more likely to invert intended
 * insertion orders, and make the job harder for readers, as they
 * might need to switch stores (I believe he has readers jump stores
 * if the item gets a new insertion in the newer table).
 *
 * That leads to a much more complicated algorithm. Even though our
 * approach might seem like it's duplicating a lot more effort, it
 * will tend to finish the migration at least as quickly as Click's
 * approach, because it's robust to threads that are carrying the
 * burden of the migrating getting suspended by the scheduler.
 *
 * We do experiment with a hybrid approach in hihat_a -- where
 * late-arriving threads wait a bit before starting to help. In our
 * inital testing, that can reduce the number of cycles used, but
 * there doesn't seem to be a consistent improvement in time to
 * migrate. In fact, on first glance, it seems like the "everyone do
 * as much work as possible" approach probably finishes quicker on
 * average.
 */
static hihat_store_t *
hihat_store_migrate(hihat_store_t *self, hihat_t *top)
{
    hihat_store_t  *new_store;
    hihat_store_t  *candidate_store;
    uint64_t         new_size;
    hihat_bucket_t *bucket;
    hihat_bucket_t *new_bucket;
    hihat_record_t  record;
    hihat_record_t  candidate_record;
    hihat_record_t  expected_record;
    hatrack_hash_t   expected_hv;
    hatrack_hash_t   hv;
    uint64_t         i, j;
    uint64_t         bix;
    uint64_t         new_used;
    uint64_t         expected_used;

    
    /* If we're a late-enough writer, let's just double check to see if 
     * we need to help at all.
     */
    new_store = atomic_read(&top->store_current);
    
    if (new_store != self) {
	return new_store;
    }
    
    new_used  = 0;
    
    /* Quickly run through every history bucket, and mark any bucket
     * that doesn't already have F_MOVING set.  Note that the CAS
     * could fail due to some other updater, so we keep CASing until
     * we know it was successful.
     *
     * Note that we COULD go bucket-by-bucket, lock the bucket, and
     * migrate this bucket, before locking the next bucket. That would
     * be an equally valid approach that doesn't spoil the consistency
     * of anything, and we'd only make one pass through the array,
     * instead of two passes (we mark buckets as migrated in the same
     * pass as the one where we do the actual migration).
     *
     * However, as with the Click algorithm, that would lead to some
     * explicit inversion of write order. With our approach, writers
     * that migrate will tend to caravan together, and the resulting
     * writes will be less inverted; closer to random.
     *
     * I may explore the performance impact of moving this to one pass
     * at some future date, but for now, I'm going to continue to
     * assume that the most of migration will amortize out to
     * something pretty low in all cases.
     */
    for (i = 0; i <= self->last_slot; i++) {
        bucket                = &self->buckets[i];
        record                = atomic_read(&bucket->record);
        candidate_record.item = record.item;

        do {
            if (record.info & HIHAT_F_MOVING) {
                break;
            }
	    /* Note that, if record.info is zero, the bucket is either
	     * deleted, or not written to yet. We can declare the
	     * migration of this bucket successful now, instead of 
	     * doing a second value check later on, saving everyone
	     * a small bit of work.
	     */
	    if (record.info) {
		candidate_record.info = record.info | HIHAT_F_MOVING;
	    } else {
		candidate_record.info = HIHAT_F_MOVING | HIHAT_F_MOVED;
	    }
        } while (!LCAS(&bucket->record,
                       &record,
                       candidate_record,
                       HIHAT_CTR_F_MOVING));

	/* Here, whether we installed our record or not, we look at
	 * the old record to count how many items we're going to
	 * migrate, so we can try to install the value in the new
	 * store when we're done, as used_count.
	 *
	 * Since used_count is part of our resize metric, we want this
	 * to stay as accurate as possible, which is why we don't just
	 * read item_count from the top-level... late writers might get
	 * suspended before bumping the count, leading us to under-count
	 * the items in the next table.
	 */
        if (record.info & HIHAT_EPOCH_MASK) {
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
        candidate_store = hihat_store_new(new_size);
	
        if (!LCAS(&self->store_next,
                  &new_store,
                  candidate_store,
                  HIHAT_CTR_NEW_STORE)) {
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
        bucket = &self->buckets[i];
        record = atomic_read(&bucket->record);

        if (record.info & HIHAT_F_MOVED) {
            continue;
        }

	// If it hasn't been moved, there's definitely an item in it,
	// as empty buckets got MOVED set in the first loop.
        hv  = atomic_read(&bucket->hv);
        bix = hatrack_bucket_index(&hv, new_store->last_slot);

	// This loop acquires a bucket in the destination hash table,
	// with the same bucket acquisition logic as operations above.
        for (j = 0; j <= new_store->last_slot; j++) {
            new_bucket     = &new_store->buckets[bix];
	    expected_hv    = atomic_read(&new_bucket->hv);
	    if (hatrack_bucket_unreserved(&expected_hv)) {
		if (LCAS(&new_bucket->hv,
			 &expected_hv,
			 hv,
			 HIHAT_CTR_MIGRATE_HV)) {
		    break;
		}
	    }
	    if (!hatrack_hashes_eq(&expected_hv, &hv)) {
		bix = (bix + 1) & new_store->last_slot;
		continue;
            }
            break;
        }

	// Set up the value we want to install, as well as the
	// value we expect to see in the target bucket, if our
	// thread succeeds in doing the move.
        candidate_record.info = record.info & HIHAT_EPOCH_MASK;
        candidate_record.item = record.item;
        expected_record.info  = 0;
        expected_record.item  = NULL;

	// The only way this can fail is if some other thread succeeded,
	// so we don't need to concern ourselves with the return value.
        LCAS(&new_bucket->record,
	     &expected_record,
	     candidate_record,
	     HIHAT_CTR_MIG_REC);

	// Whether we won or not, assume the winner might have
	// stalled.  Every thread attempts to update the source
	// bucket, to denote that the move was successful.
        candidate_record.info = record.info | HIHAT_F_MOVED;
        LCAS(&bucket->record, &record, candidate_record, HIHAT_CTR_F_MOVED2);
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
         HIHAT_CTR_LEN_INSTALL);

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
    if (LCAS(&top->store_current, &self, new_store, HIHAT_CTR_STORE_INSTALL)) {
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
