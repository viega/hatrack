/*
 * Copyright © 2022 John Viega
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
 *  Name:           oldhat.c
 *  Description:    Old, Legacy, Dated Hardware-Acceptable Table
 *
 *                  This table stays away from 128-bit compare-and
 *                  swap operations.  It does so by keeping all bucket
 *                  information in a single structure, and only ever
 *                  CASing a pointer to said structure.
 *
 *                  The net result is we require a lot of dynamic
 *                  memory allocation.
 *
 *                  Note that an alternate approach would be to use
 *                  something closer to hihat, but implement it with
 *                  64-bit hash values.  This is doable, but has some
 *                  issues:
 *
 *                  1) 64 bit hashes aren't really enough to ensure
 *                     uniqueness (even with a good hash function);
 *                     you probably should ALSO test the key directly,
 *                     which we'd love to be able to skip.
 *
 *                  2) We need to be able to atomically modify the
 *                     contents, along with the metadata... whether
 *                     the item is present, and migration status.
 *
 *                     If we only have 64 bits, we have to restrict
 *                     the item to pointers, because we'll need to
 *                     "steal" the lowest three bits of the pointer
 *                     for status.
 *
 *                     Alternatively to pointer-stealing, we can swap
 *                     in a pointer to a data record
 *                     atomically... which is how we ended up with
 *                     this algorithm... we're already doing that, so
 *                     why settle for a 64-bit identity test, when we
 *                     can skip auxillary checking?
 *
 *                     Nonetheless, at some point, we might add a hat
 *                     to the hatrack that uses 64-bit hashes and
 *                     pointer stealing. If you don't care about the
 *                     overhead of additional identity testing (i.e.,
 *                     if you're willing to tolerate occasional
 *                     undetected collisions), it will be
 *                     significantly faster than this algorithm (much
 *                     like hihat is on 128-bit systems), because it
 *                     will have better cache behavior, and will do
 *                     vastly less memory allocation / deallocation.
 *
 *  Author:         John Viega, john@zork.org
 */

#include <hatrack.h>

#ifdef HATRACK_COMPILE_ALL_ALGORITHMS

// clang-format off
static oldhat_store_t  *oldhat_store_new    (uint64_t);
static void             oldhat_store_delete (oldhat_store_t *, void *);
static void            *oldhat_store_get    (oldhat_store_t *, hatrack_hash_t,
					     bool *);
static void            *oldhat_store_put    (oldhat_store_t *, oldhat_t *,
					      hatrack_hash_t, void *, bool *);
static void            *oldhat_store_replace(oldhat_store_t *, oldhat_t *,
					      hatrack_hash_t, void *, bool *);
static bool             oldhat_store_add    (oldhat_store_t *, oldhat_t *,
					      hatrack_hash_t, void *);
static void            *oldhat_store_remove (oldhat_store_t *, oldhat_t *,
					      hatrack_hash_t, bool *);
static oldhat_store_t  *oldhat_store_migrate(oldhat_store_t *, oldhat_t *);
// clang-format on

/*
 * As with hihat and many of our lock-free algorithms, we wrap our
 * operations via MMM, a mini-memory management wrapper, that deals
 * with the fact that we might need to make a decision to free
 * something allocated dynamically while other threads still have a
 * reference to it.
 *
 * Specifically, for the sake of oldhat, MMM makes sure that, when
 * data records are "retired", they are not freed until all threads
 * are done with them.
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
oldhat_t *
oldhat_new(void)
{
    oldhat_t *ret;

    ret = (oldhat_t *)malloc(sizeof(oldhat_t));

    oldhat_init(ret);

    return ret;
}

oldhat_t *
oldhat_new_size(char size)
{
    oldhat_t *ret;

    ret = (oldhat_t *)malloc(sizeof(oldhat_t));

    oldhat_init_size(ret, size);

    return ret;
}

void
oldhat_init(oldhat_t *self)
{
    oldhat_init_size(self, HATRACK_MIN_SIZE_LOG);

    return;
}

void
oldhat_init_size(oldhat_t *self, char size)
{
    oldhat_store_t *store;
    uint64_t        len;

    if (size > (ssize_t)(sizeof(intptr_t) * 8)) {
        abort();
    }

    if (size < HATRACK_MIN_SIZE_LOG) {
        abort();
    }

    len   = 1 << size;
    store = oldhat_store_new(len);

    atomic_store(&self->store_current, store);
    atomic_store(&self->item_count, 0);

    return;
}

/* oldhat_cleanup()
 *
 * Cleans up the internal state of an oldhat object. Generally, you
 * should be confident that all threads except the one from which
 * you're calling this have stopped using the table (generally meaning
 * they no longer hold a reference to the store).
 *
 * Note that this function does not clean up the top-level object. You
 * can either free it yourself (however it was allocated), or if you
 * used oldhat_new() or the default malloc, you can call
 * oldhat_delete(), in which case you should NOT call this function.
 */
void
oldhat_cleanup(oldhat_t *self)
{
    oldhat_store_t *store;

    store = atomic_load(&self->store_current);

    mmm_retire(store);
    return;
}

/* oldhat_delete()
 *
 * Deletes an oldhat object. Generally, you should be confident that
 * all threads except the one from which you're calling this have
 * stopped using the table (generally meaning they no longer hold a
 * reference to the store).
 *
 * Note that this function assumes the oldhat object was allocated via
 * the default malloc. If it wasn't, don't call this directly, but do
 * note that the stores were created via mmm_alloc(), and the most
 * recent store will need to be retired via mmm_retire().
 */
void
oldhat_delete(oldhat_t *self)
{
    oldhat_cleanup(self);
    free(self);

    return;
}

/* oldhat_get()
 *
 * Note that we pass no key, since the hash value associated with the
 * key, being 128 bits, is sufficient to handle identity.
 */
void *
oldhat_get(oldhat_t *self, hatrack_hash_t hv, bool *found)
{
    void *ret;

    mmm_start_basic_op();

    ret = oldhat_store_get(self->store_current, hv, found);

    mmm_end_op();

    return ret;
}

/* oldhat_put()
 *
 * Insert, whether or not the item previously was in the hash table.
 * Returns the old value, if any.  Found parameter is optional, and
 * indicates whether or not the key was previously in the table (and,
 * by consequence, whether memory management might be needed).
 *
 * If the key wasn't already present, NULL also gets returned, but
 * that is also a valid value for the table, thus the extra parameter.
 */
void *
oldhat_put(oldhat_t *self, hatrack_hash_t hv, void *item, bool *found)
{
    void *ret;

    mmm_start_basic_op();

    ret = oldhat_store_put(self->store_current, self, hv, item, found);

    mmm_end_op();

    return ret;
}

/* oldhat_replace()
 *
 * Insert, but only if the item previously was in the hash table.
 * Returns the old value, if any.  Found parameter is optional, and
 * indicates whether or not the key was previously in the table.
 *
 * If there's a miss, NULL also gets returned, but that is also a
 * valid value for the table, thus the extra parameter.
 */
void *
oldhat_replace(oldhat_t *self, hatrack_hash_t hv, void *item, bool *found)
{
    void *ret;

    mmm_start_basic_op();

    ret = oldhat_store_replace(self->store_current, self, hv, item, found);

    mmm_end_op();

    return ret;
}

/* oldhat_add()
 *
 * Add the item to the table, but only if the item was NOT previously
 * in the table.  Returns true or false, depending on whether the
 * item added or not.
 */
bool
oldhat_add(oldhat_t *self, hatrack_hash_t hv, void *item)
{
    bool ret;

    mmm_start_basic_op();

    ret = oldhat_store_add(self->store_current, self, hv, item);

    mmm_end_op();

    return ret;
}

/* oldhat_remove()
 *
 * Remove the item from the hash table, if it's present in the table.
 * The old value is returned for memory allocation purposes.
 *
 * The return value and found parameter basically work like they do
 * with the _put and _replace operations.
 */
void *
oldhat_remove(oldhat_t *self, hatrack_hash_t hv, bool *found)
{
    void *ret;

    mmm_start_basic_op();

    ret = oldhat_store_remove(self->store_current, self, hv, found);

    mmm_end_op();

    return ret;
}

/* oldhat_len()
 *
 * Returns the approximate number of items currently in the
 * table. Note that we strongly discourage using this call, since it
 * is close to meaningless in multi-threaded programs, as the value at
 * the time of check could be dramatically different by the time of
 * use.
 */
uint64_t
oldhat_len(oldhat_t *self)
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
oldhat_view(oldhat_t *self, uint64_t *num, bool sort)
{
    hatrack_view_t  *view;
    hatrack_view_t  *p;
    oldhat_record_t *record;
    uint64_t         i;
    uint64_t         num_items;
    uint64_t         alloc_len;
    oldhat_store_t  *store;

    /* Again, we need to do this before grabbing our pointer to the
     * store, to make sure it doesn't get deallocated out from under
     * us. We also have to call mmm_end_op() when done.
     *
     * That is to say, a migration could be in progress at any time
     * during this view operation.  As a reader, we can safely ignore
     * the migration in all cases, and just work from the store we're
     * currently referencing.
     */
    mmm_start_basic_op();

    store     = atomic_read(&self->store_current);
    alloc_len = sizeof(hatrack_view_t) * (store->last_slot + 1);
    view      = (hatrack_view_t *)malloc(alloc_len);
    p         = view;

    for (i = 0; i <= store->last_slot; i++) {
        record = atomic_read(&store->buckets[i]);

        if (!record || !record->used) {
            continue;
        }

        p->item       = record->item;
        p->sort_epoch = mmm_get_create_epoch(record);
        p++;
    }

    num_items = p - view;
    *num      = num_items;

    if (!num_items) {
        free(view);
        mmm_end_op();

        return NULL;
    }

    view = realloc(view, *num * sizeof(hatrack_view_t));

    if (sort) {
        qsort(view, num_items, sizeof(hatrack_view_t), hatrack_quicksort_cmp);
    }

    mmm_end_op();

    return view;
}

/* New stores get allocated with mmm_alloc_committed. As a result, the
 * underlying memory is zeroed out, so we only need to initialize
 * non-zero items.
 *
 * The cleanup handler is called by the memory management system, for
 * us to do any cleanup tasks before the store is finally free()'d.
 */
// clang-format off

static oldhat_store_t *
oldhat_store_new(uint64_t size)
{
    uint64_t        alloc_len;
    oldhat_store_t *store;

    alloc_len        = sizeof(oldhat_store_t);
    alloc_len       += sizeof(oldhat_record_t *) * size;
    store            = (oldhat_store_t *)mmm_alloc_committed(alloc_len);
    store->last_slot = size - 1;
    store->threshold = hatrack_compute_table_threshold(size);

    mmm_add_cleanup_handler(store, (mmm_cleanup_func)oldhat_store_delete, NULL);

    return store;
}

/* When we're sure there are no threads left using the table, we need
 * to also deallocate the records in that table. Since the threads are
 * out of the table, it's safe to deallocate the records immediately,
 * instead of adding to a freelist, which we do by calling
 * mmm_retire_unused(), which asserts the memory currently is not in
 * use by any thread.
 *
 * This doesn't get called directly anywhere; instead, when we
 * allocate stores above, we add a cleanup handler. MMM calls this
 * function when it's confident it can delete the store.
 */
static void
oldhat_store_delete(oldhat_store_t *self, void *unused)
{
    uint64_t         i;
    oldhat_record_t *record;

    for (i = 0; i <= self->last_slot; i++) {
        record = atomic_load(&self->buckets[i]);

        if (record) {
            mmm_retire_unused(record);
        }
    }

    return;
}

/*
 * This is probably the most straightforward get operation of all our
 * hash tables.  We simply load the bucket, check to see if the hash
 * is right, and if it is, return the item we find in the record.
 *
 * If the hash is wrong, we do basic linear probing... we just move to
 * the next bucket in sequence (which should provide slightly better
 * cache performance on reprobing, even though each bucket load does
 * require a pointer dereference).
 */
static void *
oldhat_store_get(oldhat_store_t *self,
                 hatrack_hash_t  hv,
                 bool           *found)
{
    uint64_t         bix;
    uint64_t         i;
    oldhat_record_t *record;

    bix = hatrack_bucket_index(hv, self->last_slot);

    for (i = 0; i <= self->last_slot; i++) {
        record = atomic_load(&self->buckets[bix]);
	
        if (!record) {
            break;
        }
	
        if (!hatrack_hashes_eq(hv, record->hv)) {
            bix = (bix + 1) & self->last_slot;
            continue;
        }
	
        if (!record->used) {
            break;
        }
	
        if (found) {
            *found = true;
        }
	
        return record->item;
    }
    
    // not found.
    if (found) {
        *found = false;
    }
    
    return NULL;
}

/*
 * For our write operation, we build out the record we want to
 * install, and then apply our hash function (and possibly our probing
 * function) to look for either an existing entry, or an empty bucket
 * (whichever comes first).
 *
 * If a previous entry exists:
 *
 *   1) If the entry has NOT been deleted, we ask MMM to set the logical
 *      record creation time of our record to the creation time of the
 *      record we are replacing.
 *
 *   2) We try to use a CAS operation to replace the record with our
 *      new record. If we fail at this CAS, it could be for two reasons:
 *
 *      a) Because the table is now being migrated, in which case we
 *         need to go help with the migration.
 *
 *      b) Because another thread beat us to using this bucket. This
 *         case we treat this as if we actually "won" the race, but
 *         the other thread overwrote our value. In that case, we
 *         return the item passed in to the caller, for memory
 *         management.
 *
 *   3) Assuming the CAS was successful, we 'retire' the old record.
 *
 * If no previous entry exists, we try CASing our record into the
 * bucket. If we succeed, then we just wrote a new entry into the
 * table. If we fail, someone "beat" us, and we behave per above.
 *
 * If we've successfully written in a NEW record, we then need to make
 * sure that there aren't now too many entries in the table. If there
 * are, we help migrate the table to a new store before returning.
 *
 * On small tables, we might cycle through all the buckets, and not
 * have room for ourselves, in which case the table definitely needs a
 * migration, and we go off and do that before retrying our operation.
 */
static void *
oldhat_store_put(oldhat_store_t *self,
                 oldhat_t       *top,
                 hatrack_hash_t  hv,
                 void           *item,
                 bool           *found)
{
    oldhat_record_t *candidate;
    oldhat_record_t *record;
    uint64_t         bix;
    uint64_t         i;

    /* Set up the record we want to install.
     *
     * If we abandon this record, for instance, to go off and migrate
     * the table, we need to make sure to give back the memory, via
     * mmm_retire_unused().
     */
    candidate       = mmm_alloc_committed(sizeof(oldhat_record_t));
    candidate->hv   = hv;
    candidate->item = item;
    candidate->used = true;

    /* Given our hash value, find the first bucket we should look at.
     * The subsequent for() loop advances the bucket index (modulo the
     * table size), for cases when the bucket is occupied by an entry
     * with a different hash value.
     */
    bix = hatrack_bucket_index(hv, self->last_slot);

    for (i = 0; i <= self->last_slot; i++) {
        record = atomic_load(&self->buckets[bix]);
	
        if (!record) {
            /* This bucket was unused at the time we loaded the record,
             * so we will try to "acquire" the bucket for our hash value
             * by swapping in our record.
             *
             * If we lose that race, then it could be another thread
             * claimed the bucket using a different hash value that
             * maps to the same bucket, or it could be there's a new
             * item we can overwrite.
             *
             * Either way, when the CAS fails, the record that beat
             * our CAS will be in the 'record' variable, and we
             * continue below, as if we loaded an existing record the
             * first time.
             */
            if (CAS(&self->buckets[bix], &record, candidate)) {
                if (found) {
                    *found = false;
                }
		
                /* Being a new entry, we need to bump both the item
                 * count (which estimates how many items are in the
                 * table), and we need to bump the used_count, which
                 * indicates how many buckets are in use.
                 *
                 * If we hit the threshold, we go migrate the table
                 * before returning.
                 */
		
                atomic_fetch_add(&top->item_count, 1);
		
                if (atomic_fetch_add(&self->used_count, 1) >= self->threshold) {
                    oldhat_store_migrate(self, top);
                }
		
                return NULL;
            }
        }
        /* If the current record has a different hash value, this is
         * not the bucket we're looking for, so move on to the next
         * bucket in sequence. Otherwise, jump a few lines down to
         * continue, now that we've found our bucket.
         */
        if (!hatrack_hashes_eq(hv, record->hv)) {
            bix = (bix + 1) & self->last_slot;
            continue;
        }
	
        goto found_bucket;
    }

    /* This code might get jumpped to from below, if we notice a
     * migration is in progress. However, it can also get called by
     * exhausting the above 'for' loop, in cases where a small table
     * fills up incredibly quickly.
     */
migrate_and_retry:
    // We'll re-allocate this when we retry, below.
    mmm_retire_unused(candidate);
    /* The migration operation returns the newest top store; we call
     * ourselves recursively on that new store to retry.
     *
     * Note that this is tail-recursive, so could be slightly
     * optimized.  But, in practice, the average number of recursive
     * calls, when there is a migration, will be very close to 1, so
     * it's hardly worth it, but the compiler can knock itself out if
     * it wants to do it for us.
     */
    self = oldhat_store_migrate(self, top);
    
    return oldhat_store_put(self, top, hv, item, found);

found_bucket:
    /* If there's no record here, it would be equally valid to be a
     * bad citizen and skip the migration, and return faster.
     *
     * In reality, this would save a bit of work on misses that lead
     * to a migration.
     *
     * But, for the moment, I prefer to be a good citizen, when it's
     * easy to do!  (I might change this, but if so, I'm going to do
     * it across all the hats in the hatrack, and estimate the
     * impact).
     *
     * Therefore, we check for migration before checking for whether
     * the record is in use.
     */
    if (record->moving) {
        goto migrate_and_retry;
    }
    /* This is where we copy the creation time of the previous item,
     * for purposes of maintaining a sort order (which we get when
     * oldhat_view() is called).
     *
     * We could also skip this, and order based on most recent
     * write-time. However, we take this approach to be consistent
     * with Python's 'sorted' dictionary implementation, since it's
     * the most prominent sorted hash table, from my perspective.
     */
    if (record->used) {
        mmm_copy_create_epoch(candidate, record);
    }
    /*
     * Here's where we try to install our record. If it succeeds, as
     * the thread that overwrote, we're responsible for memory
     * management of the record, so we will use mmm_retire(),
     * signifying that there's a new record in place. Note that we can
     * still use the record for the rest of this function-- we won't
     * try to free it until some future epoch, since we ourselves have
     * taken out a reservation that will keep this record alive until
     * the next time this thread asks for a reservation...
     *
     * Note that there will definitely be a record here (when there's
     * not a record, we install via the CAS at the top of the
     * function). However, the record might indicate there was not
     * actually an item in this bucket.
     *
     * In such a case, we also need to bump back up the item count
     * (which represents the number of items actively in the table,
     * NOT the number of occupied buckets... the later includes
     * deletion records, item_count does not).
     */
    if (CAS(&self->buckets[bix], &record, candidate)) {
        mmm_retire_fast(record);
        if (!record->used) {
            if (found) {
                *found = false;
            }
	    
            atomic_fetch_add(&top->item_count, 1);
	    
            return NULL;
        }
        else {
            if (found) {
                *found = true;
            }
	    
            return record->item;
        }
    }
    /* If we get here, the CAS failed. Either it's time to migrate, or
     * someone beat us to the punch, in which case we pretend we were
     * successful, but overwritten.
     */
    if (record->moving) {
        goto migrate_and_retry;
    }
    
    mmm_retire_unused(candidate);
    
    return item;
}

/* This is similar to oldhat_store_put(), except that if we find
 * there's no record there to replace, we indicate failure, and do not
 * change the table.
 *
 * Note that, even if there's no entry to replace, if we notice
 * there's a resize in progress, we go off and help, then try again --
 * there might be another write pending; we can race with them after
 * the resize. It would be perfectly valid, and perhaps a smidge
 * faster to not try again in that case.
 */
static void *
oldhat_store_replace(oldhat_store_t *self,
                     oldhat_t       *top,
                     hatrack_hash_t  hv,
                     void           *item,
                     bool           *found)
{
    oldhat_record_t *candidate;
    oldhat_record_t *record;
    uint64_t         bix;
    uint64_t         i;

    candidate       = mmm_alloc_committed(sizeof(oldhat_record_t));
    candidate->hv   = hv;
    candidate->item = item;
    candidate->used = true;

    bix = hatrack_bucket_index(hv, self->last_slot);

    for (i = 0; i <= self->last_slot; i++) {
        record = atomic_load(&self->buckets[bix]);
	
        if (!record) {
            goto not_found;
        }
	
        if (!hatrack_hashes_eq(hv, record->hv)) {
            bix = (bix + 1) & self->last_slot;
            continue;
        }
	
        goto found_bucket;
    }

not_found:
    if (found) {
        *found = false;
    }
    
    mmm_retire_unused(candidate);
    
    return NULL;

migrate_and_retry:
    mmm_retire_unused(candidate);
    
    self = oldhat_store_migrate(self, top);
    
    return oldhat_store_replace(self, top, hv, item, found);

found_bucket:
    if (record->moving) {
        goto migrate_and_retry;
    }
    
    if (!record->used) {
        goto not_found;
    }
    mmm_copy_create_epoch(candidate, record);

    if (CAS(&self->buckets[bix], &record, candidate)) {
        mmm_retire(record);
	
        if (found) {
            *found = true;
        }
	
        return record->item;
    }
    
    if (record->moving) {
        goto migrate_and_retry;
    }
    
    mmm_retire_unused(candidate);
    
    return item;
}

/*
 * Again, based on oldhat_store_put(), but only inserts if the item is
 * NOT already in the table.  Remember that there are two ways we
 * could determine that it's okay to add our item:
 *
 *  1) Because the item has NEVER been in the current store, in which
 *     case we'll have found a hash bucket that's never had a record
 *     installed.  Or,
 *
 *  2) Because we found a record associated with the item, but the
 *     record tells us that the item was deleted.
 */
static bool
oldhat_store_add(oldhat_store_t *self,
                 oldhat_t       *top,
                 hatrack_hash_t  hv,
                 void           *item)
{
    oldhat_record_t *candidate;
    oldhat_record_t *record;
    uint64_t         bix;
    uint64_t         i;

    candidate       = mmm_alloc_committed(sizeof(oldhat_record_t));
    candidate->hv   = hv;
    candidate->item = item;
    candidate->used = true;

    bix = hatrack_bucket_index(hv, self->last_slot);

    for (i = 0; i <= self->last_slot; i++) {
        record = atomic_load(&self->buckets[bix]);
	
        if (!record) {
            if (CAS(&self->buckets[bix], &record, candidate)) {
                if (atomic_fetch_add(&self->used_count, 1) >= self->threshold) {
                    oldhat_store_migrate(self, top);
                }
		
                return true;
            }
        }
	
        if (!hatrack_hashes_eq(hv, record->hv)) {
            bix = (bix + 1) & self->last_slot;
            continue;
        }
	
        goto found_bucket;
    }

migrate_and_retry:
    mmm_retire_unused(candidate);
    self = oldhat_store_migrate(self, top);
    
    return oldhat_store_add(self, top, hv, item);

found_bucket:
    if (record->moving) {
        goto migrate_and_retry;
    }
    
    if (record->used) {
        mmm_retire_unused(candidate);
	
        return false;
    }
    
    if (CAS(&self->buckets[bix], &record, candidate)) {
        mmm_retire(record);
        atomic_fetch_add(&top->item_count, 1);
	
        return true;
    }
    
    /* If we get here, the CAS failed. Either it's time to migrate, or
     * someone beat us to the punch, in which case we return false.
     */
    if (record->moving) {
        goto migrate_and_retry;
    }
    
    mmm_retire_unused(candidate);
    
    return false;
}

/* Similar logic to functions above; the bucket acquisition looks more
 * like a "replace", but we just remove (and return the old value),
 * instead of putting something on top.
 *
 *  We do this by swapping in a record that keeps the bucket reserved
 *  for our hash value, but indicates that the item has been deleted.
 */
static void *
oldhat_store_remove(oldhat_store_t *self,
                    oldhat_t       *top,
                    hatrack_hash_t  hv,
                    bool           *found)
{
    oldhat_record_t *candidate;
    oldhat_record_t *record;
    uint64_t         bix;
    uint64_t         i;

    candidate       = mmm_alloc_committed(sizeof(oldhat_record_t));
    candidate->hv   = hv;
    candidate->used = false;

    bix = hatrack_bucket_index(hv, self->last_slot);

    for (i = 0; i <= self->last_slot; i++) {
        record = atomic_load(&self->buckets[bix]);
	
        if (!record) {
            goto not_found;
        }
	
        if (!hatrack_hashes_eq(hv, record->hv)) {
            bix = (bix + 1) & self->last_slot;
            continue;
        }
	
        goto found_bucket;
    }

not_found:
    mmm_retire_unused(candidate);
    
    if (found) {
        *found = false;
    }
    
    return NULL;

migrate_and_retry:
    mmm_retire_unused(candidate);
    self = oldhat_store_migrate(self, top);
    
    return oldhat_store_remove(self, top, hv, found);

found_bucket:
    if (record->moving) {
        goto migrate_and_retry;
    }
    
    if (!record->used) {
        goto not_found;
    }
    
    if (CAS(&self->buckets[bix], &record, candidate)) {
        mmm_retire(record);
	
        if (found) {
            *found = true;
        }
	
        atomic_fetch_sub(&top->item_count, 1);
	
        return record->item;
    }
    /* If we get here, the CAS failed. Either it's time to migrate, or
     * someone beat us to the punch, in which case we fail.
     */
    if (record->moving) {
        goto migrate_and_retry;
    }

    goto not_found;
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
 * that a migration is in progress, by setting the boolean variable
 * 'moving' in the current record.
 *
 * That effectively locks all writes on a bucket, but instead of
 * sitting around and waiting for the migration to occur, writer
 * threads will attempt to help with the migration, usually before
 * completing their own operation.
 *
 * We have writer threads help with the migration because threads can
 * stall due to the scheduler, and we don't want any thread to be
 * dependent on any other thread's progress. Therefore, every thread
 * that notices that a migration is in progress, goes through the
 * ENTIRE process of attempting to migrate.
 *
 * In oldhat, there is one exception to this policy (which is noted
 * above). Basically, when we insert a brand new entry that triggers a
 * resize, we don't notice the need to resize until AFTER we've
 * successfully added our item. We'll still help with the migration,
 * but we don't need to retry our operation.
 *
 * In our migration algorithm, whenever a thread sees that some other
 * thread completed the migration of a single record, they abandon
 * that particular piece of the migration, and move on to the next
 * piece.
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
 * Anyway, threads start by visiting every single bucket, installing a
 * record that is the same as whatever was there before, except with
 * the "moving" flag set. Writers can proceed without delay. Other
 * threads may be doing the same thing in parallel, of course, which
 * is quite alright.
 *
 * If, at this point, we see that the bucket is empty, we will still
 * install a record into that bucket, with no associated hash value,
 * indicating that the bucket is empty, but that a migration is in
 * progress. This will stop late writers from adding to the old table
 * during a migration.
 *
 * Then, the thread cycle back through all buckets, and attempt to
 * migrate them, by copying data from the records in the current
 * table, and trying to install those records in the new table. The
 * only reason this copy operation might fail is if another writer is
 * being successful, so if we fail we ignore it, and keep going.
 *
 * As a result of this approach, the migration itself is lock-free.
 * The only reason we might "spin" on a CAS loop is if late writers
 * manage to update the value of a bucket, while we're waiting to mark
 * the bucket as moving. While the number of threads may be bounded,
 * if threads keep writing out the same value over and over, it's
 * conceptually possible for them to never notice that a migration is
 * happening.
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
 * Click's migration algorithm is indeed a much more complicated
 * algorithm than ours. Even though our approach might seem like it's
 * duplicating a lot more effort, it will tend to finish the migration
 * at least as quickly as Click's approach, because it's robust to
 * threads that are carrying the burden of the migrating getting
 * suspended by the scheduler.
 */
static oldhat_store_t *
oldhat_store_migrate(oldhat_store_t *self, oldhat_t *top)
{
    oldhat_record_t *candidate_record;
    oldhat_store_t  *new_store;
    oldhat_store_t  *candidate_store;
    uint64_t         new_size;
    oldhat_record_t *record;
    uint64_t         i, j;
    uint64_t         bix;
    uint64_t         record_sz;
    uint64_t         new_used;
    uint64_t         expected_used;

    /* Run through every bucket, and mark any bucket that
     * doesn't already know we're moving.  Note that the CAS could
     * fail due to some other updater, so we keep CASing until we know
     * it was successful.
     */
    new_used         = 0;
    record_sz        = sizeof(oldhat_record_t);
    candidate_record = (oldhat_record_t *)mmm_alloc_committed(record_sz);

    /* The work to mark existing records as non-writable is much more
     * work than in any of our other hash table implementations,
     * because we have to make a complete copy of the old record,
     * install a replacement, and then free the old record.
     *
     * With other implementations that use pointers to records, we
     * ease this burden a bit by stealing the status bits from the
     * pointer itself.
     *
     * That would work here as well, and eliminatate our need to free
     * old records. However, I'm not sure it appreciably speeds up
     * workloads in the grand scheme of things (especially since
     * migrations are usually pretty rare), and for the moment, I want
     * this to be the ONE lock-free hash table implementation that's
     * more focused on being dirt-simple (with the rest, I care more
     * about performance, or whatever other functionality I'm adding).
     *
     * Note that we COULD go bucket-by-bucket, lock the bucket, and
     * migrate this bucket, before locking the next bucket. That would
     * be an equally valid approach that doesn't spoil the consistency
     * of anything, and we'd only make one pass through the array,
     * instead of three passes.
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
        record = atomic_read(&self->buckets[i]);
	
        do {
            if (!record) {

		// Sets the hash value to 0.
		hatrack_bucket_initialize(&candidate_record->hv);
		
                candidate_record->item   = NULL;
                candidate_record->used   = false;
                candidate_record->moving = true;
                candidate_record->moved  = true;
            }
            else {
                if (record->moving) {
                    goto add_to_length;
                }
		
                candidate_record->hv     = record->hv;
                candidate_record->item   = record->item;
                candidate_record->used   = record->used;
                candidate_record->moving = true;
                candidate_record->moved  = false;
		
                if (candidate_record->used) {
                    mmm_copy_create_epoch(candidate_record, record);
                }
            }
        } while (!CAS(&self->buckets[i], &record, candidate_record));
        /* If we are here, we exited due to the CAS being successful,
         * which means we need to replenish our candidate_record, and
         * retire the old record, if any.
         */
        if (record) {
            mmm_retire_fast(record);
        }
	
        candidate_record = (oldhat_record_t *)mmm_alloc_committed(record_sz);

        /* Here, whether we installed our record or not, we look at
         * the old record (if any), to count how many items we're
         * going to migrate, so we can try to install the value in the
         * new store when we're done, as used_count.
         *
         * Since used_count is part of our resize metric, we want this
         * to stay as accurate as possible, which is why we don't just
         * read item_count from the top-level... late writers might get
         * suspended before bumping the count, leading us to under-count
         * the items in the next table.
         */
add_to_length:
        if (record && record->used) {
            new_used++;
        }
    }

    new_store = atomic_read(&self->store_next);

    /* If we couldn't acquire a store, try to install one into
     * store_current->next_store. If we fail, free it, and begin using
     * the one that was successfully installed.
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
        candidate_store = oldhat_store_new(new_size);

        if (!CAS(&self->store_next, &new_store, candidate_store)) {
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
     * Note that, since the table has resized, the bucket index our
     * hash value maps to may change, and there may be new
     * collisions. So we can't just copy a record to the same index in
     * the new table; we need to go through the bucket allocation
     * process again (including the linear probing).
     */
    for (i = 0; i <= self->last_slot; i++) {
        record = atomic_read(&self->buckets[i]);

        if (record->moved) {
            goto next_migration;
        }
        // Note that, if we're here, then there's definitely a record
        // in place; for items w/o a record in place, 'moved' gets
        // set.
        bix = hatrack_bucket_index(record->hv, new_store->last_slot);
	
        candidate_record->hv     = record->hv;
        candidate_record->item   = record->item;
        candidate_record->used   = true;
        candidate_record->moving = false;
        candidate_record->moved  = false;
	
        mmm_copy_create_epoch(candidate_record, record);

        for (j = 0; j <= new_store->last_slot; j++) {
            record = atomic_read(&new_store->buckets[bix]);
	    
            if (!record) {
                if (CAS(&new_store->buckets[bix], &record, candidate_record)) {
                    candidate_record
                        = (oldhat_record_t *)mmm_alloc_committed(record_sz);
		    
                    goto next_migration;
                }
            }
	    
            if (!hatrack_hashes_eq(record->hv, candidate_record->hv)) {
                bix = (bix + 1) & new_store->last_slot;
                continue;
            }
	    
            break; // Someone else got the job done.
        }
next_migration:
        continue;
    }

    /* This is our third pass, where we go and mark every old bucket
     * as fully migrated. I believe all of my other algorithms combine
     * this with the second pass, because it's vastly less code (the
     * reason for two passes is to try to get all writers migrating as
     * quickly as possible; a third pass has no other benefit, other
     * than the potential code clarity).
     */

    for (i = 0; i <= self->last_slot; i++) {
        record = atomic_read(&self->buckets[i]);
	
        do {
            if (record->moved) {
                goto next_mark_finished;
            }
	    
            candidate_record->hv     = record->hv;
            candidate_record->item   = record->item;
            candidate_record->used   = record->used;
            candidate_record->moving = true;
            candidate_record->moved  = true;
	    
            if (candidate_record->used) {
                mmm_copy_create_epoch(candidate_record, record);
            }
        } while (!CAS(&self->buckets[i], &record, candidate_record));
        /* If I'm here, I exited due to the CAS being successful,
         * which means I need to replenish my candidate_record, and
         * retire the old record, which there will definitely be, this
         * time.
         */
        mmm_retire_fast(record);
	
        candidate_record = (oldhat_record_t *)mmm_alloc_committed(record_sz);

next_mark_finished:
        continue;
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
    CAS(&new_store->used_count, &expected_used, new_used);

    /* We always had a record pre-allocated for our CAS operations; we
     * held on to it when a CAS operation failed, and allocated a new
     * one, whenever a CAS was successful. That's a much simpler
     * approach than always trying to keep state about whether we have
     * a record ready-- we just always make sure we have a record ready.
     *
     * The consequence of that approach is that we will necessarily
     * get here with one record allocated that we're not going to use,
     * so we go ahead and free it.
     */
    mmm_retire_unused(candidate_record);

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
    if (CAS(&top->store_current, &self, new_store)) {
        mmm_retire_fast(self);
    }

    /* Instead of returning new_store here, we accept that we might
     * have been suspended for a while at some point, and there might
     * be an even later migration. So we grab the top-most store, even
     * though we expect that it's generally going to be the same as
     * next_store.
     */
    return top->store_current;
}

#endif
