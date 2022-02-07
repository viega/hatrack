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
 *  Name:           duncecap.c
 *  Description:    Don't Use: Crappy Educational Code, Albeit Parallel.
 *
 *                  This uses a per-data structure lock that writers hold
 *                  for their entire operation.
 *
 *                  Readers also use the lock, but only for a minimal
 *                  amount of time-- enough time to grab a pointer to
 *                  the current store, and to increment a reference
 *                  count in that store.  The lock does not need to
 *                  be held when readers exit.
 *
 *  Author:         John Viega, john@zork.org
 *
 */

#include <hatrack.h>

#ifdef HATRACK_COMPILE_ALL_ALGORITHMS

// clang-format off
static duncecap_store_t *duncecap_store_new    (uint64_t);
static void             *duncecap_store_get    (duncecap_store_t *,
						hatrack_hash_t, bool *);
static void             *duncecap_store_put    (duncecap_store_t *,
						duncecap_t *, hatrack_hash_t,
						void *, bool *);
static void             *duncecap_store_replace(duncecap_store_t *,
						hatrack_hash_t, void *, bool *);
static bool              duncecap_store_add    (duncecap_store_t *,
						duncecap_t *, hatrack_hash_t,
						void *);
static void             *duncecap_store_remove (duncecap_store_t *,
						duncecap_t *, hatrack_hash_t,
						bool *);
static void             duncecap_migrate       (duncecap_t *);
// clang-format on

/* These macros clean up duncecap_view() to make it more readable.  With
 * duncecap, we allow compile-time configuration to determine whether
 * views are consistent or not (this is set in stone for all our other
 * algorithms, except for tophat, where it's runtime selectable).
 *
 * With consistent views, the results of a call to duncecap_view() are
 * guaranteed to be a moment-in-time result. To do this, we act like a
 * writer, grabbing the write-lock to keep the table from mutating
 * while we are using it.
 *
 * Otherwise, we act like a reader, and call the inline functions
 * duncecap_reader_enter() and duncecap_reader_exit() (defined in
 * duncecap.h).
 *
 * This behavior is controlled with the configuration variable
 * DUNCECAP_CONSISTENT_VIEWS. Inconsistent views are the default.
 */
#ifndef DUNCECAP_CONSISTENT_VIEWS
#define duncecap_viewer_enter(self)       duncecap_reader_enter(self)
#define duncecap_viewer_exit(self, store) duncecap_reader_exit(store)
#else
static inline duncecap_store_t *
duncecap_viewer_enter(duncecap_t *self)
{
    if (pthread_mutex_lock(&self->mutex)) {
        abort();
    }

    return self->store_current;
}

static inline void
duncecap_viewer_exit(duncecap_t *self, duncecap_store_t *unused)
{
    if (pthread_mutex_unlock(&self->mutex)) {
        abort();
    }

    return;
}
#endif

/* duncecap_new()
 *
 * Allocates a new duncecap object with the system malloc, and
 * initializes it.
 */
duncecap_t *
duncecap_new(void)
{
    duncecap_t *ret;

    ret = (duncecap_t *)malloc(sizeof(duncecap_t));

    duncecap_init(ret);

    return ret;
}

duncecap_t *
duncecap_new_size(char size)
{
    duncecap_t *ret;

    ret = (duncecap_t *)malloc(sizeof(duncecap_t));

    duncecap_init_size(ret, size);

    return ret;
}

/* duncecap_init()
 *
 * It's expected that duncecap instances will be created via the
 * default malloc.  This function cannot rely on zero-initialization
 * of its own object.
 */
void
duncecap_init(duncecap_t *self)
{
    duncecap_init_size(self, HATRACK_MIN_SIZE_LOG);

    return;
}

void
duncecap_init_size(duncecap_t *self, char size)
{
    duncecap_store_t *store;
    uint64_t          len;

    if (size > (ssize_t)(sizeof(intptr_t) * 8)) {
        abort();
    }

    if (size < HATRACK_MIN_SIZE_LOG) {
        abort();
    }

    len                 = 1 << size;
    store               = duncecap_store_new(len);
    self->store_current = store;
    self->item_count    = 0;
    self->next_epoch    = 1;

    pthread_mutex_init(&self->mutex, NULL);

    return;
}

/* duncecap_cleanup()
 *
 * This function is meant to be called when duncecap should clean up
 * its own internal state before deallocation. When you do so, it's
 * your responsibility to make sure that no threads are going to use
 * the object anymore.
 *
 * duncecap_delete() below is similar, except that it also calls
 * free() on the actual top-level object as well, under the assumption
 * it was created with the default malloc implementation.
 */
void
duncecap_cleanup(duncecap_t *self)
{
    pthread_mutex_destroy(&self->mutex);
    free(self->store_current);

    return;
}

/*
 * duncecap_delete()
 *
 * Deletes a duncecap object. Generally, you should be confident that
 * all threads except the one from which you're calling this have
 * stopped using the table (generally meaning they no longer hold a
 * reference to the store).
 *
 * Note that this function assumes the duncecap object was allocated
 * via the default malloc. If it wasn't, don't call this directly, but
 * do note that the stores were created via the system malloc, and the
 * most recent store will need to be freed (and the mutex destroyed).
 *
 * This is particularly important, not just because you might use
 * memory after freeing it (a reliability and security concern), but
 * also because using a mutex after it's destroyed is undefined. In
 * practice, there's a good chance that any thread waiting on this
 * mutex when it's destroyed will hang indefinitely.
 */
void
duncecap_delete(duncecap_t *self)
{
    duncecap_cleanup(self);
    free(self);

    return;
}

/* duncecap_get()
 *
 * The function atomically acquires the lock for the current store in
 * order to get a reference to the store and register itself. Then it
 * releases the lock, and calls the private function
 * duncecap_store_get() on the store, in order to do the rest of the
 * work. Note that a writer might change the store before we are done,
 * but that's okay-- new readers might end up in the new store, but
 * we'll still get a consistent view of the table, and the memory will
 * not be freed out from under us while we are reading (see
 * duncecap_migrate).
 *
 * This function takes the hash value of the item to look up, but you
 * do NOT pass the actual key to this API. There's no need, because we
 * aren't storing it, and it isn't being used in any identity test
 * (the hash value itself is sufficient).
 *
 * Since we accept any values when inserting into the table, the
 * caller might need to be able to differentiate whether or not the
 * item was in the table. If so, you can pass a non-null address in
 * the found parameter, and the memory location will be set to true if
 * the item was in the table, and false if it was not.
 */
void *
duncecap_get(duncecap_t *self, hatrack_hash_t hv, bool *found)
{
    void             *ret;
    duncecap_store_t *store;

    store = duncecap_reader_enter(self);
    ret   = duncecap_store_get(store, hv, found);

    duncecap_reader_exit(store);

    return ret;
}

/* duncecap_put()
 *
 * Lock the hash table for writing, and when we get ownership of the
 * lock, perform a 'put' operation in the current store (via
 * duncecap_store_put()), migrating the store if needed.
 *
 * 'Put' inserts into the table, whether or not the associated hash
 * value already has a stored item. If it does have a stored item, the
 * old value will be returned (so that it can be deleted, if
 * necessary; the table does not do memory management for the actual
 * contents). Also, if you pass a memory address in the found
 * parameter, the associated memory location will get the value true
 * if the item was already in the table, and false otherwise.
 *
 * Note that, if you're using a key and a value, pass them together
 * in a single object in the item parameter.
 */
void *
duncecap_put(duncecap_t *self, hatrack_hash_t hv, void *item, bool *found)
{
    void *ret;

    if (pthread_mutex_lock(&self->mutex)) {
        abort();
    }

    ret = duncecap_store_put(self->store_current, self, hv, item, found);

    if (pthread_mutex_unlock(&self->mutex)) {
        abort();
    }

    return ret;
}

/* duncecap_replace()
 *
 * Lock the hash table for writing, and when we get ownership of the
 * lock, perform a 'replace' operation in the current store (via
 * duncecap_store_replace()). This cannot lead to a store migration.
 *
 * The 'replace' operation swaps out the old value for the new value
 * provided, returning the old value, for purposes of the caller doing
 * any necessary memory allocation.  If there was not already an
 * associated item with the correct hash in the table, then NULL will
 * be returned, and the memory location referred to in the found
 * parameter will, if not NULL, be set to false.
 *
 * If you want the value to be set, whether or not the item was in the
 * table, then use duncecap_put().
 *
 * Note that, if you're using a key and a value, pass them together
 * in a single object in the item parameter.
 */
void *
duncecap_replace(duncecap_t *self, hatrack_hash_t hv, void *item, bool *found)
{
    void *ret;

    if (pthread_mutex_lock(&self->mutex)) {
        abort();
    }

    ret = duncecap_store_replace(self->store_current, hv, item, found);

    if (pthread_mutex_unlock(&self->mutex)) {
        abort();
    }

    return ret;
}

/* duncecap_add()
 *
 * Lock the hash table for writing, and when we get ownership of the
 * lock, perform a 'add' operation in the current store (via
 * duncecap_store_add()).
 *
 * The 'add' operation adds an item to the hash table, but only if
 * there isn't currently an item stored with the associated hash
 * value.  If the item would lead to 75% of the buckets being in use,
 * then a table migration will occur (via swimhat_migrate())
 *
 * If an item previously existed, but has since been deleted, the
 * add operation will still succeed.
 *
 * Returns true if the insertion is succesful, and false otherwise.
 */
bool
duncecap_add(duncecap_t *self, hatrack_hash_t hv, void *item)
{
    bool ret;

    if (pthread_mutex_lock(&self->mutex)) {
        abort();
    }

    ret = duncecap_store_add(self->store_current, self, hv, item);

    if (pthread_mutex_unlock(&self->mutex)) {
        abort();
    }

    return ret;
}

/* duncecap_remove()
 *
 * Lock the hash table for writing, and when we get ownership of the
 * lock, perform a 'remove' operation in the current store (via
 * duncecap_store_remove()).
 *
 * The 'remove' operation removes an item to the hash table, if it is
 * already present (i.e., if there is currently an item stored with
 * the associated hash value, at the time of the operation).  If the
 * item would lead to 75% of the buckets being in use, then a table
 * migration will occur (via swimhat_migrate())
 *
 * If an item was successfully removed, the old item will be returned
 * (for purposes of memory management), and the value true will be
 * written to the memory address provided in the 'found' parameter, if
 * appropriate.  If the item wasn't in the table at the time of the
 * operation, then NULL gets returned, and 'found' gets set to false,
 * when a non-NULL address is provided.
 *
 * If an item previously existed, but has since been deleted, the
 * behavior is the same as if the item was never in the table.
 */
void *
duncecap_remove(duncecap_t *self, hatrack_hash_t hv, bool *found)
{
    void *ret;

    if (pthread_mutex_lock(&self->mutex)) {
        abort();
    }

    ret = duncecap_store_remove(self->store_current, self, hv, found);

    if (pthread_mutex_unlock(&self->mutex)) {
        abort();
    }

    return ret;
}

/* duncecap_len()
 *
 * Returns the approximate number of items currently in the
 * table. Note that we strongly discourage using this call, since it
 * is close to meaningless in multi-threaded programs, as the value at
 * the time of check could be dramatically different by the time of
 * use.
 */
uint64_t
duncecap_len(duncecap_t *self)
{
    return self->item_count;
}

/* duncecap_view()
 *
 * This returns an array of hatrack_view_t items, representing all of
 * the items in the hash table, for the purposes of iterating over the
 * items, for any reason. The number of items in the view will be
 * stored in the memory address pointed to by the second parameter,
 * num. If the third parameter (sort) is set to true, then quicksort
 * will be used to sort the items in the view, based on the insertion
 * order.
 */
hatrack_view_t *
duncecap_view(duncecap_t *self, uint64_t *num, bool sort)
{
    hatrack_view_t    *view;
    duncecap_store_t  *store;
    hatrack_view_t    *p;
    duncecap_bucket_t *cur;
    duncecap_bucket_t *end;
    duncecap_record_t  record;
    uint64_t           count;
    uint64_t           last_slot;
    uint64_t           alloc_len;

    store     = duncecap_viewer_enter(self);
    last_slot = store->last_slot;
    alloc_len = sizeof(hatrack_view_t) * (last_slot + 1);
    view      = (hatrack_view_t *)malloc(alloc_len);
    p         = view;
    cur       = store->buckets;
    end       = cur + (last_slot + 1);
    count     = 0;

    while (cur < end) {
        record = atomic_read(&cur->record);

        if (!record.epoch) {
            cur++;
            continue;
        }

        p->item       = record.item;
        p->sort_epoch = record.epoch;

        count++;
        p++;
        cur++;
    }

    *num = count;

    if (!count) {
        free(view);
        duncecap_viewer_exit(self, store);

        return NULL;
    }

    view = (hatrack_view_t *)realloc(view, sizeof(hatrack_view_t) * count);

    if (sort) {
        qsort(view, count, sizeof(hatrack_view_t), hatrack_quicksort_cmp);
    }

    duncecap_viewer_exit(self, store);
    return view;
}

// clang-format off
static duncecap_store_t *
duncecap_store_new(uint64_t size)
{
    duncecap_store_t *ret;
    uint64_t          alloc_len;

    alloc_len      = sizeof(duncecap_store_t);
    alloc_len     += size * sizeof(duncecap_bucket_t);
    ret            = (duncecap_store_t *)calloc(1, alloc_len);
    ret->last_slot = size - 1;
    ret->threshold = hatrack_compute_table_threshold(size);

    return ret;
}

static void *
duncecap_store_get(duncecap_store_t *self, hatrack_hash_t hv, bool *found)
{
    uint64_t           bix;
    uint64_t           last_slot;
    uint64_t           i;
    duncecap_bucket_t *cur;
    duncecap_record_t  record;

    last_slot = self->last_slot;
    bix       = hatrack_bucket_index(hv, last_slot);

    for (i = 0; i <= last_slot; i++) {
        cur = &self->buckets[bix];
	
        if (hatrack_hashes_eq(hv, cur->hv)) {
            /* Since readers can run concurrently to writers, it is
             * possible the hash has been written, but no item has
             * been written yet. So we need to load atomically, then
             * make sure there's something to return.
             */
            record = atomic_read(&cur->record);

            if (record.epoch) {
                if (found) {
                    *found = true;
                }
		
                return record.item;
            }
            else {
                if (found) {
                    *found = false;
                }
		
                return NULL;
            }
        }
        if (hatrack_bucket_unreserved(cur->hv)) {
            if (found) {
                *found = false;
            }
	    
            return NULL;
        }
	
        bix = (bix + 1) & last_slot;
    }
    __builtin_unreachable();
}

static void *
duncecap_store_put(duncecap_store_t *self,
                   duncecap_t       *top,
                   hatrack_hash_t    hv,
                   void             *item,
                   bool             *found)
{
    uint64_t           bix;
    uint64_t           i;
    uint64_t           last_slot;
    duncecap_bucket_t *cur;
    duncecap_record_t  record;
    void              *ret;

    last_slot = self->last_slot;
    bix       = hatrack_bucket_index(hv, last_slot);

    for (i = 0; i <= last_slot; i++) {
        cur = &self->buckets[bix];
	
        if (hatrack_hashes_eq(hv, cur->hv)) {
            record = atomic_load(&cur->record);

            if (!record.epoch) {
                if (found) {
                    *found = false;
                }
		
                record.epoch = top->next_epoch++;
                ret          = NULL;
		
                top->item_count++;
                // The bucket has already been used, so we do NOT bump
                // used_count in this case.
            }
            else {
                if (found) {
                    *found = true;
                }
		
                ret = record.item;
            }

            record.item = item;
            atomic_store(&cur->record, record);

            return ret;
        }
	
        if (hatrack_bucket_unreserved(cur->hv)) {
            if (self->used_count + 1 == self->threshold) {
                duncecap_migrate(top);
		
                return duncecap_store_put(top->store_current,
                                          top,
                                          hv,
                                          item,
                                          found);
            }
            self->used_count++;
            top->item_count++;
	    
            cur->hv      = hv;
            record.item  = item;
            record.epoch = top->next_epoch++;
	    
            atomic_store(&cur->record, record);

            if (found) {
                *found = false;
            }
	    
            return NULL;
        }
	
        bix = (bix + 1) & last_slot;
    }
    __builtin_unreachable();
}

static void *
duncecap_store_replace(duncecap_store_t *self,
                       hatrack_hash_t    hv,
                       void             *item,
                       bool             *found)
{
    uint64_t           bix;
    uint64_t           i;
    uint64_t           last_slot;
    duncecap_bucket_t *cur;
    duncecap_record_t  record;
    void              *ret;

    last_slot = self->last_slot;
    bix       = hatrack_bucket_index(hv, last_slot);

    for (i = 0; i <= last_slot; i++) {
        cur = &self->buckets[bix];
	
        if (hatrack_hashes_eq(hv, cur->hv)) {
            record = atomic_read(&cur->record);

            if (!record.epoch) {
                if (found) {
                    *found = false;
                }
		
                return NULL;
            }
	    
            ret         = record.item;
            record.item = item;

            atomic_store(&cur->record, record);

            if (found) {
                *found = true;
            }
	    
            return ret;
        }
        if (hatrack_bucket_unreserved(cur->hv)) {
            if (found) {
                *found = false;
            }
	    
            return NULL;
        }
	
        bix = (bix + 1) & last_slot;
    }
    __builtin_unreachable();
}

static bool
duncecap_store_add(duncecap_store_t *self,
                   duncecap_t       *top,
                   hatrack_hash_t    hv,
                   void             *item)
{
    uint64_t           bix;
    uint64_t           i;
    uint64_t           last_slot;
    duncecap_bucket_t *cur;
    duncecap_record_t  record;

    last_slot = self->last_slot;
    bix       = hatrack_bucket_index(hv, last_slot);

    for (i = 0; i <= last_slot; i++) {
        cur = &self->buckets[bix];

        if (hatrack_hashes_eq(hv, cur->hv)) {
            record = atomic_read(&cur->record);

            if (record.epoch) {
                return false;
            }
	    
            record.item  = item;
            record.epoch = top->next_epoch++;
	    
            top->item_count++;
            atomic_store(&cur->record, record);
	    
            return true;
        }

        // In this branch, there's definitely nothing there at the
        // time of the operation, and we should add.
        if (hatrack_bucket_unreserved(cur->hv)) {
            if (self->used_count + 1 == self->threshold) {
                duncecap_migrate(top);
                return duncecap_store_add(top->store_current, top, hv, item);
            }
	    
            self->used_count++;
            top->item_count++;
	    
            cur->hv      = hv;
            record.item  = item;
            record.epoch = top->next_epoch++;
	    
            atomic_store(&cur->record, record);

            return true;
        }
	
        bix = (bix + 1) & last_slot;
    }
    __builtin_unreachable();
}

static void *
duncecap_store_remove(duncecap_store_t *self,
                      duncecap_t       *top,
                      hatrack_hash_t    hv,
                      bool             *found)
{
    uint64_t           bix;
    uint64_t           i;
    uint64_t           last_slot;
    duncecap_bucket_t *cur;
    duncecap_record_t  record;
    void              *ret;

    last_slot = self->last_slot;
    bix       = hatrack_bucket_index(hv, last_slot);

    for (i = 0; i <= last_slot; i++) {
        cur = &self->buckets[bix];
	
        if (hatrack_hashes_eq(hv, cur->hv)) {
            record = atomic_read(&cur->record);

            if (!record.epoch) {
                if (found) {
                    *found = false;
                }
		
                return NULL;
            }

            ret          = record.item;
            record.epoch = 0;
	    
            atomic_store(&cur->record, record);
            --top->item_count;

            if (found) {
                *found = true;
            }
	    
            return ret;
        }
	
        if (hatrack_bucket_unreserved(cur->hv)) {
            if (found) {
                *found = false;
            }
	    
            return NULL;
        }
	
        bix = (bix + 1) & last_slot;
    }
    __builtin_unreachable();
}

/* duncecap_migrate()
 *
 * When we call this, we will have a write lock on the table, so it's
 * mostly straightforward to migrate. We only need to make sure that
 * all reader are done reading out of the old store, before we delete
 * it.
 */
static void
duncecap_migrate(duncecap_t *self)
{
    duncecap_store_t  *cur_store;
    duncecap_store_t  *new_store;
    duncecap_bucket_t *cur;
    duncecap_bucket_t *target;
    duncecap_record_t  record;
    uint64_t           new_size;
    uint64_t           cur_last_slot;
    uint64_t           new_last_slot;
    uint64_t           i, n, bix;

    cur_store     = self->store_current;
    new_size      = hatrack_new_size(cur_store->last_slot,
				     duncecap_len(self) + 1);
    cur_last_slot = cur_store->last_slot;
    new_last_slot = new_size - 1;
    new_store     = duncecap_store_new(new_size);

    for (n = 0; n <= cur_last_slot; n++) {
        cur    = &cur_store->buckets[n];
        record = atomic_read(&cur->record);

        if (!record.epoch) {
            continue;
        }
	
        bix = hatrack_bucket_index(cur->hv, new_last_slot);
      
        for (i = 0; i < new_size; i++) {
            target = &new_store->buckets[bix];
	    
            if (hatrack_bucket_unreserved(target->hv)) {
                target->hv = cur->hv;
                atomic_store(&target->record, record);
                break;
            }
	    
            bix = (bix + 1) & new_last_slot;
        }
    }

    new_store->used_count = self->item_count;
    self->store_current   = new_store;

    // Busy-wait for all the readers to exit the old store, before
    // releasing the old store's memory.
    while (atomic_load(&cur_store->readers))
        ;
    
    free(cur_store);

    return;
}

#endif
