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
 *  Name:           newshat.c
 *  Description:    Now Everyone Writes Simultaneously (HAsh Table)
 *
 *                  Uses pthread locks on a per-bucket basis, and
 *                  allows multiple simultaneous writers, except when
 *                  performing table migration.
 *
 *  Author:         John Viega, john@zork.org
 *
 */

#include "newshat.h"

// clang-format off

// Not static, because tophat needs to call it, but nonetheless, don't
// stick it in our public prototypes.
       newshat_store_t *newshat_store_new    (uint64_t);
static void             newshat_store_delete (newshat_store_t *);
static void            *newshat_store_get    (newshat_store_t *, newshat_t *,
					      hatrack_hash_t *, bool *);
static void            *newshat_store_put    (newshat_store_t *, newshat_t *,
					      hatrack_hash_t *, void *,
					      bool *);
static void            *newshat_store_replace(newshat_store_t *, newshat_t *,
					      hatrack_hash_t *, void *,
					      bool *);
static bool             newshat_store_add    (newshat_store_t *, newshat_t *,
					      hatrack_hash_t *, void *);
static void            *newshat_store_remove (newshat_store_t *, newshat_t *,
					      hatrack_hash_t *, bool *);
static newshat_store_t *newshat_store_migrate(newshat_store_t *, newshat_t *);

// clang-format on

/* newshat_init()
 *
 * It's expected that newshat instances will be created via the
 * default malloc.  This function cannot rely on zero-initialization
 * of its own object.
 *
 * For the definition of HATRACK_MIN_SIZE, this is computed in
 * config.h, since we require hash table buckets to always be sized to
 * a power of two. To set the size, you instead set the preprocessor
 * variable HATRACK_MIN_SIZE_LOG.
 */
void
newshat_init(newshat_t *self)
{
    newshat_store_t *store = newshat_store_new(HATRACK_MIN_SIZE);
    self->item_count       = 0;
    self->next_epoch       = 1; // 0 is reserved for empty buckets.
    self->store_current    = store;
    pthread_mutex_init(&self->migrate_mutex, NULL);

    return;
}
/* newshat_get(), _put(), _replace(), _add(), _remove()
 *
 * These functions need to safely acquire a reference to the current
 * store, before looking for the hash value in the store, to make sure
 * they don't end up dereferencing a pointer to memory that's been
 * freed. We do so, by using our memory management implementation,
 * mmm.
 *
 * Essentially, mmm keeps a global, atomically updated counter of
 * memory "epochs". Each write operation starts a new epoch. Each
 * memory object records its "write" epoch, as well as its "retire"
 * epoch, meaning the epoch in which mmm_retire() was called.
 *
 * The way mmm protects from freeing data that might be in use by
 * parallel threads, is as follows:
 *
 * 1) All threads "register" by writing the current epoch into a
 *    special array, when they start an operation.  This is done via
 *    mmm_start_basic_op(), which is inlined and defined in mmm.h.
 *    Essentially, the algorithm will ensure that, if a thread has
 *    registered for an epoch, no values from that epoch onward will
 *    be deleted.
 *
 * 2) When the operation is done, they "unregister", via mmm_end_op().
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
 * There are more options with mmm, that we don't use in newshat. See
 * mmm.c for more details on the algorithm, and options.
 *
 * Once the reference is required, we delegate to the function
 * newshat_store_<whatever_is_appropriate>() to do the work. Note that
 * the API (including use of the found parameter) works as with every
 * other hash table; see refhat.c or swimcap.c for more details on the
 * parameters, if needed.
 */
void *
newshat_get(newshat_t *self, hatrack_hash_t *hv, bool *found)
{
    void *ret;

    mmm_start_basic_op();
    ret = newshat_store_get(self->store_current, self, hv, found);
    mmm_end_op();

    return ret;
}

void *
newshat_put(newshat_t *self, hatrack_hash_t *hv, void *item, bool *found)
{
    void *ret;

    mmm_start_basic_op();
    ret = newshat_store_put(self->store_current, self, hv, item, found);
    mmm_end_op();

    return ret;
}

void *
newshat_replace(newshat_t *self, hatrack_hash_t *hv, void *item, bool *found)
{
    void *ret;

    mmm_start_basic_op();
    ret = newshat_store_replace(self->store_current, self, hv, item, found);
    mmm_end_op();

    return ret;
}

bool
newshat_add(newshat_t *self, hatrack_hash_t *hv, void *item)
{
    bool ret;

    mmm_start_basic_op();
    ret = newshat_store_add(self->store_current, self, hv, item);
    mmm_end_op();

    return ret;
}

void *
newshat_remove(newshat_t *self, hatrack_hash_t *hv, bool *found)
{
    void *ret;

    mmm_start_basic_op();
    ret = newshat_store_remove(self->store_current, self, hv, found);
    mmm_end_op();

    return ret;
}

/*
 * newshat_delete()
 *
 * Deletes a newshat object. Generally, you should be confident that
 * all threads except the one from which you're calling this have
 * stopped using the table (generally meaning they no longer hold a
 * reference to the store).
 *
 * Note that this function assumes the newshat object was allocated
 * via the default malloc. If it wasn't, don't call this directly, but
 * do note that the stores were created via mmm_alloc(), and the most
 * recent store will need to be retired via mmm_retire. And the
 * migration mutex needs to be destroyed, as well.
 *
 * The mmm_retire() call will handle destroying any per-bucket
 * mutexes, once there are no readers in the store.
 */
void
newshat_delete(newshat_t *self)
{
    mmm_retire(self->store_current);
    if (pthread_mutex_destroy(&self->migrate_mutex)) {
        abort();
    }
    free(self);

    return;
}

/* newshat_len()
 *
 * Returns the approximate number of items currently in the
 * table. Note that we strongly discourage using this call, since it
 * is close to meaningless in multi-threaded programs, as the value at
 * the time of check could be dramatically different by the time of
 * use.
 */
uint64_t
newshat_len(newshat_t *self)
{
    return self->item_count;
}

/* newshat_view()
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
newshat_view(newshat_t *self, uint64_t *num, bool sort)
{
    hatrack_view_t   *view;
    newshat_store_t  *store;
    hatrack_view_t   *p;
    newshat_bucket_t *cur;
    newshat_bucket_t *end;
    newshat_record_t  contents;
    uint64_t          count;
    uint64_t          last_slot;
    uint64_t          alloc_len;

    mmm_start_basic_op();

    store     = self->store_current;
    last_slot = store->last_slot;
    alloc_len = sizeof(hatrack_view_t) * (last_slot + 1);
    view      = (hatrack_view_t *)malloc(alloc_len);
    p         = view;
    cur       = store->buckets;
    end       = cur + (last_slot + 1);
    count     = 0;

    while (cur < end) {
        /* Because everything we need is in the contents field, and
         * because mmm ensures we will not have the store deleted out
         * from underneath us, it's safe for us to read each bucket
         * individually, without any locking -- in the context of
         * individual buckets, we won't get inconsistent states,
         * even though our view of the table overall might end up
         * inconsistent.
         *
         * We *could* achieve full consistency by holding every bucket
         * lock in parallel, as we build our view, and then releasing
         * them all at the end.
         *
         * However, that would unnecessarily stall other readers.  We
         * provide another solution to this problem in several other
         * hash tables, including ballcap, lohat0, lohat1 and woolhat.
         */
        contents = atomic_read(&cur->contents);

        if (!contents.info) {
            cur++;
            continue;
        }
        p->hv         = cur->hv;
        p->item       = contents.item;
        p->sort_epoch = contents.info;
        count++;
        p++;
        cur++;
    }

    *num = count;

    if (!count) {
        free(view);
        mmm_end_op();
        return NULL;
    }

    view = (hatrack_view_t *)realloc(view, sizeof(hatrack_view_t) * count);

    if (sort) {
        qsort(view, count, sizeof(hatrack_view_t), hatrack_quicksort_cmp);
    }

    mmm_end_op();

    return view;
}

/* newshat_store_new()
 *
 * Note that this function uses MMM's facility for registering a
 * cleanup handler on a per-allocation basis. Every store will call
 * newshat_store_delete() when it's time to ACTUALLY return memory for
 * the store. This allows us to defer tearing down the per-bucket
 * mutexes until we're confident all readers are done with the
 * store. If we were to try to tear down the mutexes at the time when
 * we retire the store, we'd ultimately end up with thread hangs, as
 * threads waiting on mutexes will find they never acquire the mutex,
 * due to deletion.
 *
 * Here, new stores being allocated with mmm_alloc_committed, the
 * underlying memory is zeroed out, so we only need to initialize
 * non-zero items.
 */
newshat_store_t *
newshat_store_new(uint64_t size)
{
    newshat_store_t *ret;
    uint64_t         i;
    uint64_t         alloc_len;

    alloc_len = sizeof(newshat_store_t) + size * sizeof(newshat_bucket_t);
    ret       = (newshat_store_t *)mmm_alloc_committed(alloc_len);

    mmm_add_cleanup_handler(ret, (void (*)(void *))newshat_store_delete);

    ret->last_slot = size - 1;
    ret->threshold = hatrack_compute_table_threshold(size);

    for (i = 0; i <= ret->last_slot; i++) {
        pthread_mutex_init(&ret->buckets[i].mutex, NULL);
    }

    return ret;
}

// Only called via the memory management cleanup handler, to
// deallocated mutexes.
static void
newshat_store_delete(newshat_store_t *self)
{
    uint64_t i;

    for (i = 0; i <= self->last_slot; i++) {
        pthread_mutex_destroy(&self->buckets[i].mutex);
    }
}

/*
 * Since we read contents atomically, and everything we need is in
 * there, readers do not have to hold the lock.
 */
static void *
newshat_store_get(newshat_store_t *self,
                  newshat_t       *top,
                  hatrack_hash_t  *hv,
                  bool            *found)
{
    uint64_t          bix;
    uint64_t          last_slot;
    uint64_t          i;
    newshat_bucket_t *cur;
    newshat_record_t  contents;

    last_slot = self->last_slot;
    bix       = hatrack_bucket_index(hv, last_slot);

    for (i = 0; i <= last_slot; i++) {
        cur = &self->buckets[bix];

        if (hatrack_hashes_eq(hv, &cur->hv)) {
            contents = atomic_read(&cur->contents);

            if (!contents.info) {
                if (found) {
                    *found = false;
                }
                return NULL;
            }
            if (found) {
                *found = true;
            }
            return contents.item;
        }
        if (hatrack_bucket_unreserved(&cur->hv)) {
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
newshat_store_put(newshat_store_t *self,
                  newshat_t       *top,
                  hatrack_hash_t  *hv,
                  void            *item,
                  bool            *found)
{
    uint64_t          bix;
    uint64_t          i;
    uint64_t          last_slot;
    newshat_bucket_t *cur;
    newshat_record_t  contents;
    void             *ret;

    last_slot = self->last_slot;
    bix       = hatrack_bucket_index(hv, last_slot);

    for (i = 0; i <= last_slot; i++) {
        cur = &self->buckets[bix];
        /* Hashes are written once, and then never altered. However,
         * they're 128-bits, and could be written out in two 64-bit
         * chunks. So if two threads are doing parallel puts of the
         * same value, they need to be able to both end up in this
         * bucket.
         *
         * As a result, put and add operations need to lock BEFORE
         * checking the bucket. But, get, replace and remove
         * operations can wait until after the hash test, because they
         * don't care if the bucket is half-written (they will end up
         * with a miss, because the bucket isn't written, resulting in
         * no action, which is the correct outcome).
         */
        if (pthread_mutex_lock(&cur->mutex)) {
            abort();
        }
        // We could have checked this before acquiring the lock,
        // but we would still have to check it again after. It's
        // not worth checking twice.
        if (cur->migrated) {
            if (pthread_mutex_unlock(&cur->mutex)) {
                abort();
            }
            return newshat_store_put(top->store_current, top, hv, item, found);
        }
        if (hatrack_hashes_eq(hv, &cur->hv)) {
            contents = atomic_read(&cur->contents);

            if (!contents.info) {
                contents.item = item;
                contents.info = top->next_epoch++; // For sort ordering.
                atomic_store(&cur->contents, contents);
                top->item_count++;
                if (found) {
                    *found = false;
                }
                if (pthread_mutex_unlock(&cur->mutex)) {
                    abort();
                }
                return NULL;
            }
            ret           = contents.item;
            contents.item = item;
            // Note that, since we're overwriting something that was already
            // here, we don't need to update the epoch or the USED flag.
            atomic_store(&cur->contents, contents);
            if (found) {
                *found = true;
            }
            if (pthread_mutex_unlock(&cur->mutex)) {
                abort();
            }
            return ret;
        }
        if (hatrack_bucket_unreserved(&cur->hv)) {
            if (self->used_count == self->threshold) {
                if (pthread_mutex_unlock(&cur->mutex)) {
                    abort();
                }
                self = newshat_store_migrate(self, top);
                return newshat_store_put(self, top, hv, item, found);
            }
            self->used_count++;
            top->item_count++;
            cur->hv       = *hv;
            contents.item = item;
            contents.info = top->next_epoch++;
            atomic_store(&cur->contents, contents);
            if (found) {
                *found = false;
            }
            if (pthread_mutex_unlock(&cur->mutex)) {
                abort();
            }
            return NULL;
        }
        if (pthread_mutex_unlock(&cur->mutex)) {
            abort();
        }
        bix = (bix + 1) & last_slot;
    }
    __builtin_unreachable();
}

static void *
newshat_store_replace(newshat_store_t *self,
                      newshat_t       *top,
                      hatrack_hash_t  *hv,
                      void            *item,
                      bool            *found)
{
    uint64_t          bix;
    uint64_t          i;
    uint64_t          last_slot;
    newshat_bucket_t *cur;
    newshat_record_t  contents;
    void             *ret;

    last_slot = self->last_slot;

    bix = hatrack_bucket_index(hv, last_slot);

    for (i = 0; i <= last_slot; i++) {
        cur = &self->buckets[bix];
        // Unlike put and add operations, we only need to lock the
        // bucket once we've made sure we've found the right one.
        // See the note in newshat_store_put() for more detail.
        if (hatrack_hashes_eq(hv, &cur->hv)) {
            if (pthread_mutex_lock(&cur->mutex)) {
                abort();
            }
            if (cur->migrated) {
                if (pthread_mutex_unlock(&cur->mutex)) {
                    abort();
                }
                return newshat_store_put(top->store_current,
                                         top,
                                         hv,
                                         item,
                                         found);
            }
            contents = atomic_read(&cur->contents);
            if (!contents.info) {
                if (found) {
                    *found = false;
                }
                if (pthread_mutex_unlock(&cur->mutex)) {
                    abort();
                }
                return NULL;
            }
            ret           = contents.item;
            contents.item = item;
            // Note that, since we're overwriting something that was already
            // here, we don't need to update the epoch or the USED flag.
            atomic_store(&cur->contents, contents);
            if (found) {
                *found = true;
            }
            if (pthread_mutex_unlock(&cur->mutex)) {
                abort();
            }
            return ret;
        }
        if (hatrack_bucket_unreserved(&cur->hv)) {
            if (found) {
                *found = false;
            }
            return NULL;
        }
        bix = (bix + 1) & last_slot;
    }
    __builtin_unreachable();
}

bool
newshat_store_add(newshat_store_t *self,
                  newshat_t       *top,
                  hatrack_hash_t  *hv,
                  void            *item)
{
    uint64_t          bix;
    uint64_t          i;
    uint64_t          last_slot;
    newshat_bucket_t *cur;
    newshat_record_t  contents;

    last_slot = self->last_slot;
    bix       = hatrack_bucket_index(hv, last_slot);

    for (i = 0; i <= last_slot; i++) {
        cur = &self->buckets[bix];

        // This operation needs to acquire the bucket lock before
        // running tests, per the comment in newshat_store_put().
        if (pthread_mutex_lock(&cur->mutex)) {
            abort();
        }
        if (cur->migrated) {
            if (pthread_mutex_unlock(&cur->mutex)) {
                abort();
            }
            return newshat_store_add(top->store_current, top, hv, item);
        }
        if (hatrack_hashes_eq(hv, &cur->hv)) {
            contents = atomic_read(&cur->contents);
            if (!contents.info) {
                contents.item = item;
                contents.info = top->next_epoch++;
                atomic_store(&cur->contents, contents);
                top->item_count++;
                if (pthread_mutex_unlock(&cur->mutex)) {
                    abort();
                }
                return true;
            }
            if (pthread_mutex_unlock(&cur->mutex)) {
                abort();
            }
            return false;
        }
        if (hatrack_bucket_unreserved(&cur->hv)) {
            if (self->used_count == self->threshold) {
                if (pthread_mutex_unlock(&cur->mutex)) {
                    abort();
                }
                self = newshat_store_migrate(self, top);
                return newshat_store_add(self, top, hv, item);
            }
            self->used_count++;
            top->item_count++;
            cur->hv       = *hv;
            contents.item = item;
            contents.info = top->next_epoch++;
            atomic_store(&cur->contents, contents);
            if (pthread_mutex_unlock(&cur->mutex)) {
                abort();
            }
            return true;
        }
        if (pthread_mutex_unlock(&cur->mutex)) {
            abort();
        }
        bix = (bix + 1) & last_slot;
    }
    __builtin_unreachable();
}

void *
newshat_store_remove(newshat_store_t *self,
                     newshat_t       *top,
                     hatrack_hash_t  *hv,
                     bool            *found)
{
    uint64_t          bix;
    uint64_t          i;
    uint64_t          last_slot;
    newshat_bucket_t *cur;
    newshat_record_t  contents;
    void             *ret;

    last_slot = self->last_slot;
    bix       = hatrack_bucket_index(hv, last_slot);

    for (i = 0; i <= last_slot; i++) {
        cur = &self->buckets[bix];
        if (hatrack_bucket_unreserved(&cur->hv)) {
            if (found) {
                *found = false;
            }
            return NULL;
        }
        if (hatrack_hashes_eq(hv, &cur->hv)) {
            if (pthread_mutex_lock(&cur->mutex)) {
                abort();
            }
            if (cur->migrated) {
                if (pthread_mutex_unlock(&cur->mutex)) {
                    abort();
                }
                return newshat_store_remove(top->store_current, top, hv, found);
            }
            contents = atomic_read(&cur->contents);
            if (!contents.info) {
                if (found) {
                    *found = false;
                }
                if (pthread_mutex_unlock(&cur->mutex)) {
                    abort();
                }
                return NULL;
            }

            ret           = contents.item;
            contents.info = 0; // No epoch, empty bucket.

            atomic_store(&cur->contents, contents);
            --top->item_count;

            if (found) {
                *found = true;
            }
            if (pthread_mutex_unlock(&cur->mutex)) {
                abort();
            }
            return ret;
        }
        bix = (bix + 1) & last_slot;
    }
    __builtin_unreachable();
}

static newshat_store_t *
newshat_store_migrate(newshat_store_t *store, newshat_t *top)
{
    newshat_store_t  *new_store;
    newshat_bucket_t *cur;
    newshat_bucket_t *target;
    newshat_record_t  contents;
    uint64_t          new_size;
    uint64_t          cur_last_slot;
    uint64_t          new_last_slot;
    uint64_t          i, n, bix;
    uint64_t          items_to_migrate = 0;

    // Besides the mutex for each bucket, we have a migration mutex
    // that ensures only one thread is working on migrations.
    // We're definitely relying on a fair scheduler.
    if (pthread_mutex_lock(&top->migrate_mutex)) {
        abort();
    }

    /* Our first order of business once we acquire the lock is to make
     * sure that there's still work to do.  Esentially, the first
     * thread to grab the lock does the work, and the remaining
     * threads block until the migrating thread finishes.
     */
    if (store != top->store_current) {
        // Someone else migrated it, and now we can go finish our
        // write (handled by the code that called newshat_store_migrate).
        new_store = top->store_current;
        if (pthread_mutex_unlock(&top->migrate_mutex)) {
            abort();
        }
        return new_store;
    }

    /* At this point, we've acquired the migration lock, but we will
     * need to prevent updates to buckets after we've migrated them.
     * We're going to accomplish that by going ahead and grabbing
     * every single bucket lock before we actually migrate.
     */
    cur_last_slot = store->last_slot;

    for (n = 0; n <= cur_last_slot; n++) {
        cur      = &store->buckets[n];
        contents = atomic_read(&cur->contents);

        if (pthread_mutex_lock(&cur->mutex)) {
            abort();
        }
        if (contents.info) {
            items_to_migrate++;
        }
    }

    new_size      = hatrack_new_size(cur_last_slot, items_to_migrate + 1);
    new_last_slot = new_size - 1;
    new_store     = newshat_store_new(new_size);

    for (n = 0; n <= cur_last_slot; n++) {
        cur      = &store->buckets[n];
        contents = atomic_read(&cur->contents);
        if (!contents.info) {
            continue;
        }
        bix = hatrack_bucket_index(&cur->hv, new_last_slot);
        for (i = 0; i < new_size; i++) {
            target = &new_store->buckets[bix];
            if (hatrack_bucket_unreserved(&target->hv)) {
                target->hv = cur->hv;
                atomic_store(&target->contents, contents);
                break;
            }
            bix = (bix + 1) & new_last_slot;
        }
        cur->migrated = true;
    }

    new_store->used_count = top->item_count;
    top->store_current    = new_store;

    // The old store cannot be deleted immediately, as it might have
    // readers. We use mmm to ensure it's not freed until it's safe.
    // see mmm.c for more details.
    mmm_retire(store);

    /* The new store is officially open at this point, but we
     * still need to go through and unblock readers, who will
     * figure out that they need to change stores.
     * That's a two-step process:
     *
     * 1) We need to unlock every bucket.
     * 2) We need to unlock the migration mutex.
     */
    for (n = 0; n <= cur_last_slot; n++) {
        cur = &store->buckets[n];
        if (pthread_mutex_unlock(&cur->mutex)) {
            abort();
        }
    }

    if (pthread_mutex_unlock(&top->migrate_mutex)) {
        abort();
    }

    return new_store;
}
