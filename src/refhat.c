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
 *  Name:           refhat.c
 *  Description:    A reference hashtable that only works single-threaded.
 *
 *  Author:         John Viega, john@zork.org
 *
 */

#include <hatrack.h>

static void refhat_migrate(refhat_t *);

/* refhat_init()
 *
 * It's expected that refhat instances will be created via the default
 * malloc.  This function cannot rely on zero-initialization of its
 * own object, though it does zero-initialize the buckets it allocates
 * (also with the default memory allocator).
 *
 * For the definition of HATRACK_MIN_SIZE, this is computed in
 * config.h, since we require hash table buckets to always be sized to
 * a power of two. To set the size, you instead set the preprocessor
 * variable HATRACK_MIN_SIZE_LOG.
 *
 * Note that next_epoch starts out as 1, because a 0 value indicates
 * deletion.
 */
void
refhat_init(refhat_t *self)
{
    uint64_t size;

    size             = HATRACK_MIN_SIZE;
    self->last_slot  = size - 1;
    self->threshold  = hatrack_compute_table_threshold(size);
    self->used_count = 0;
    self->item_count = 0;
    self->next_epoch = 1;
    self->buckets    = (refhat_bucket_t *)calloc(size, sizeof(refhat_bucket_t));

    return;
}

/* refhat_get()
 *
 * Looks up the item associated with the passed hash value, returning
 * it, if appropriate. If not, NULL will be returned. If an address is
 * passed in the found parameter, then the value will be set based on
 * whether the item was found in the table. That allows for items to
 * be of arbitrary types. Without this parameter, we'd have to
 * restrict items to pointer types, so that the return value could
 * indicate not-found.  Nonetheless, if you ARE keeping only pointer
 * types, you can safely pass NULL to the found parameter, and only
 * check the return value.
 *
 * With all of our hashtable implementations, we do linear probing to
 * deal with hash collisions.  We keep the table size to a power of
 * two, so that we can wrap around using a bitwise AND (with one minus
 * the table size), as opposed to doing a mod operation, as the mod
 * operation is usually significantly more expensive.
 *
 * We could save a few operaitons by searching only from the initial
 * hashed index (bix) to self->last_slot, and then if that fails,
 * search from index 0 to bix-1. But, that greatly increases the
 * number of lines of code, and make it brittle to changes. Plus, in
 * my initial testing didn't make more than a 2-3% difference in
 * performance, if that.
 *
 * So for now, we'll skip that optimization.
 *
 * Note that there are a few items that we use (as does every other
 * hash table), including:
 *
 * hatrack_hash_t -- A representation of a hash value, as two 64-bit
 *                   words. We use 128-bit hash values to effectively
 *                   eliminate collisions, if good hash functions are
 *                   selected.
 *
 * hatrack_view_t -- A data type that provides all items in the hash
 *                   table (usually at a given moment in time), in
 *                   insertion order, if requested (returned from
 *                   the hashtable_view() function).
 *
 * Functions to examine and compare hash values, to calculate the
 * initial bucket to use, given a hash value, etc. These functions
 * should be self-explainatory for the most part, but see
 * hatrack_common.h for definitions, if needed.
 */
void *
refhat_get(refhat_t *self, hatrack_hash_t hv, bool *found)
{
    uint64_t         bix;
    uint64_t         i;
    refhat_bucket_t *cur;

    bix = hatrack_bucket_index(hv, self->last_slot);

    for (i = 0; i <= self->last_slot; i++) {
        cur = &self->buckets[bix];

        if (hatrack_hashes_eq(hv, cur->hv)) {
            if (!cur->epoch) {
                if (found) {
                    *found = false;
                }

                return NULL;
            }

            if (found) {
                *found = true;
            }

            return cur->item;
        }

        if (hatrack_bucket_unreserved(cur->hv)) {
            if (found) {
                *found = false;
            }

            return NULL;
        }
        bix = (bix + 1) & self->last_slot;
    }
    /* Macro that works w/ Clang and GCC to hint to the compiler that
     * this code cannot be reached; useful both for suppressing
     * warnings about return values, and can aid in compiler
     * optimizations.
     */
    __builtin_unreachable();
}

/* refhat_put()
 *
 * This function will insert the item into the table, whether or not
 * the item is already present. If there is an item present, it will
 * be returned, for the sake of memory management.
 *
 * If an address is provided in the found parameter, the associated
 * memory location will get the value true if the item was already in
 * the table, and false otherwise.
 *
 * Note that, if you're using a key and a value, pass them together
 * in a single object in the item parameter.
 */
void *
refhat_put(refhat_t *self, hatrack_hash_t hv, void *item, bool *found)
{
    uint64_t         bix;
    uint64_t         i;
    refhat_bucket_t *cur;
    void            *ret;

    bix = hatrack_bucket_index(hv, self->last_slot);

    for (i = 0; i <= self->last_slot; i++) {
        cur = &self->buckets[bix];

        if (hatrack_hashes_eq(hv, cur->hv)) {
            if (!cur->epoch) {
                cur->item  = item;
                cur->epoch = self->next_epoch++;
                self->item_count++;

                if (found) {
                    *found = false;
                }

                return NULL;
            }

            ret       = cur->item;
            cur->item = item;

            if (found) {
                *found = true;
            }

            return ret;
        }
        if (hatrack_bucket_unreserved(cur->hv)) {
            if (self->used_count + 1 == self->threshold) {
                refhat_migrate(self);

                return refhat_put(self, hv, item, found);
            }

            self->used_count++;
            self->item_count++;

            cur->hv    = hv;
            cur->item  = item;
            cur->epoch = self->next_epoch++;

            if (found) {
                *found = false;
            }

            return NULL;
        }

        bix = (bix + 1) & self->last_slot;
    }
    __builtin_unreachable();
}

/* refhat_replace()
 *
 * This function replaces an item in the hash table, returning the old
 * value.  If there was not already an associated item with the
 * correct hash in the table, then NULL will be returned, and the
 * memory location referred to in the found parameter will, if not
 * NULL, be set to false.
 *
 * If you want the value to be set, whether or not the item was in the
 * table, then use refhat_put().
 *
 * Note that, if you're using a key and a value, pass them together
 * in a single object in the item parameter.
 */
void *
refhat_replace(refhat_t *self, hatrack_hash_t hv, void *item, bool *found)
{
    uint64_t         bix;
    uint64_t         i;
    refhat_bucket_t *cur;
    void            *ret;

    bix = hatrack_bucket_index(hv, self->last_slot);

    for (i = 0; i <= self->last_slot; i++) {
        cur = &self->buckets[bix];

        if (hatrack_hashes_eq(hv, cur->hv)) {
            if (!cur->epoch) {
                if (found) {
                    *found = false;
                }

                return NULL;
            }

            ret       = cur->item;
            cur->item = item;

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

        bix = (bix + 1) & self->last_slot;
    }
    __builtin_unreachable();
}

/* refhat_add()
 *
 * This function adds an item to the hash table, but only if there
 * isn't currently an item stored with the associated hash value.
 *
 * If an item was deleted since the last resize, we might find the
 * hash value in the table, but not the item. When items are deleted,
 * their epoch value is set to 0, which is not given out to actual
 * inserts.  Anyway, in this scenario where the hash value is there,
 * but the epoch is zero, the add operation should still succeed.
 *
 * Returns true if the insertion is succesful, and false otherwise.
 */
bool
refhat_add(refhat_t *self, hatrack_hash_t hv, void *item)
{
    uint64_t         bix;
    uint64_t         i;
    refhat_bucket_t *cur;

    bix = hatrack_bucket_index(hv, self->last_slot);

    for (i = 0; i <= self->last_slot; i++) {
        cur = &self->buckets[bix];

        if (hatrack_hashes_eq(hv, cur->hv)) {
            if (!cur->epoch) {
                cur->item  = item;
                cur->epoch = self->next_epoch++;
                self->item_count++;

                return true;
            }

            return false;
        }

        if (hatrack_bucket_unreserved(cur->hv)) {
            if (self->used_count + 1 == self->threshold) {
                refhat_migrate(self);

                return refhat_add(self, hv, item);
            }

            self->used_count++;
            self->item_count++;

            cur->hv    = hv;
            cur->item  = item;
            cur->epoch = self->next_epoch++;

            return true;
        }

        bix = (bix + 1) & self->last_slot;
    }
    __builtin_unreachable();
}

/* refhat_remove()
 *
 * This function removes the item associated with the hash value from
 * the table, returning it, and setting the value of found
 * appropriately, if an address is passed in.
 */
void *
refhat_remove(refhat_t *self, hatrack_hash_t hv, bool *found)
{
    uint64_t         bix;
    uint64_t         i;
    refhat_bucket_t *cur;
    void            *ret;

    bix = hatrack_bucket_index(hv, self->last_slot);

    for (i = 0; i <= self->last_slot; i++) {
        cur = &self->buckets[bix];

        if (hatrack_hashes_eq(hv, cur->hv)) {
            if (!cur->epoch) {
                if (found) {
                    *found = false;
                }

                return NULL;
            }

            ret        = cur->item;
            cur->epoch = 0;

            --self->item_count;

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

        bix = (bix + 1) & self->last_slot;
    }
    __builtin_unreachable();
}

/* refhat_delete()
 *
 * This deallocates a table allocated with the default malloc.  The
 * buckets will always be allocated by the default malloc, so if,
 * for some reason, you use a different allocator, use the default
 * malloc on the buckets, and delete the object yourself.
 */
void
refhat_delete(refhat_t *self)
{
    free(self->buckets);
    free(self);

    return;
}

/* refhat_len()
 *
 * Returns the number of items currently in the table. Note that we
 * strongly discourage using this call, since it is close to
 * meaningless in multi-threaded programs, as the value at the time
 * of check could be dramatically different by the time of use.
 */
uint64_t
refhat_len(refhat_t *self)
{
    return self->item_count;
}

/* refhat_view()
 *
 * This returns an array of hatrack_view_t items, representing all of
 * the items in the hash table, for the purposes of iterating over the
 * items, for any reason. The number of items in the view will be
 * stored in the memory address pointed to by the second parameter,
 * num. If the third parameter (sort) is set to true, then quicksort
 * will be used to sort the items in the view, based on the insertion
 * order.
 *
 * Note that we can avoid the cost to sort altogether in a
 * single-threaded hash table by having two arrays. In practice,
 * though, the cost of sorts, despite being n log n, isn't significant
 * in practice, if it isn't a frequent operation.
 *
 * We do explore a similar representation to improve sorting times in
 * multi-threaded tables with our lohat-a and lohat-b tables.
 */
hatrack_view_t *
refhat_view(refhat_t *self, uint64_t *num, bool sort)
{
    hatrack_view_t  *view;
    hatrack_view_t  *p;
    refhat_bucket_t *cur;
    refhat_bucket_t *end;

    view = (hatrack_view_t *)malloc(sizeof(hatrack_view_t) * self->item_count);
    p    = view;
    cur  = self->buckets;
    end  = cur + (self->last_slot + 1);

    while (cur < end) {
        if (hatrack_bucket_unreserved(cur->hv) || !cur->epoch) {
            cur++;
            continue;
        }

        p->item       = cur->item;
        p->sort_epoch = cur->epoch;

        p++;
        cur++;
    }

    *num = self->item_count;

    if (sort) {
        qsort(view,
              self->item_count,
              sizeof(hatrack_view_t),
              hatrack_quicksort_cmp);
    }

    return view;
}

/* refhat_migrate()
 *
 * This function migrates the buckets, to new memory. It is called
 * when 75% of the buckets would be used on an insert. The new size of
 * the table is determined based on how full the current buckets would
 * be, if there were no deleted items. If the table would be more than
 * 50% full, we double the size of the table. If the table would be
 * less than 25% full, then we halve the size of the table.
 *
 * Otherwise, we leave the table the same size.
 *
 * The calculation to figure out how many buckets to allocate is done
 * by hatrack_new_size(), which lives in hatrack_common.c
 */
static void
refhat_migrate(refhat_t *self)
{
    refhat_bucket_t *new_buckets;
    refhat_bucket_t *cur;
    refhat_bucket_t *target;
    uint64_t         bucket_size;
    uint64_t         num_buckets;
    uint64_t         new_last_slot;
    uint64_t         i, n, bix;

    bucket_size   = sizeof(refhat_bucket_t);
    num_buckets   = hatrack_new_size(self->last_slot, self->item_count + 1);
    new_last_slot = num_buckets - 1;
    new_buckets   = (refhat_bucket_t *)calloc(num_buckets, bucket_size);

    for (n = 0; n <= self->last_slot; n++) {
        cur = &self->buckets[n];

        if (hatrack_bucket_unreserved(cur->hv) || !cur->epoch) {
            continue;
        }

        bix = hatrack_bucket_index(cur->hv, new_last_slot);

        for (i = 0; i < num_buckets; i++) {
            target = &new_buckets[bix];

            if (hatrack_bucket_unreserved(target->hv)) {
                target->hv    = cur->hv;
                target->item  = cur->item;
                target->epoch = cur->epoch;
                break;
            }

            bix = (bix + 1) & new_last_slot;
        }
    }

    free(self->buckets);

    self->used_count = self->item_count;
    self->buckets    = new_buckets;
    self->last_slot  = new_last_slot;
    self->threshold  = hatrack_compute_table_threshold(num_buckets);

    return;
}
