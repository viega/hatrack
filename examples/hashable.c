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
 *  Name:           hashable.c
 *
 *  Description:    This example shows how to create a "hashable"
 *                  object type, using a toy example for a string class
 *                  (one with no methods implemented :).
 *
 *                  We show the ability to hash memory at a specific
 *                  offset from the start of an object, as well as the
 *                  ability to cache hash values at an offset.
 *
 *                  The main program creates a dictionary mapping
 *                  command line arguments to their position, and
 *                  warns whenever a key is duplicated.
 *
 *                  As we cache hash values, we print them out. Then,
 *                  we print the unique arguments, one per line.
 *
 *                  Then we do the same things, using sets instead of
 *                  dictionaries.
 *
 *  Author:         John Viega, john@zork.org
 */

#include <hatrack.h>

#include <stdio.h>

/* ex_str_t
 *
 * A very basic string class. Beyond the length and raw bytes, it contains
 * two fields, a reference count that we use for memory management, and a
 * cached hash value, so that we only ever need to calculate it once per
 * object.
 */
typedef struct {
    uint64_t         len; // Not including the null.
    _Atomic uint64_t refcount;
    hatrack_hash_t   hv;
    char             bytes[]; // Null-terminated.
} ex_str_t;

/* This initializes a string object from a C string, setting up the
 * initial reference count (the caller implicitly gets a reference).
 *
 * We call hatrack_bucket_initialize() to portably zero-out the stored
 * hash value.
 */
ex_str_t *
ex_str_from_cstr(char *s)
{
    ex_str_t *ret;
    uint64_t  len;

    len           = strlen(s);
    ret           = (ex_str_t *)calloc(1, sizeof(ex_str_t) + len + 1);
    ret->len      = len;
    ret->refcount = 1;

    hatrack_bucket_initialize(&ret->hv);

    strcpy(ret->bytes, s);

    return ret;
}

void
ex_str_incref(ex_str_t *obj)
{
    atomic_fetch_add(&obj->refcount, 1);
}

// Just for demo purposes, returns true when a decref frees an object.
bool
ex_str_decref(ex_str_t *obj)
{
    /* Remember, atomic_fetch_sub() returns the fetched value, as it
     * exists prior to the subtraction. So we know we're the last
     * decref when we read 1.
     */
    if (atomic_fetch_sub(&obj->refcount, 1) == 1) {
        free(obj);
        return true;
    }

    return false;
}

// Annnd, that's it for our "string API"!

/* This is a callback, which gets called when an object is "removed"
 * from the table.
 *
 * In this example, we incref as we put into the table, as if we're
 * going to drop the reference to go off and do other things.  This is
 * where we decref, once something is being ejected from the table.
 *
 * Note that, since the value is an int, we leave that alone, we only
 * worry about the key. Also, since sets don't have keys, don't get
 * to reuse this function.
 */
static void
dict_decref_for_table(hatrack_dict_t *unused, hatrack_dict_item_t *item)
{
    ex_str_t *str;

    str = (ex_str_t *)item->key;

    printf("Decref on eject of string '%s' (@ %p)\n", str->bytes, str);

    if (ex_str_decref((ex_str_t *)str)) {
        printf("(no more refereces; calling free!)\n");
    }

    return;
}

/* Same basic idea, but since there are no values, sets get passed the
 * item directly, instead of in a container.
 */
static void
set_decref_for_table(hatrack_set_t *unused, ex_str_t *str)
{
    printf("Decref on eject of string '%s' (@ %p)\n", str->bytes, str);

    if (ex_str_decref((ex_str_t *)str)) {
        printf("(no more references; calling free!)\n");
    }
}

/* This gets called on each item that gets passed OUT of the hash
 * table. In this example, it will get called once for every item in a
 * view.
 *
 * Note that, in dictionaries, this function gets called with either a
 * key or a value-- you register a handler for keys seprately from
 * values. That's done because, as with this example, you might have
 * the need for different memory management strategies for keys and
 * values (again, values are copied around; we don't store them in
 * dynamically allocated memory).
 */
static void
both_incref_on_ret(hatrack_dict_t *unused, ex_str_t *returning)
{
    ex_str_incref(returning);
    printf("Incref of string '%s' on return (@%p)\n",
           returning->bytes,
           returning);
}

void
dict_example(int argc, ex_str_t *argv[])
{
    hatrack_dict_t      *dict;
    hatrack_dict_item_t *items;
    uint64_t             i;
    uint64_t             num;
    ex_str_t            *s;

    dict = hatrack_dict_new(HATRACK_DICT_KEY_TYPE_OBJ_CSTR);

    // Tell the hash table where in the data structure to find the
    // char * to hash.
    hatrack_dict_set_hash_offset(dict, offsetof(ex_str_t, bytes));

    // Tell the hash table where to cache the hash value.
    hatrack_dict_set_cache_offset(dict, offsetof(ex_str_t, hv));

    // Set up the memory handler for when items are ejected from the
    // table.
    hatrack_dict_set_free_handler(dict,
                                  (hatrack_mem_hook_t)dict_decref_for_table);

    /* Set up the handler that notifies us to incref objects, before
     * they lose their "protection".
     *
     * If we don't use this, another thread could cause the string
     * we read to be freed before we get the chance to increment the
     * reference count.
     */
    hatrack_dict_set_key_return_hook(dict,
                                     (hatrack_mem_hook_t)both_incref_on_ret);

    // Since we're mapping strings to numbers, we don't worry about
    // memory handlers for the values.
    for (i = 0; i < (uint64_t)argc; i++) {
        s = argv[i];

        if (!hatrack_dict_add(dict, s, (void *)i)) {
            fprintf(stderr,
                    "Detected duplicate argument at argv[%ld]: %s\n",
                    i,
                    argv[i]->bytes);
        }
        else {
            printf("Incref %s to put into the dict (@ %p)\n", s->bytes, s);
            ex_str_incref(s);
        }
    }

    items = hatrack_dict_items_sort(dict, &num);

    printf("Unique arguments:\n");

    for (i = 0; i < num; i++) {
        s = (ex_str_t *)items[i].key;
        printf("%s (@ arg #%ld)\n", s->bytes, (long)items[i].value);

        printf("decref %s from view since we're done w/ it (@%p)\n",
               s->bytes,
               s);
        if (ex_str_decref(s)) {
            printf("(no more references; calling free!)\n");
        }
    }

    free(items);

    // This will cause our callback to run on each item, having us
    // decref each item.
    hatrack_dict_delete(dict);

    return;
}

/* This is all fairly similar to the above.
 *
 * The major difference is that hash values should already be cached.
 * We check to see if it's really cached before we insert.
 */
void
set_example(int argc, ex_str_t *argv[])
{
    hatrack_set_t *set;
    ex_str_t     **items;
    uint64_t       i;
    uint64_t       num;
    ex_str_t      *s;

    set = hatrack_set_new(HATRACK_DICT_KEY_TYPE_OBJ_CSTR);

    hatrack_set_set_hash_offset(set, offsetof(ex_str_t, bytes));
    hatrack_set_set_cache_offset(set, offsetof(ex_str_t, hv));

    hatrack_set_set_free_handler(set, (hatrack_mem_hook_t)set_decref_for_table);
    hatrack_set_set_return_hook(set, (hatrack_mem_hook_t)both_incref_on_ret);

    for (i = 0; i < (uint64_t)argc; i++) {
        s = argv[i];

        if (s->hv) {
            printf("Found cached hash value: %016lx%016lx\n",
                   (long)(s->hv & 0xffffffffffffffff),
                   (long)(s->hv >> 64));
        }
        else {
            printf("Uh-oh, didn't find a cached hash value :-(\n");
        }

        if (!hatrack_set_add(set, s)) {
            fprintf(stderr,
                    "Detected duplicate argument at argv[%ld]: %s\n",
                    (long)i,
                    argv[i]->bytes);
        }
        else {
            printf("Incref %s to put into the set (@ %p)\n", s->bytes, s);
            ex_str_incref(s);
        }
    }

    items = (ex_str_t **)hatrack_set_items_sort(set, &num);

    printf("Unique arguments:\n");

    for (i = 0; i < num; i++) {
        s = items[i];
        printf("%s\n", s->bytes);
        printf("decref %s from view since we're done w/ it (@%p)\n",
               s->bytes,
               s);
        if (ex_str_decref(s)) {
            printf("(no more references; calling free!)\n");
        }
    }

    free(items);

    hatrack_set_delete(set);

    return;
}

ex_str_t **
instantiate_objects(int argc, char *argv[])
{
    ex_str_t **ret;
    int        i;

    ret = (ex_str_t **)malloc(sizeof(ex_str_t *) * argc);

    for (i = 0; i < argc; i++) {
        ret[i] = ex_str_from_cstr(argv[i]);
    }

    return ret;
}

int
main(int argc, char *argv[])
{
    ex_str_t **str_objs;
    int        i;

    str_objs = instantiate_objects(argc, argv);

    dict_example(argc, str_objs);
    // Now let's do the same thing with sets.
    set_example(argc, str_objs);

    for (i = 0; i < argc; i++) {
        printf("Decrefing string '%s' before exit (@%p)\n",
               str_objs[i]->bytes,
               str_objs[i]);
        if (ex_str_decref(str_objs[i])) {
            printf("(no more refereces; calling free!)\n");
        }
    }

    return 0;
}
