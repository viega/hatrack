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

typedef struct {
    uint64_t       len; // Not including the null.
    hatrack_hash_t hv;
    char           bytes[]; // Null-terminated.
} ex_str_t;

ex_str_t *
ex_str_from_cstr(char *s)
{
    ex_str_t *ret;
    uint64_t  len;

    len      = strlen(s);
    ret      = (ex_str_t *)calloc(1, sizeof(ex_str_t) + len + 1);
    ret->len = len;

    hatrack_bucket_initialize(&ret->hv);

    strcpy(ret->bytes, s);

    return ret;
}

void
dict_example(int argc, char *argv[])
{
    hatrack_dict_t      *dict;
    hatrack_dict_item_t *items;
    uint64_t             i;
    uint64_t             num;
    ex_str_t            *s;

    dict = hatrack_dict_new(HATRACK_DICT_KEY_TYPE_OBJ_CSTR);

    hatrack_dict_set_hash_offset(dict, offsetof(ex_str_t, bytes));
    hatrack_dict_set_cache_offset(dict, offsetof(ex_str_t, hv));

    for (i = 0; i < (uint64_t)argc; i++) {
        s = ex_str_from_cstr(argv[i]);

        if (!hatrack_dict_add(dict, s, (void *)i)) {
            fprintf(stderr,
                    "Detected duplicate argument at argv[%lld]: %s\n",
                    i,
                    argv[i]);
        }
        else {
            printf("cached hash: %llx%llx\n",
                   (long long)(s->hv & 0xffffffffffffffff),
                   (long long)(s->hv >> 64));
        }
    }

    items = hatrack_dict_items_sort(dict, &num);

    printf("Unique arguments:\n");

    for (i = 0; i < num; i++) {
        s = (ex_str_t *)items[i].key;
        printf("%s (@ arg #%lld)\n", s->bytes, (uint64_t)items[i].value);
        free(s);
    }

    hatrack_dict_delete(dict);

    return;
}

void
set_example(int argc, char *argv[])
{
    hatrack_set_t *set;
    ex_str_t     **items;
    uint64_t       i;
    uint64_t       num;
    ex_str_t      *s;

    set = hatrack_set_new(HATRACK_DICT_KEY_TYPE_OBJ_CSTR);

    hatrack_set_set_hash_offset(set, offsetof(ex_str_t, bytes));
    hatrack_set_set_cache_offset(set, offsetof(ex_str_t, hv));

    for (i = 0; i < (uint64_t)argc; i++) {
        s = ex_str_from_cstr(argv[i]);

        if (!hatrack_set_add(set, s)) {
            fprintf(stderr,
                    "Detected duplicate argument at argv[%lld]: %s\n",
                    i,
                    argv[i]);
        }
        else {
            printf("cached hash: %llx%llx\n",
                   (long long)(s->hv & 0xffffffffffffffff),
                   (long long)(s->hv >> 64));
        }
    }

    items = (ex_str_t **)hatrack_set_items_sort(set, &num);

    printf("Unique arguments:\n");

    for (i = 0; i < num; i++) {
        s = items[i];
        printf("%s\n", s->bytes);
        free(s);
    }

    hatrack_set_delete(set);

    return;
}

int
main(int argc, char *argv[])
{
    dict_example(argc, argv);
    // Now let's do the same thing with sets.
    set_example(argc, argv);

    return 0;
}
