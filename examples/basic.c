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
 *  Name:           basic.c
 *
 *  Description:    This example creates and uses two dictionaries.
 *
 *                  The first is a dictionary where the values are the
 *                  command-line arguments, and the keys are the index
 *                  associated with that argument.
 *
 *                  The second is a dictionary containing all environment
 *                  variables passed.
 *
 *                  The dictionary containing environment variable
 *                  information dynamically allocates the keys and
 *                  values, and uses a "free handler" to ask our code
 *                  to deallocate whatever needs to be deallocated
 *                  when the items are being ejected (which happens
 *                  when we delete the table, before exit).
 *
 *  Author:         John Viega, john@zork.org
 */

#include <hatrack.h>
#include <string.h>
#include <stdio.h>
#include <stdbool.h>

static void
print_argv(hatrack_dict_t *argv, bool ordered)
{
    hatrack_dict_value_t *values;
    uint64_t              i, num;

    if (ordered) {
        fprintf(stderr, "argv (cmd line order): \n  ");

        values = hatrack_dict_values_sort(argv, &num);
    }
    else {
        fprintf(stderr, "argv (hash order): \n  ");
        values = hatrack_dict_values_nosort(argv, &num);
    }

    for (i = 0; i < num; i++) {
        fprintf(stderr, "%s ", (char *)values[i]);
    }
    fprintf(stderr, "\n");

    return;
}

static void
print_envp(hatrack_dict_t *argv, bool ordered)
{
    hatrack_dict_item_t *items;
    uint64_t             i, num;

    if (ordered) {
        fprintf(stderr, "env (actual order): \n");
        items = hatrack_dict_items_sort(argv, &num);
    }
    else {
        fprintf(stderr, "env (hash order): \n");
        items = hatrack_dict_items_nosort(argv, &num);
    }

    for (i = 0; i < num; i++) {
        fprintf(stderr,
                "  %s: %s\n",
                (char *)items[i].key,
                (char *)items[i].value);
    }
    fprintf(stderr, "\n");

    return;
}

static void
envp_free_handler(hatrack_dict_t *unused, hatrack_dict_item_t *item)
{
    fprintf(stderr,
            "Freeing: %s: %s\n",
            (char *)item->key,
            (char *)item->value);

    free(item->key);
    free(item->value);

    return;
}

int
main(int argc, char *argv[], char *envp[])
{
    hatrack_dict_t *argv_dict;
    hatrack_dict_t *envp_dict;
    uint64_t        i;
    char           *env_eq; // Pointer to the equals mark.
    char           *env_key;
    char           *env_val;
    char           *p;

    argv_dict = hatrack_dict_new(HATRACK_DICT_KEY_TYPE_INT);
    envp_dict = hatrack_dict_new(HATRACK_DICT_KEY_TYPE_CSTR);

    hatrack_dict_set_free_handler(envp_dict,
                                  (hatrack_mem_hook_t)envp_free_handler);

    for (i = 0; i < (uint64_t)argc; i++) {
        hatrack_dict_put(argv_dict, (void *)i, argv[i]);
    }

    i = 0;

    /* Environment variables are of the form KEY=VALUE.  When we're
     * hashing, the hash function is going to run on the key, but will
     * be looking for a null terminator.
     *
     * We could modify the string in place, replacing the = with a null,
     * or we could just stdrup the key (and the value while we're at it)
     * to show how to use the hatrack free handler.
     */
    while (envp[i]) {
        p       = envp[i];
        env_eq  = strchr(p, '=');
        env_key = strndup(p, env_eq - p);
        env_val = strdup(env_eq + 1);

        hatrack_dict_put(envp_dict, env_key, env_val);

        i++;
    }

    print_envp(envp_dict, false);
    print_envp(envp_dict, true);

    fprintf(stderr, "\n");

    print_argv(argv_dict, false);
    print_argv(argv_dict, true);

    hatrack_dict_delete(argv_dict);
    hatrack_dict_delete(envp_dict);

    return 0;
}
