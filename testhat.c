/*
 * Copyright © 2021 John Viega
 *
 * See LICENSE.txt for licensing info.
 *
 *  Name:           testhat.c
 *  Description:    A wrapper to provide a single interface to all
 *                  the implementations, for ease of testing.
 *
 *                  Note that this interface isn't particularly high
 *                  level:
 *
 *                  1) You need to do the hashing yourself, and pass in
 *                     the value.
 *
 *                  2) You just pass in a pointer to an "item" that's
 *                     expected to represent the key/item pair.
 *
 *                  3) You need to do your own memory management for
 *                     the key / item pairs, if appropriate.
 *
 *                  Most of the implementation is inlined in the header
 *                  file, since it merely dispatches to individual
 *                  implementations.
 *
 *  Author:         John Viega, john@zork.org
 *
 */

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include "testhat.h"

#ifndef HATRACK_MAX_HATS
#define HATRACK_MAX_HATS 1024
#endif

typedef struct {
    char             *name;
    hatrack_vtable_t *vtable;
    size_t            size;
} testhat_table_info_t;

static uint64_t             next_table_type_id = 0;
static testhat_table_info_t implementation_info[HATRACK_MAX_HATS];

static void testhat_init_default_algorithms() __attribute__((constructor));

// We assume this is all single-threaded.
int64_t
testhat_id_by_name(char *name)
{
    uint64_t i;

    for (i = 0; i < next_table_type_id; i++) {
        if (!strcmp(implementation_info[i].name, name)) {
            return i;
        }
    }

    return -1;
}

int64_t
testhat_register_algorithm(char *name, hatrack_vtable_t *vtable, size_t size)
{
    uint64_t ret;

    if (testhat_id_by_name(name) != -1) {
        fprintf(stderr, "Error: table entry '%s' added twice.\n", name);
        abort();
    }

    ret                             = next_table_type_id++;
    implementation_info[ret].name   = name;
    implementation_info[ret].vtable = vtable;
    implementation_info[ret].size   = size;

    return ret;
}

testhat_t *
testhat_new(char *name)
{
    testhat_t *ret;
    int64_t    i = testhat_id_by_name(name);

    if (i == -1) {
        fprintf(stderr, "Error: table type named '%s' not registered.\n", name);
        abort();
    }

    ret         = (testhat_t *)calloc(1, sizeof(testhat_t));
    ret->htable = (void *)calloc(1, implementation_info[i].size);

    memcpy(ret, implementation_info[i].vtable, sizeof(hatrack_vtable_t));

    (*ret->vtable.init)(ret->htable);

    return ret;
}

// clang-format off

hatrack_vtable_t refhat0_vtable = {
    .init   = (hatrack_init_func)refhat0_init,
    .get    = (hatrack_get_func)refhat0_get,
    .put    = (hatrack_put_func)refhat0_base_put,
    .remove = (hatrack_remove_func)refhat0_remove,
    .delete = (hatrack_delete_func)refhat0_delete,
    .len    = (hatrack_len_func)refhat0_len,
    .view   = (hatrack_view_func)refhat0_view
};

hatrack_vtable_t hihat1_vtable = {
    .init   = (hatrack_init_func)hihat1_init,
    .get    = (hatrack_get_func)hihat1_get,
    .put    = (hatrack_put_func)hihat1_put,
    .remove = (hatrack_remove_func)hihat1_remove,
    .delete = (hatrack_delete_func)hihat1_delete,
    .len    = (hatrack_len_func)hihat1_len,
    .view   = (hatrack_view_func)hihat1_view
};


hatrack_vtable_t hihat64_vtable = {
    .init   = (hatrack_init_func)hihat64_init,
    .get    = (hatrack_get_func)hihat64_get,
    .put    = (hatrack_put_func)hihat64_put,
    .remove = (hatrack_remove_func)hihat64_remove,
    .delete = (hatrack_delete_func)hihat64_delete,
    .len    = (hatrack_len_func)hihat64_len,
    .view   = (hatrack_view_func)hihat64_view
};

hatrack_vtable_t lowhat0_vtable = {
    .init   = (hatrack_init_func)lowhat0_init,
    .get    = (hatrack_get_func)lowhat0_get,
    .put    = (hatrack_put_func)lowhat0_put,
    .remove = (hatrack_remove_func)lowhat0_remove,
    .delete = (hatrack_delete_func)lowhat0_delete,
    .len    = (hatrack_len_func)lowhat0_len,
    .view   = (hatrack_view_func)lowhat0_view
};

hatrack_vtable_t lowhat1_vtable = {
    .init   = (hatrack_init_func)lowhat1_init,
    .get    = (hatrack_get_func)lowhat1_get,
    .put    = (hatrack_put_func)lowhat1_put,
    .remove = (hatrack_remove_func)lowhat1_remove,
    .delete = (hatrack_delete_func)lowhat1_delete,
    .len    = (hatrack_len_func)lowhat1_len,
    .view   = (hatrack_view_func)lowhat1_view
};

hatrack_vtable_t lowhat2_vtable = {
    .init   = (hatrack_init_func)lowhat2_init,
    .get    = (hatrack_get_func)lowhat2_get,
    .put    = (hatrack_put_func)lowhat2_put,
    .remove = (hatrack_remove_func)lowhat2_remove,
    .delete = (hatrack_delete_func)lowhat2_delete,
    .len    = (hatrack_len_func)lowhat2_len,
    .view   = (hatrack_view_func)lowhat2_view
};

hatrack_vtable_t swimcap_vtable = {
    .init   = (hatrack_init_func)swimcap_init,
    .get    = (hatrack_get_func)swimcap_get,
    .put    = (hatrack_put_func)swimcap_base_put,
    .remove = (hatrack_remove_func)swimcap_remove,
    .delete = (hatrack_delete_func)swimcap_delete,
    .len    = (hatrack_len_func)swimcap_len,
    .view   = (hatrack_view_func)swimcap_view
};

// clang-format on

static void
testhat_init_default_algorithms()
{
    testhat_register_algorithm("refhat0", &refhat0_vtable, sizeof(refhat0_t));
    testhat_register_algorithm("hihat1", &hihat1_vtable, sizeof(hihat1_t));
    testhat_register_algorithm("hihat64", &hihat64_vtable, sizeof(hihat64_t));
    testhat_register_algorithm("lowhat0", &lowhat0_vtable, sizeof(lowhat0_t));
    testhat_register_algorithm("lowhat1", &lowhat1_vtable, sizeof(lowhat1_t));
    testhat_register_algorithm("lowhat2", &lowhat2_vtable, sizeof(lowhat2_t));
    testhat_register_algorithm("swimcap", &swimcap_vtable, sizeof(swimcap_t));
    testhat_register_algorithm("swimcap2", &swimcap_vtable, sizeof(swimcap2_t));
}
