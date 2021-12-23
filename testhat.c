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

#include "testhat.h"

#include <string.h>
#include <stdlib.h>
#include <stdio.h>

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
    .init    = (hatrack_init_func)refhat0_init,
    .get     = (hatrack_get_func)refhat0_get,
    .put     = (hatrack_put_func)refhat0_put,
    .putcond = (hatrack_putcond_func)refhat0_put_if_empty,    
    .remove  = (hatrack_remove_func)refhat0_remove,
    .delete  = (hatrack_delete_func)refhat0_delete,
    .len     = (hatrack_len_func)refhat0_len,
    .view    = (hatrack_view_func)refhat0_view
};

hatrack_vtable_t swimcap_vtable = {
    .init    = (hatrack_init_func)swimcap_init,
    .get     = (hatrack_get_func)swimcap_get,
    .put     = (hatrack_put_func)swimcap_put,
    .putcond = (hatrack_putcond_func)swimcap_put_if_empty,    
    .remove  = (hatrack_remove_func)swimcap_remove,
    .delete  = (hatrack_delete_func)swimcap_delete,
    .len     = (hatrack_len_func)swimcap_len,
    .view    = (hatrack_view_func)swimcap_view
};

hatrack_vtable_t swimcap2_vtable = {
    .init    = (hatrack_init_func)swimcap2_init,
    .get     = (hatrack_get_func)swimcap2_get,
    .put     = (hatrack_put_func)swimcap2_put,
    .putcond = (hatrack_putcond_func)swimcap2_put_if_empty,    
    .remove  = (hatrack_remove_func)swimcap2_remove,
    .delete  = (hatrack_delete_func)swimcap2_delete,
    .len     = (hatrack_len_func)swimcap2_len,
    .view    = (hatrack_view_func)swimcap2_view
};

hatrack_vtable_t newshat_vtable = {
    .init    = (hatrack_init_func)newshat_init,
    .get     = (hatrack_get_func)newshat_get,
    .put     = (hatrack_put_func)newshat_put,
    .putcond = (hatrack_putcond_func)newshat_put_if_empty,    
    .remove  = (hatrack_remove_func)newshat_remove,
    .delete  = (hatrack_delete_func)newshat_delete,
    .len     = (hatrack_len_func)newshat_len,
    .view    = (hatrack_view_func)newshat_view
};

hatrack_vtable_t hihat1_vtable = {
    .init    = (hatrack_init_func)hihat1_init,
    .get     = (hatrack_get_func)hihat1_get,
    .put     = (hatrack_put_func)hihat1_put,
    .putcond = (hatrack_putcond_func)hihat1_put_if_empty,
    .remove  = (hatrack_remove_func)hihat1_remove,
    .delete  = (hatrack_delete_func)hihat1_delete,
    .len     = (hatrack_len_func)hihat1_len,
    .view    = (hatrack_view_func)hihat1_view
};


hatrack_vtable_t hihat64_vtable = {
    .init    = (hatrack_init_func)hihat64_init,
    .get     = (hatrack_get_func)hihat64_get,
    .put     = (hatrack_put_func)hihat64_put,
    .putcond = (hatrack_putcond_func)hihat64_put_if_empty,    
    .remove  = (hatrack_remove_func)hihat64_remove,
    .delete  = (hatrack_delete_func)hihat64_delete,
    .len     = (hatrack_len_func)hihat64_len,
    .view    = (hatrack_view_func)hihat64_view
};

hatrack_vtable_t lohat0_vtable = {
    .init    = (hatrack_init_func)lohat0_init,
    .get     = (hatrack_get_func)lohat0_get,
    .put     = (hatrack_put_func)lohat0_put,
    .putcond = (hatrack_putcond_func)lohat0_put_if_empty,
    .remove  = (hatrack_remove_func)lohat0_remove,
    .delete  = (hatrack_delete_func)lohat0_delete,
    .len     = (hatrack_len_func)lohat0_len,
    .view    = (hatrack_view_func)lohat0_view
};

hatrack_vtable_t lohat1_vtable = {
    .init    = (hatrack_init_func)lohat1_init,
    .get     = (hatrack_get_func)lohat1_get,
    .put     = (hatrack_put_func)lohat1_put,
    .putcond = (hatrack_putcond_func)lohat1_put_if_empty,    
    .remove  = (hatrack_remove_func)lohat1_remove,
    .delete  = (hatrack_delete_func)lohat1_delete,
    .len     = (hatrack_len_func)lohat1_len,
    .view    = (hatrack_view_func)lohat1_view
};

hatrack_vtable_t lohat2_vtable = {
    .init    = (hatrack_init_func)lohat2_init,
    .get     = (hatrack_get_func)lohat2_get,
    .put     = (hatrack_put_func)lohat2_put,
    .putcond = (hatrack_putcond_func)lohat2_put_if_empty,    
    .remove  = (hatrack_remove_func)lohat2_remove,
    .delete  = (hatrack_delete_func)lohat2_delete,
    .len     = (hatrack_len_func)lohat2_len,
    .view    = (hatrack_view_func)lohat2_view
};

// clang-format on

static void
testhat_init_default_algorithms()
{
    testhat_register_algorithm("refhat0", &refhat0_vtable, sizeof(refhat0_t));
    testhat_register_algorithm("swimcap", &swimcap_vtable, sizeof(swimcap_t));
    testhat_register_algorithm("swimcap2",
                               &swimcap2_vtable,
                               sizeof(swimcap2_t));
    testhat_register_algorithm("newshat", &newshat_vtable, sizeof(newshat_t));
    testhat_register_algorithm("hihat1", &hihat1_vtable, sizeof(hihat1_t));
    testhat_register_algorithm("hihat64", &hihat64_vtable, sizeof(hihat64_t));
    testhat_register_algorithm("lohat0", &lohat0_vtable, sizeof(lohat0_t));
    testhat_register_algorithm("lohat1", &lohat1_vtable, sizeof(lohat1_t));
    testhat_register_algorithm("lohat2", &lohat2_vtable, sizeof(lohat2_t));
}
