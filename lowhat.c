/*
 * Copyright © 2021 John Viega
 *
 * See LICENSE.txt for licensing info.
 *
 *  Name:           lowhat.c
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
#include "lowhat.h"

void *
lowhat_new(lowhat_table_type_t type)
{
    lowhat_vtable_t *ret;

    switch (type) {
    case LOWHAT_0:
        ret = malloc(sizeof(lowhat0_t));
        memcpy(ret, &lowhat0_vtable, sizeof(lowhat_vtable_t));
        break;
    case LOWHAT_1:
        ret = malloc(sizeof(lowhat1_t));
        memcpy(ret, &lowhat1_vtable, sizeof(lowhat_vtable_t));
        break;
    case LOWHAT_2:
        ret = malloc(sizeof(lowhat2_t));
        memcpy(ret, &lowhat2_vtable, sizeof(lowhat_vtable_t));
        break;
    case HIHAT_1:
        ret = malloc(sizeof(hihat1_t));
        memcpy(ret, &hihat1_vtable, sizeof(lowhat_vtable_t));
        break;
    case REFHAT_0:
        ret = malloc(sizeof(refhat0_t));
        memcpy(ret, &refhat0_vtable, sizeof(lowhat_vtable_t));
        break;
    default:
        abort();
    }

    (*ret->init)(ret);

    return (void *)ret;
}
