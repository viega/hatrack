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

#include "lowhat.h"

lowhat_t *
lowhat_new(lowhat_table_type_t type)
{
    lowhat_t *ret = (lowhat_t *)malloc(sizeof(lowhat_t));

    switch (type) {
    case LOWHAT_1:
        ret->vtable = lowhat1_vtable;
        break;
    case LOWHAT_2:
	ret->vtable = lowhat2_vtable;
	break;
    default:
        abort();
    }

    (*ret->vtable.init)(ret);

    return ret;
}
