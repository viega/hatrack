/*
 * Copyright © 2021 John Viega
 *
 * See LICENSE.txt for licensing info.
 *
 *  Name:           lowhat2.h
 *  Description:    Linearizeable, Ordered, Wait-free HAsh Table (LOWHAT)
 *                  This version keeps two tables, for partial ordering.
 *  Author:         John Viega, john@zork.org
 *
 */

#ifndef __LOWHAT2_H__
#define __LOWHAT2_H__

#include "lowhat_common.h"

// This API requires that you deal with hashing the key external to
// the API.  You might want to cache hash values, use different
// functions for different data objects, etc.
//
// We do require 128-bit hash values, and require that the hash value
// alone can stand in for object identity. One might, for instance,
// choose a 3-universal keyed hash function, or if hash values need to
// be consistent across runs, something fast and practical like XXH3.

// clang-format off
void           lowhat2_init(lowhat_t *);
void          *lowhat2_get(lowhat_t *, lowhat_hash_t *, bool *);
void          *lowhat2_put(lowhat_t *, lowhat_hash_t *, void *, bool, bool *);
void          *lowhat2_remove(lowhat_t *, lowhat_hash_t *, bool *);
void           lowhat2_delete(lowhat_t *);
uint64_t       lowhat2_len(lowhat_t *);
lowhat_view_t *lowhat2_view(lowhat_t *, uint64_t *);
// clang-format on

extern const lowhat_vtable_t lowhat2_vtable;

#endif
