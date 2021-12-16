/*
 * Copyright © 2021 John Viega
 *
 * See LICENSE.txt for licensing info.
 *
 *  Name:           lowhat0.h
 *  Description:    Linearizeable, Ordered, Wait-free HAsh Table (LOWHAT)
 *                  This version never orders, it just sorts when needed.
 *  Author:         John Viega, john@zork.org
 *
 */

#ifndef __LOWHAT0_H__
#define __LOWHAT0_H__

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
void           lowhat0_init(lowhat_t *);
void          *lowhat0_get(lowhat_t *, lowhat_hash_t *, bool *);
void          *lowhat0_put(lowhat_t *, lowhat_hash_t *, void *, bool, bool *);
void          *lowhat0_remove(lowhat_t *, lowhat_hash_t *, bool *);
void           lowhat0_delete(lowhat_t *);
uint64_t       lowhat0_len(lowhat_t *);
lowhat_view_t *lowhat0_view(lowhat_t *, uint64_t *);
// clang-format on

extern const lowhat_vtable_t lowhat0_vtable;

#endif
