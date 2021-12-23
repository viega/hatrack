/*
 * Copyright © 2021 John Viega
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
 *  Name:           hash.h
 *  Description:    Hash functions for common data tyes, using the
 *                  third-party XXH3-128 hash function.
 *
 *                  Note that these hash functions are not used by the
 *                  core algorithms. Instead, they are used in the
 *                  wrapper in testhat.c, which we use for our test
 *                  harness that dispatches to the algorithm.
 *
 *  Author:         John Viega, john@zork.org
 *
 */

#ifndef __HATRACK_HASH_H__
#define __HATRACK_HASH_H__

#include "xxhash.h"
#include "hatrack_common.h"

#include <string.h>

typedef union {
    hatrack_hash_t lhv;
    XXH128_hash_t  xhv;
} hash_internal_conversion_t;

static inline hatrack_hash_t
hash_cstr(char *key)
{
    hash_internal_conversion_t u;

    u.xhv = XXH3_128bits(key, strlen(key));

    return u.lhv;
}

static inline hatrack_hash_t
hash_int(uint64_t key)
{
    hash_internal_conversion_t u;

    u.xhv = XXH3_128bits(&key, sizeof(uint64_t));

    return u.lhv;
}

static inline hatrack_hash_t
hash_double(double key)
{
    hash_internal_conversion_t u;

    u.xhv = XXH3_128bits(&key, sizeof(double));

    return u.lhv;
}

static inline hatrack_hash_t
hash_pointer(void *key)
{
    hash_internal_conversion_t u;

    u.xhv = XXH3_128bits(&key, sizeof(void *));

    return u.lhv;
}

#endif
