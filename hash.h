#include "xxhash.h"
#include "hatrack_common.h"
#include <string.h>

#ifndef __HATRACK_HASH_H__
#define __HATRACK_HASH_H__

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
