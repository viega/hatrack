/*
 * Copyright © 2021 John Viega
 *
 * See LICENSE.txt for licensing info.
 *
 *  Name:           newshat.c
 *  Description:    Now Everyone Writes Simultaneously (HAsh Table)
 *  Author:         John Viega, john@zork.org
 */

#ifndef __NEWSHAT_H__
#define __NEWSHAT_H__

#include "hatrack_common.h"
#include "pthread.h"

typedef struct {
    alignas(32) hatrack_hash_t hv;
    void           *item;
    bool            deleted;
    bool            migrated;
    pthread_mutex_t write_mutex;
} newshat_bucket_t;

typedef struct {
    alignas(32) uint64_t last_slot;
    uint64_t         threshold;
    uint64_t         used_count;
    newshat_bucket_t buckets[];
} newshat_store_t;

typedef struct {
    alignas(32) uint64_t item_count;
    newshat_store_t *store;
    pthread_mutex_t  migrate_mutex;
} newshat_t;

void     newshat_init(newshat_t *);
void    *newshat_get(newshat_t *, hatrack_hash_t *, bool *);
void    *newshat_base_put(newshat_t *, hatrack_hash_t *, void *, bool, bool *);
void    *newshat_put(newshat_t *, hatrack_hash_t *, void *, bool *);
bool     newshat_put_if_empty(newshat_t *, hatrack_hash_t *, void *);
void    *newshat_remove(newshat_t *, hatrack_hash_t *, bool *);
void     newshat_delete(newshat_t *);
uint64_t newshat_len(newshat_t *);
hatrack_view_t *newshat_view(newshat_t *, uint64_t *);

#endif
