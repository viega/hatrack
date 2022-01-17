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
 *  Name:           set.h
 *  Description:    Higher level set interface based on woolhat.
 *
 *  Author:         John Viega, john@zork.org
 */

#ifndef __HATRACK_SET_H__
#define __HATRACK_SET_H__

#include <hatrack/woolhat.h>
#include <hatrack/dict.h>


typedef struct {
    woolhat_t              woolhat_instance;
    hatrack_hash_info_t    hash_info;
    uint32_t               item_type;
    void                  *free_handler;
} hatrack_set_t;

typedef void (*hatrack_set_free_handler_t)(hatrack_set_t *, void *);

// clang-format off
hatrack_set_t  *hatrack_set_new             (uint32_t);
void            hatrack_set_init            (hatrack_set_t *, uint32_t);
void            hatrack_set_cleanup         (hatrack_set_t *);
void            hatrack_set_delete          (hatrack_set_t *);
void            hatrack_set_set_hash_offset (hatrack_set_t *, int32_t);
void            hatrack_set_set_cache_offset(hatrack_set_t *, int32_t);
void            hatrack_set_set_custom_hash (hatrack_set_t *,
					     hatrack_hash_function_t);
void            hatrack_set_set_free_handler(hatrack_set_t *,
					     hatrack_set_free_handler_t);
bool            hatrack_set_contains        (hatrack_set_t *, void *);
bool            hatrack_set_put             (hatrack_set_t *, void *);
bool            hatrack_set_add             (hatrack_set_t *, void *);
bool            hatrack_set_remove          (hatrack_set_t *, void *);
void           *hatrack_set_items           (hatrack_set_t *, uint64_t *);
void           *hatrack_set_items_sort      (hatrack_set_t *, uint64_t *);
bool            hatrack_set_is_eq           (hatrack_set_t *, hatrack_set_t *);
bool            hatrack_set_is_superset     (hatrack_set_t *, hatrack_set_t *,
					     bool);
bool            hatrack_set_is_subset       (hatrack_set_t *, hatrack_set_t *,
					     bool);
bool            hatrack_set_is_disjoint     (hatrack_set_t *, hatrack_set_t *);
hatrack_set_t  *hatrack_set_difference      (hatrack_set_t *, hatrack_set_t *);
hatrack_set_t  *hatrack_set_union           (hatrack_set_t *, hatrack_set_t *);
hatrack_set_t  *hatrack_set_intersection    (hatrack_set_t *, hatrack_set_t *);
hatrack_set_t  *hatrack_set_disjunction     (hatrack_set_t *, hatrack_set_t *);



#endif
