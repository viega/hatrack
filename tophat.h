/*
 * Copyright © 2021-2022 John Viega
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
 *  Name:           tophat.h
 *  Description:    Adaptive hash table that starts off fast, but
 *                  migrates to a multi-reader / multi-writer
 *                  implementation once the table is accessed by
 *                  multiple threads simultaneously.
 *
 *  Author:         John Viega, john@zork.org
 *
 */

#ifndef __TOPHAT_H__
#define __TOPHAT_H__

#include "hatrack_common.h"
#include "witchhat.h"
#include "woolhat.h"
#include "refhat-a.h"
#include "hatvtable.h"

// clang-format off

typedef struct {
    void              *htable;
    hatrack_vtable_t  *vtable;
} tophat_algo_info_t;
    
typedef struct {
    alignas(16)
    _Atomic tophat_algo_info_t implementation;
    uint32_t                   flags;
} tophat_t;

enum : uint64_t {
    TOPHAT_F_CONSISTENT_VIEWS = 0x01,
};

void            tophat_init_cst  (tophat_t *);
void            tophat_init_fast (tophat_t *);
void           *tophat_get       (tophat_t *, hatrack_hash_t *, bool *);
void           *tophat_put       (tophat_t *, hatrack_hash_t *, void *, bool *);
void           *tophat_replace   (tophat_t *, hatrack_hash_t *, void *, bool *);
bool            tophat_add       (tophat_t *, hatrack_hash_t *, void *);
void           *tophat_remove    (tophat_t *, hatrack_hash_t *, bool *);
void            tophat_delete    (tophat_t *);
uint64_t        tophat_len       (tophat_t *);
hatrack_view_t *tophat_view      (tophat_t *, uint64_t *, bool);


#endif
