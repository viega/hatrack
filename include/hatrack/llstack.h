/*
 * Copyright © 2022 John Viega
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License atn
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *  Name:           llstack.c
 *
 *  Description:    A lock-free, linked-list based stack, primarily for 
 *                  reference.
 *
 *  Author:         John Viega, john@zork.org
 */

#ifndef __LLSTACK_H__
#define __LLSTACK_H__

#include <stdint.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <stdalign.h>

typedef struct llstack_node_t llstack_node_t;

struct llstack_node_t {
    llstack_node_t *next;
    void           *item;
};

typedef struct {
    _Atomic (llstack_node_t *) head;
} llstack_t;

// clang-format off
llstack_t *llstack_new    (void);
void       llstack_init   (llstack_t *);
void       llstack_cleanup(llstack_t *);
void       llstack_delete (llstack_t *);
void       llstack_push   (llstack_t *, void *);
void      *llstack_pop    (llstack_t *, bool *);

#endif
