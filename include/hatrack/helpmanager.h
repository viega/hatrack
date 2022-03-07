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
 *  Name:           helpmanager.h
 *  Description:    Support for data-structure specific announce arrays, for
 *                  supporting wait freedom.
 *
 *                  Our approach to this problem is to fully linearize
 *                  all operations subject to help.  The help manager
 *                  has a master help record that dictates the
 *                  operation we're currently working on. 
 *
 *                  If a thread comes in with work to be done, they
 *                  first work to complete the existing operation. 
 *
 *                  If they find that some thread has signaled for
 *                  help, they will scan the announce array helping
 *                  all requests, before installing theirs.
 *
 *                  If installation fails more than a fixed number of
 *                  times, they enqueue their own help request.
 *                  
 *
 *  Author:         John Viega, john@zork.org
 */

#ifndef __HELPMANAGER_H__
#define __HELPMANAGER_H__

#include <stdint.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <hatrack/hatrack_config.h>
#include <hatrack/mmm.h>

typedef struct {
    void   *data;
    int64_t jobid;
} help_cell_t;

typedef struct {
    uint64_t op;
    int64_t  jobid;
} help_op_t;

typedef struct {
    uint64_t            op;
    void               *input;
    void               *aux;
    _Atomic help_cell_t success;
    _Atomic help_cell_t retval;
} help_record_t;

typedef _Atomic help_record_t help_record_atomic_t;

typedef void (*helper_func)(void *, help_record_t *, uint64_t);

static help_record_t thread_records[HATRACK_THREADS_MAX];

typedef struct {
    void                *parent;
    helper_func         *vtable;
    capq_t               capq;
} help_manager_t;

static inline void *
hatrack_help_get_parent(help_manager_t *manager)
{
    return manager->parent;
}

void  hatrack_help_init    (help_manager_t *, void *, helper_func *, bool);
void *hatrack_perform_wf_op(help_manager_t *, uint64_t, void *, void *, bool *);
void  hatrack_complete_help(help_manager_t *, help_record_t *, int64_t, void *,
			    bool);

#endif
