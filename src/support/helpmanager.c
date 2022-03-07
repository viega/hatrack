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
 *  Name:           helpmanager.c
 *  Description:    Support for data-structure specific linearization 
 *                  through a wait-free CAPQ (compare-and-pop queue),
 *                  
 *
 *  Author:         John Viega, john@zork.org
 *
 */

#include <hatrack.h>
#include <strings.h>

void
hatrack_help_init(help_manager_t *manager,
		  void           *parent, 
		  helper_func    *vtable,
		  bool            zero)
{
    if (zero) {
	bzero(manager, sizeof(help_manager_t));
    }

    manager->parent = parent;
    manager->vtable = vtable;

    capq_init(&manager->capq);

    return;
}

static const help_cell_t empty_cell = {
    .data = NULL,
    .jobid = -1
};

void *
hatrack_perform_wf_op(help_manager_t *manager,
		      uint64_t        op,
		      void           *data,
		      void           *aux,
		      bool           *foundret)
{
    help_record_t *my_record;
    help_record_t *other_record;
    capq_top_t     qtop;
    int64_t        my_jobid;
    int64_t        other_jobid;
    bool           found;
    helper_func    f;
    help_cell_t    retcell;
    help_cell_t    foundcell;    

    my_record                = &thread_records[mmm_mytid];
    my_record->op            = op;
    my_record->input         = data;
    my_record->aux           = aux;
    my_record->success       = empty_cell;
    my_record->retval        = empty_cell;

    my_jobid = capq_enqueue(&manager->capq, my_record);

    do {
	qtop = capq_top(&manager->capq, &found);
	if (!found) {
	    break;
	}
	other_jobid  = qtop.state;
	other_record = qtop.item;
	f            = manager->vtable[other_record->op];
	
	(*f)(manager, other_record, other_jobid);
    } while (other_jobid < my_jobid);

    retcell = atomic_load(&my_record->retval);

    if (foundret) {
	foundcell = atomic_load(&my_record->success);
	*foundret = (bool)foundcell.data;
    }

    return retcell.data;
}

void
hatrack_complete_help(help_manager_t *manager,
		      help_record_t  *record,
		      int64_t         jobid,
		      void           *result,
		      bool            success)
{
    help_cell_t candidate;
    help_cell_t expected;

    expected = atomic_load(&record->retval);

    if (expected.jobid < jobid) {
	candidate.data  = result;
	candidate.jobid = jobid;

	CAS(&record->retval, &expected, candidate);
    }

    expected = atomic_load(&record->success);
    
    if (expected.jobid < jobid) {
	candidate.data  = (void *)success;
	candidate.jobid = jobid;

	CAS(&record->success, &expected, candidate);
    }

    capq_cap(&manager->capq, jobid);

    return;
}
