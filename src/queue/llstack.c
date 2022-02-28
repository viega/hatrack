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
 *  Description:    A lock-free, linked-list based stack, primarily for 
 *                  reference.
 *
 *                  This is basically the "classic" lock-free
 *                  construction, except that we do not need to have
 *                  an ABA field, due to our use of MMM.
 *
 *  Author:         John Viega, john@zork.org
 */

#include <hatrack.h>

llstack_t *
llstack_new(void)
{
    llstack_t *ret;
    
    ret = (llstack_t *)malloc(sizeof(llstack_t));

    llstack_init(ret);

    return ret;
}

void
llstack_init(llstack_t *self)
{
    atomic_store(&self->head, NULL);
}

/* You're better off emptying the stack manually to do memory management
 * on the contents.  But if you didn't, we'll still clean up the records
 * we allocated, at least!
 */
void
llstack_cleanup(llstack_t *self)
{
    bool found;

    do {
	llstack_pop(self, &found);
    } while(found);

    return;
}

void
llstack_delete(llstack_t *self)
{
    llstack_cleanup(self);
    free(self);

    return;
}

void
llstack_push(llstack_t *self, void *item)
{
    llstack_node_t *node;
    llstack_node_t *head;

    mmm_start_basic_op();
    
    node       = mmm_alloc_committed(sizeof(llstack_node_t));
    head       = atomic_read(&self->head);
    node->item = item;

    do {
	node->next = head;
    } while (!CAS(&self->head, &head, node));

    mmm_end_op();
    
    return;
}

void *
llstack_pop(llstack_t *self, bool *found)
{
    llstack_node_t *old_head;
    void           *ret;
    
    mmm_start_basic_op();

    old_head = atomic_read(&self->head);

    while (old_head && !CAS(&self->head, &old_head, old_head->next));

    if (!old_head) {
	if (found) {
	    *found = false;
	}

	mmm_end_op();
	return NULL;
    }

    if (found) {
	*found = true;
    }

    ret = old_head->item;

    mmm_retire(old_head);
    mmm_end_op();

    return ret;
}
