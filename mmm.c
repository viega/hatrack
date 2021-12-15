#include "mmm.h"

// clang-format off
__thread pthread_once_t mmm_inited       = PTHREAD_ONCE_INIT;
_Atomic  uint64_t       mmm_epoch        = ATOMIC_VAR_INIT(MMM_EPOCH_FIRST);
_Atomic  uint64_t       mmm_nexttid      = ATOMIC_VAR_INIT(0);
__thread uint64_t       mmm_mytid        = -1; 
__thread uint64_t       mmm_retire_ctr   = 0;
__thread mmm_header_t  *mmm_retire_list  = NULL;
         uint64_t       mmm_reservations[MMM_THREADS_MAX] = { 0, };
//clang-format on

static void mmm_empty(void);

#ifndef MMM_ALLOW_TID_GIVEBACKS

void
mmm_register_thread(void)
{
    mmm_mytid = atomic_fetch_add(&mmm_nexttid, 1);
    if (mmm_mytid >= MMM_THREADS_MAX) {
	abort();
    }

    mmm_reservations[mmm_mytid] = MMM_EPOCH_UNRESERVED;
}

#else /* MMM_ALLOW_TID_GIVEBACKS */

_Atomic (epoch_free_tids_t *) mmm_free_tids;

void
mmm_register_thread(void)
{
    mmm_free_tids_t *head;

    if (mmm_mytid != (uint64_t)-1) {
	return;
    }
    mmm_mytid = atomic_fetch_add(&mmm_nexttid, 1);
    if (mmm_mytid >= MMM_THREADS_MAX) {
	head = atomic_load(mmm_free_tids);
	do {
	    if (!head) {
		abort();
	    }
	} while (!CAS(&mmm_free_tids, &head, head->next));
	mmm_mytid = head->tid;
	mmm_retire(head);
    }

    mmm_reservations[mmm_mytid] = MMM_EPOCH_UNRESERVED;    
}

void
mmm_tid_giveback(void)
{
    mmm_free_tids_t *new_head;
    mmm_free_tids_t *old_head;

    new_head       = mmm_alloc(sizeof(mmm_free_tids_t));
    new_head->tid  = mmm_mytid;
    old_head       = atomic_load(mmm_free_tids);

    do {
	new_head->next = old_head;
    } while (!CAS(&free_tids, &old_head, new_head));
    
}
#endif /* MMM_ALLOW_TID_GIVEBACKS */


void
mmm_retire(void *ptr)
{
    mmm_header_t *cell = mmm_get_header(ptr);

#if 1
    // Avoid multiple threads adding this to their retire list.
    // Since retire_epoch is meant to only be accessed by a single
    // thread at once, we really should make this atomic.
    if (cell->retire_epoch) {
	return;
    }
#endif

    cell->retire_epoch = atomic_load(&mmm_epoch);
    cell->next         = mmm_retire_list;
    mmm_retire_list    = cell;

    if (++mmm_retire_ctr & MMM_RETIRE_FREQ) {
	mmm_retire_ctr = 0;
	mmm_empty();
    }
}

// For now, our cleanup function spins until it is able to retire
// everything on its list. Soon, when we start worrying about thread
// kills, we will change this to add its contents to an "ophan" list.

void
mmm_clean_up_before_exit(void)
{
    mmm_end_op();
    
    while (mmm_retire_list) {
	mmm_empty();
    }
}

#ifdef LOWHAT_MMMALLOC_CTRS
_Atomic uint64_t mmm_alloc_ctr = ATOMIC_VAR_INIT(0);
_Atomic uint64_t mmm_free_ctr  = ATOMIC_VAR_INIT(0);
#endif

// The basic gist of this algorithm is that we're going to look at
// every reservation we can find, identifying the oldest reservation
// in the list.
//
// Then, we can then safely free anything in the list with an earlier
// retirement epoch than the reservation time. Since the items in the
// stack were pushed on in order of their retirement epoch, it
// suffices to find the first item that is lower than the target,
// and free everything else.

static void
mmm_empty(void)
{
    mmm_header_t *tmp;
    mmm_header_t *cell;
    uint64_t      lowest;
    uint64_t      reservation;
    uint64_t      lasttid;

    // We don't have to search the whole array, just the items assigned
    // to active threads. Even if a new thread comes along, it will
    // not be able to reserve something that's already been retired
    // by the time we call this.
    lasttid = atomic_load(&mmm_nexttid);
    
    if (lasttid > MMM_THREADS_MAX) {
	lasttid = MMM_THREADS_MAX;
    }

    // We start out w/ the "lowest" reservation we've seen as
    // MMM_EPOCH_MAX.  If this value never changes, then it
    // means no epochs were reserved, and we can safely
    // free every record in our stack.
    
    lowest = MMM_EPOCH_MAX;

    for (uint64_t i = 0; i < lasttid; i++) {
	reservation = mmm_reservations[i];
	if (reservation < lowest) {
	    lowest = reservation;
	}
    }
    
    // The list here is ordered by retire epoch, with most recent on
    // top.  Go down the list until the NEXT cell is the first item we
    // should delete.
    // 
    // Then, set the current cell's next pointer to NULL (since
    // it's the new end of the list), and then place the pointer at
    // the top of the list of cells to delete.
    //
    // Note that this function is only called if there's something
    // something on the retire list, so cell will never start out
    // empty.

    cell = mmm_retire_list;

    // Special-case this, in case we have to delete the head cell,
    // to make sure we reinitialize the linked list right.
    if (mmm_retire_list->retire_epoch < lowest) {
	mmm_retire_list = NULL;
    } else {
	while (true) {
	    // We got to the end of the list, and didn't
	    // find one we should bother deleting.
	    if (!cell->next) {
		return;
	    }
	    if (cell->next->retire_epoch < lowest) {
		tmp       = cell;
		cell      = cell->next;
		tmp->next = NULL;
		break;
	    }
	    cell = cell->next;
	}
    }

    // Now cell and everything below it can be freed.
    while (cell) {
	tmp  = cell;
	cell = cell->next;
#ifdef LOWHAT_MMMALLOC_CTRS
	atomic_fetch_add(&mmm_free_ctr, 1);
#endif
	free(tmp);
    }
}
