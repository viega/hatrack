#ifndef __HAT_VTABLE_H__
#define __HAT_VTABLE_H__

#include "hatrack_common.h"

/* For testing and for our tophat implementation, we keep vtables of
 * the operations to make it easier to switch between different
 * algorithms for testing. These types are aliases for the methods
 * that we expect to see.
 *
 * We use void * in the first parameter to all of these methods to
 * stand in for an arbitrary pointer to a hash table.
 */

// clang-format off
typedef void            (*hatrack_init_func)   (void *);
typedef void *          (*hatrack_get_func)    (void *, hatrack_hash_t *,
						bool *);
typedef void *          (*hatrack_put_func)    (void *, hatrack_hash_t *,
						void *, bool *);
typedef void *          (*hatrack_replace_func)(void *, hatrack_hash_t *,
						void *, bool *);
typedef bool            (*hatrack_add_func)    (void *, hatrack_hash_t *,
						void *);
typedef void *          (*hatrack_remove_func) (void *, hatrack_hash_t *,
						bool *);
typedef void            (*hatrack_delete_func) (void *);
typedef uint64_t        (*hatrack_len_func)    (void *);
typedef hatrack_view_t *(*hatrack_view_func)   (void *, uint64_t *, bool);

typedef struct {
    hatrack_init_func    init;
    hatrack_get_func     get;
    hatrack_put_func     put;
    hatrack_replace_func replace;
    hatrack_add_func     add;
    hatrack_remove_func  remove;
    hatrack_delete_func  delete;
    hatrack_len_func     len;
    hatrack_view_func    view;
} hatrack_vtable_t;

#endif
