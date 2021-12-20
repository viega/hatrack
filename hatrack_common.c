#include "hatrack_common.h"

#ifndef HATRACK_DONT_SORT
int
hatrack_quicksort_cmp(const void *bucket1, const void *bucket2)
{
    hatrack_view_t *item1 = (hatrack_view_t *)bucket1;
    hatrack_view_t *item2 = (hatrack_view_t *)bucket2;

    return item1->sort_epoch - item2->sort_epoch;
}

#endif
