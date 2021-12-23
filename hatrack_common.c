/*
 * Copyright © 2021 John Viega
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
 *  Name:           hatrack_common.c
 *  Description:    Functionality shared across all our default hash tables.
 *                  Most of it consists of short inlined functions, which
 *                  live in hatrack_common.h
 *
 *  Author:         John Viega, john@zork.org
 */

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
