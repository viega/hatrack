#ifndef __DEBUG_BUFFER_H__
#define __DEBUG_BUFFER_H__

#include "debug.h"

#ifdef HATRACK_DEBUG

void debug_dump(uint64_t);
void debug_thread();
void debug_other_thread(int64_t);
void debug_grep(char *);
void debug_pgrep(uintptr_t);

#endif

#endif
