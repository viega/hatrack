check_PROGRAMS = test
test_SOURCES = src/mmm.c src/debug.c src/xxhash.c src/counters.c src/hatrack_common.c src/refhat.c src/duncecap.c src/swimcap.c src/newshat.c src/ballcap.c src/hihat.c src/hihat-a.c src/oldhat.c src/lohat.c src/lohat-a.c src/witchhat.c src/woolhat.c src/tophat.c src/test.c src/testhat.c
# 64-bit systems will complain up the wazoo about the 128-bit CAS operations.
# Yes, they won't be lock free, but they will be sufficiently fast, thanks.
test_CFLAGS = -Wall -Wextra -Wno-atomic-alignment
lib_LIBRARIES = libhatrack.a
libhatrack_a_CFLAGS  = -Wall -Wextra -Wno-atomic-alignment
libhatrack_a_SOURCES = src/mmm.c src/debug.c src/xxhash.c src/counters.c src/hatrack_common.c src/refhat.c src/duncecap.c src/swimcap.c src/newshat.c src/ballcap.c src/hihat.c src/hihat-a.c src/oldhat.c src/lohat.c src/lohat-a.c src/witchhat.c src/woolhat.c src/tophat.c

remake: clean all
format:
	@PATH_TO_ENV@ clang-format -i src/*.c include/[a-wyz]*.h

