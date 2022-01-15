bin_PROGRAMS = test
test_SOURCES = src/mmm.c src/debug.c src/xxhash.c src/counters.c src/hatrack_common.c src/refhat.c src/duncecap.c src/swimcap.c src/newshat.c src/ballcap.c src/hihat.c src/hihat-a.c src/oldhat.c src/lohat.c src/lohat-a.c src/witchhat.c src/woolhat.c src/tophat.c src/test.c src/testhat.c
test_CFLAGS = -Wall -Wextra
test_LDFLAGS = -latomic -pthread
lib_LIBRARIES = libhatrack.a
libhatrack_a_SOURCES = src/mmm.c src/debug.c src/xxhash.c src/counters.c src/hatrack_common.c src/refhat.c src/duncecap.c src/swimcap.c src/newshat.c src/ballcap.c src/hihat.c src/hihat-a.c src/oldhat.c src/lohat.c src/lohat-a.c src/witchhat.c src/woolhat.c src/tophat.c

remake: clean all
format:
	/bin/env clang-format -i [a-wyz]*.{c,h}
