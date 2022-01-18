check_PROGRAMS = tests/test
noinst_PROGRAMS = examples/basic examples/set1 examples/hashable

# 64-bit systems will complain up the wazoo about the 128-bit CAS operations.
# Yes, they won't be lock free, but they will be sufficiently fast, thanks.
libhatrack_a_CFLAGS  = -Wall -Wextra -Wno-atomic-alignment -I./include/
libhatrack_a_SOURCES = src/mmm.c src/debug.c src/xxhash.c src/counters.c src/hatrack_common.c src/refhat.c src/duncecap.c src/swimcap.c src/newshat.c src/ballcap.c src/hihat.c src/hihat-a.c src/oldhat.c src/lohat.c src/lohat-a.c src/witchhat.c src/woolhat.c src/tophat.c src/dict.c src/set.c

lib_LIBRARIES = libhatrack.a

tests_test_SOURCES = tests/test.c tests/testhat.c
tests_test_CFLAGS = -Wall -Wextra -Wno-atomic-alignment -I./include/
tests_test_LDADD = ./libhatrack.a

examples_basic_SOURCES = examples/basic.c
examples_basic_CFLAGS = -Wall -Wextra -I./include
examples_basic_LDADD = ./libhatrack.a

examples_set1_SOURCES = examples/set1.c
examples_set1_CFLAGS = -Wall -Wextra -I./include
examples_set1_LDADD = ./libhatrack.a

examples_hashable_SOURCES = examples/hashable.c
examples_hashable_CFLAGS = -Wall -Wextra -I./include
examples_hashable_LDADD = ./libhatrack.a

include_HEADERS = include/hatrack.h 
pkginclude_HEADERS = include/hatrack/xxhash.h include/hatrack/ballcap.h include/hatrack/config.h include/hatrack/counters.h include/hatrack/debug.h include/hatrack/dict.h include/hatrack/set.h include/hatrack/duncecap.h include/hatrack/hash.h include/hatrack/hatomic.h include/hatrack/hatrack_common.h include/hatrack/hatrack_config.h include/hatrack/hatvtable.h include/hatrack/hihat.h include/hatrack/lohat-a.h include/hatrack/lohat.h include/hatrack/lohat_common.h include/hatrack/mmm.h include/hatrack/newshat.h include/hatrack/oldhat.h include/hatrack/refhat.h include/hatrack/swimcap.h include/hatrack/tophat.h include/hatrack/witchhat.h include/hatrack/woolhat.h

remake: clean all
format:
	@PATH_TO_ENV@ clang-format -i examples/*.c test/*.{c, h} src/*.c include/[a-wyz]*.h

