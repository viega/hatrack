check_PROGRAMS = tests/test
noinst_PROGRAMS = examples/basic examples/set1 examples/hashable examples/qtest examples/qperf examples/ring examples/logringex examples/array

# 64-bit systems will complain up the wazoo about the 128-bit CAS operations.
# Yes, they won't be lock free, but they will be sufficiently fast, thanks.
libhatrack_a_CFLAGS  = -std=c2x -D_POSIX_SOURCE -D_GNU_SOURCE -Wall -Wno-cast-function-type -Wno-unused-variable -Wno-format -Wextra -Wno-atomic-alignment -Wno-unused-parameter -I./include/
libhatrack_a_SOURCES = src/support/mmm.c src/support/counters.c src/support/hatrack_common.c src/support/helpmanager.c src/hash/refhat.c src/hash/duncecap.c src/hash/swimcap.c src/hash/newshat.c src/hash/ballcap.c src/hash/hihat.c src/hash/hihat-a.c src/hash/oldhat.c src/hash/lohat.c src/hash/lohat-a.c src/hash/witchhat.c src/hash/woolhat.c src/hash/tophat.c src/hash/crown.c src/hash/tiara.c src/hash/dict.c src/hash/set.c src/hash/xxhash.c src/queue/queue.c src/queue/q64.c src/queue/hq.c src/queue/capq.c src/queue/llstack.c src/queue/stack.c src/queue/hatring.c src/queue/logring.c src/queue/debug.c src/array/flexarray.c src/array/vector.c quark/quark.c quark/quark-asm.s



lib_LIBRARIES = libhatrack.a

tests_test_SOURCES = ${libhatrack_a_SOURCES} tests/test.c tests/testhat.c tests/rand.c tests/config.c tests/functional.c tests/default.c tests/performance.c
tests_test_CFLAGS = -DHATRACK_COMPILE_ALL_ALGORITHMS -std=c11 -D_POSIX_SOURCE -D_GNU_SOURCE -Wall -Wno-cast-function-type -Wno-unused-variable -Wno-format -Wextra -Wno-atomic-alignment -Wno-unused-parameter -I./include/
tests_test_LDADD = $(LIBATOMIC)
#tests_test_DEPENDENCIES =

examples_basic_SOURCES = examples/basic.c
examples_basic_CFLAGS = -std=c11 -D_POSIX_SOURCE -D_GNU_SOURCE -Wall -Wno-cast-function-type -Wno-unused-variable -Wno-format -Wextra -Wno-unused-parameter -I./include
examples_basic_LDADD = ./libhatrack.a $(LIBATOMIC)
examples_basic_DEPENDENCIES = ./libhatrack.a

examples_set1_SOURCES = examples/set1.c
examples_set1_CFLAGS = -std=c11 -D_POSIX_SOURCE -D_GNU_SOURCE -Wall -Wno-cast-function-type -Wno-unused-variable -Wno-format -Wextra -I./include
examples_set1_LDADD = ./libhatrack.a $(LIBATOMIC)
examples_set1_DEPENDENCIES = ./libhatrack.a

examples_hashable_SOURCES = examples/hashable.c
examples_hashable_CFLAGS = -std=c11 -D_POSIX_SOURCE -D_GNU_SOURCE -Wall -Wno-cast-function-type -Wno-unused-variable -Wno-format -Wextra -Wno-unused-parameter -I./include
examples_hashable_LDADD = ./libhatrack.a $(LIBATOMIC)
examples_hashable_DEPENDENCIES = ./libhatrack.a

examples_qtest_SOURCES = examples/qtest.c
examples_qtest_CFLAGS = -std=c11 -D_POSIX_SOURCE -D_GNU_SOURCE -Wall -Wno-cast-function-type -Wno-unused-variable -Wno-format -Wextra -I./include
examples_qtest_LDADD = ./libhatrack.a $(LIBATOMIC)
examples_qtest_DEPENDENCIES = ./libhatrack.a

examples_qperf_SOURCES = examples/qperf.c
examples_qperf_CFLAGS = -std=c11 -D_POSIX_SOURCE -D_GNU_SOURCE -Wall -Wno-cast-function-type -Wno-unused-variable -Wno-format -Wextra -I./include
examples_qperf_LDADD = ./libhatrack.a $(LIBATOMIC)
examples_qperf_DEPENDENCIES = ./libhatrack.a

examples_ring_SOURCES = examples/ring.c
examples_ring_CFLAGS = -std=c11 -D_POSIX_SOURCE -D_GNU_SOURCE -Wall -Wno-cast-function-type -Wno-unused-variable -Wno-format -Wextra -I./include
examples_ring_LDADD = ./libhatrack.a $(LIBATOMIC)
examples_ring_DEPENDENCIES = ./libhatrack.a

examples_logringex_SOURCES = examples/logringex.c
examples_logringex_CFLAGS = -std=c11 -D_POSIX_SOURCE -D_GNU_SOURCE -Wall -Wno-cast-function-type -Wno-unused-variable -Wno-format -Wextra -I./include
examples_logringex_LDADD = ./libhatrack.a $(LIBATOMIC)
examples_logringex_DEPENDENCIES = ./libhatrack.a

examples_array_SOURCES = examples/array.c
examples_array_CFLAGS = -std=c11 -D_POSIX_SOURCE -D_GNU_SOURCE -Wall -Wno-cast-function-type -Wno-unused-variable -Wno-format -Wextra -I./include
examples_array_LDADD = ./libhatrack.a $(LIBATOMIC)
examples_array_DEPENDENCIES = ./libhatrack.a

include_HEADERS = include/hatrack.h
pkginclude_HEADERS = include/hatrack/xxhash.h include/hatrack/ballcap.h include/hatrack/config.h include/hatrack/counters.h include/hatrack/debug.h include/hatrack/gate.h include/hatrack/dict.h include/hatrack/set.h include/hatrack/duncecap.h include/hatrack/hash.h include/hatrack/hatomic.h include/hatrack/hatrack_common.h include/hatrack/hatrack_config.h include/hatrack/hatvtable.h include/hatrack/hihat.h include/hatrack/lohat-a.h include/hatrack/lohat.h include/hatrack/lohat_common.h include/hatrack/mmm.h include/hatrack/newshat.h include/hatrack/oldhat.h include/hatrack/refhat.h include/hatrack/swimcap.h include/hatrack/tophat.h include/hatrack/witchhat.h include/hatrack/woolhat.h include/hatrack/crown.h include/hatrack/tiara.h include/hatrack/queue.h include/hatrack/q64.h include/hatrack/hq.h include/hatrack/capq.h include/hatrack/flexarray.h include/hatrack/llstack.h include/hatrack/stack.h include/hatrack/hatring.h include/hatrack/logring.h include/hatrack/helpmanager.h include/hatrack/vector.h

test: check
remake: clean all
format:
	@PATH_TO_ENV@ clang-format -i examples/*.c test/*.{c, h} src/*.c include/[a-wyz]*.h
