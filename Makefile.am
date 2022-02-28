check_PROGRAMS = tests/test
noinst_PROGRAMS = examples/basic examples/set1 examples/hashable examples/oldqx examples/qtest examples/ring examples/logringex

# 64-bit systems will complain up the wazoo about the 128-bit CAS operations.
# Yes, they won't be lock free, but they will be sufficiently fast, thanks.
libhatrack_a_CFLAGS  = -Wall -Wextra -Wno-atomic-alignment -Wno-unused-parameter  -I./include/
libhatrack_a_SOURCES = src/support/mmm.c src/support/counters.c src/support/hatrack_common.c src/hash/refhat.c src/hash/duncecap.c src/hash/swimcap.c src/hash/newshat.c src/hash/ballcap.c src/hash/hihat.c src/hash/hihat-a.c src/hash/oldhat.c src/hash/lohat.c src/hash/lohat-a.c src/hash/witchhat.c src/hash/woolhat.c src/hash/tophat.c src/hash/crown.c src/hash/tiara.c src/hash/dict.c src/hash/set.c src/hash/xxhash.c src/queue/queue.c src/queue/hq.c src/queue/llstack.c src/queue/stack.c src/queue/hatring.c src/queue/logring.c src/queue/debug.c src/array/flexarray.c

lib_LIBRARIES = libhatrack.a

tests_test_SOURCES = ${libhatrack_a_SOURCES} tests/test.c tests/testhat.c tests/rand.c tests/config.c tests/functional.c tests/default.c tests/performance.c
tests_test_CFLAGS = -DHATRACK_COMPILE_ALL_ALGORITHMS -Wall -Wextra -Wno-atomic-alignment -Wno-unused-parameter -I./include/

examples_basic_SOURCES = examples/basic.c
examples_basic_CFLAGS = -Wall -Wextra -Wno-unused-parameter -I./include
examples_basic_LDADD = ./libhatrack.a

examples_set1_SOURCES = examples/set1.c
examples_set1_CFLAGS = -Wall -Wextra -I./include
examples_set1_LDADD = ./libhatrack.a

examples_hashable_SOURCES = examples/hashable.c
examples_hashable_CFLAGS = -Wall -Wextra -Wno-unused-parameter -I./include
examples_hashable_LDADD = ./libhatrack.a

examples_oldqx_SOURCES = examples/old_queue_ex.c
examples_oldqx_CFLAGS = -Wall -Wextra -I./include
examples_oldqx_LDADD = ./libhatrack.a

examples_qtest_SOURCES = examples/test_queues.c
examples_qtest_CFLAGS = -Wall -Wextra -I./include
examples_qtest_LDADD = ./libhatrack.a

examples_ring_SOURCES = examples/ring.c
examples_ring_CFLAGS = -Wall -Wextra -I./include
examples_ring_LDADD = ./libhatrack.a

examples_logringex_SOURCES = examples/logringex.c
examples_logringex_CFLAGS = -Wall -Wextra -I./include
examples_logringex_LDADD = ./libhatrack.a

include_HEADERS = include/hatrack.h 
pkginclude_HEADERS = include/hatrack/xxhash.h include/hatrack/ballcap.h include/hatrack/config.h include/hatrack/counters.h include/hatrack/debug.h include/hatrack/dict.h include/hatrack/set.h include/hatrack/duncecap.h include/hatrack/hash.h include/hatrack/hatomic.h include/hatrack/hatrack_common.h include/hatrack/hatrack_config.h include/hatrack/hatvtable.h include/hatrack/hihat.h include/hatrack/lohat-a.h include/hatrack/lohat.h include/hatrack/lohat_common.h include/hatrack/mmm.h include/hatrack/newshat.h include/hatrack/oldhat.h include/hatrack/refhat.h include/hatrack/swimcap.h include/hatrack/tophat.h include/hatrack/witchhat.h include/hatrack/woolhat.h include/hatrack/crown.h include/hatrack/tiara.h include/hatrack/queue.h include/hatrack/hq.h include/hatrack/flexarray.h include/hatrack/llstack.h include/hatrack/stack.h include/hatrack/hatring.h include/hatrack/logring.h

test: check
remake: clean all
format:
	@PATH_TO_ENV@ clang-format -i examples/*.c test/*.{c, h} src/*.c include/[a-wyz]*.h

