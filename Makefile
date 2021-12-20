ENV      := /usr/bin/env
CC       := cc
#OPT      :=  
OPT      := -Ofast -flto
UNUSED   :=  -DHATRACK_ALWAYS_USE_QSORT -DHATRACK_MIN_SIZE_LOG=14 -DHATRACK_MMMALLOC_CTRS -DHATRACK_COUNTERS 
EXTRAS   := -DHATRACK_QSORT_THRESHOLD=256 
CFLAGS   :=  -g -Wall -Werror ${OPT} -I../include ${EXTRAS}
PROGNAME := test

all: ${PROGNAME} Makefile

SRCFILES := lowhat1.c lowhat2.c lowhat0.c hihat1.c swimcap.c refhat0.c hatrack_common.c mmm.c counters.c testhat.c xxhash.c
OBJFILES := ${SRCFILES:c=o}
lowhat1.o: lowhat1.c lowhat1.h hatrack_common.h hash.h mmm.h counters.h
lowhat2.o: lowhat2.c lowhat2.h hatrack_common.h hash.h mmm.h counters.h
lowhat0.o: lowhat0.c lowhat0.h hatrack_common.h hash.h mmm.h counters.h
hihat1.o: hihat1.c hihat1.h hatrack_common.h hash.h mmm.h counters.h
swimcap.o: swimcap.c swimcap.h hatrack_common.h hash.h mmm.h counters.h
refhat0.o: refhat0.c refhat0.h hatrack_common.h hash.h mmm.h counters.h
hatrack_common.o: hatrack_common.c hatrack_common.h hash.h mmm.h counters.h
mmm.o: mmm.c hash.h mmm.h counters.h
counters.o: counters.c counters.h
testhat.o: testhat.c testhat.h lowhat1.h lowhat2.h lowhat0.h refhat0.h swimcap.h hatrack_common.h hash.h mmm.h counters.h
test.o: test.c hash.h xxhash.h testhat.h lowhat1.h lowhat2.h lowhat0.h refhat0.h swimcap.h hatrack_common.h hash.h mmm.h counters.h
xxhash.o: xxhash.c xxhash.h

test: ${OBJFILES} test.o Makefile
	${CC} ${OBJFILES} test.o -o ${PROGNAME}

%.o: %.c Makefile
	${CC} ${CFLAGS} -c $<
remake: clean ${PROGNAME}
clean:
	rm -f *.o *~ ../include/*~ ${PROGNAME}
format:
	${ENV} clang-format -i [a-wyz]*.{c,h}

