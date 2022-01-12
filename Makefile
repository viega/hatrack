ENV      := /usr/bin/env
CC       := cc
OPT      := -Ofast -flto 
#LIBS     := -L/opt/homebrew/lib -lhoard
# Needed for linux.
#LIBS    := -latomic -pthread -lrt -lhoard
UNUSED   :=  -DHATRACK_MMMALLOC_CTRS
#EXTRAS   := -DHATRACK_COUNTERS 
#EXTRAS   :=  -DHATRACK_MMM_DEBUG -g
CFLAGS   :=  -std=c11 -Wall -Werror ${OPT} -I../include ${EXTRAS}
PROGNAME := test

all: ${PROGNAME} Makefile

# DO NOT MANUALLY EDIT OR REMOVE ANYTHING BELOW THIS LINE.
# begin autogenerated content
SRCFILES := testhat.c hatrack_common.c oldhat.c counters.c hihat-a.c woolhat.c swimcap.c duncecap.c debug.c xxhash.c refhat.c lohat.c test.c lohat-a.c mmm.c hihat.c witchhat.c tophat.c newshat.c ballcap.c
OBJFILES := ${SRCFILES:c=o}
testhat.o: testhat.c testhat.h hatvtable.h hatrack_common.h mmm.h config.h debug.h counters.h hatomic.h refhat.h duncecap.h swimcap.h newshat.h ballcap.h hihat.h oldhat.h lohat.h lohat_common.h lohat-a.h witchhat.h woolhat.h tophat.h 
hatrack_common.o: hatrack_common.c hatrack_common.h mmm.h config.h debug.h counters.h hatomic.h 
oldhat.o: oldhat.c oldhat.h hatrack_common.h mmm.h config.h debug.h counters.h hatomic.h 
counters.o: counters.c counters.h 
hihat-a.o: hihat-a.c hihat.h hatrack_common.h mmm.h config.h debug.h counters.h hatomic.h 
woolhat.o: woolhat.c woolhat.h hatrack_common.h mmm.h config.h debug.h counters.h hatomic.h 
swimcap.o: swimcap.c swimcap.h hatrack_common.h mmm.h config.h debug.h counters.h hatomic.h 
duncecap.o: duncecap.c duncecap.h hatrack_common.h mmm.h config.h debug.h counters.h hatomic.h 
debug.o: debug.c debug.h config.h 
xxhash.o: xxhash.c xxhash.h 
refhat.o: refhat.c refhat.h hatrack_common.h mmm.h config.h debug.h counters.h hatomic.h 
lohat.o: lohat.c lohat.h lohat_common.h hatrack_common.h mmm.h config.h debug.h counters.h hatomic.h 
test.o: test.c hash.h xxhash.h hatrack_common.h mmm.h config.h debug.h counters.h hatomic.h testhat.h hatvtable.h refhat.h duncecap.h swimcap.h newshat.h ballcap.h hihat.h oldhat.h lohat.h lohat_common.h lohat-a.h witchhat.h woolhat.h tophat.h 
lohat-a.o: lohat-a.c lohat-a.h lohat_common.h hatrack_common.h mmm.h config.h debug.h counters.h hatomic.h 
mmm.o: mmm.c mmm.h config.h debug.h counters.h hatomic.h hatrack_common.h 
hihat.o: hihat.c hihat.h hatrack_common.h mmm.h config.h debug.h counters.h hatomic.h 
witchhat.o: witchhat.c witchhat.h hatrack_common.h mmm.h config.h debug.h counters.h hatomic.h 
tophat.o: tophat.c tophat.h hatrack_common.h mmm.h config.h debug.h counters.h hatomic.h hatvtable.h newshat.h witchhat.h ballcap.h woolhat.h 
newshat.o: newshat.c newshat.h hatrack_common.h mmm.h config.h debug.h counters.h hatomic.h 
ballcap.o: ballcap.c ballcap.h hatrack_common.h mmm.h config.h debug.h counters.h hatomic.h 
# end autogenerated content

test: ${OBJFILES} Makefile
	${CC} ${OBJFILES} ${LIBS} -o ${PROGNAME}

%.o: %.c Makefile
	${CC} ${CFLAGS} -c $<
remake: clean ${PROGNAME}
clean:
	rm -f *.o *~ ../include/*~ ${PROGNAME}
format:
	${ENV} clang-format -i [a-wyz]*.{c,h}

