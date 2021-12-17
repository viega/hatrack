ENV      := /usr/bin/env
CC       := cc
OPT      :=  
#OPT      := -Ofast -flto
UNUSED   :=  -DLOWHAT_ALWAYS_USE_QSORT -DLOWHAT_MMMALLOC_CTRS
EXTRAS   := -DLOWHAT_QSORT_THRESHOLD=256
CFLAGS   :=  -g -Wall -Werror ${OPT} -I../include ${EXTRAS}
PROGNAME := test

all: ${PROGNAME} Makefile

# DO NOT MANUALLY EDIT OR REMOVE ANYTHING BELOW THIS LINE.
# SOURCE DEPENDENCIES ARE COMPUTED BY dep.py
# begin autogenerated content
SRCFILES := lowhat.c lowhat1.c lowhat2.c lowhat0.c refhat0.c mmm.c test.c xxhash.c
OBJFILES := ${SRCFILES:c=o}
lowhat.o: lowhat.c lowhat.h lowhat1.h lowhat2.h lowhat0.h lowhat_common.h mmm.h
lowhat1.o: lowhat1.c lowhat1.h lowhat_common.h mmm.h
lowhat2.o: lowhat2.c lowhat2.h lowhat_common.h mmm.h
lowhat0.o: lowhat0.c lowhat0.h lowhat_common.h mmm.h
refhat0.o: refhat0.c refhat0.h lowhat_common.h mmm.h
mmm.o: mmm.c mmm.h
test.o: test.c hash.h xxhash.h lowhat.h lowhat1.h lowhat2.h lowhat0.h lowhat_common.h mmm.h
xxhash.o: xxhash.c xxhash.h
# end autogenerated content
test: ${OBJFILES} Makefile
	${CC} ${OBJFILES} -o ${PROGNAME}
%.o: %.c Makefile
	${CC} ${CFLAGS} -c $<
remake: clean ${PROGNAME}
clean:
	rm -f *.o *~ ../include/*~ ${PROGNAME}
dep:
	${ENV} python3 ../scripts/makedep.py
format:
	${ENV} clang-format -i [a-wyz]*.{c,h}
prep: format dep clean all
