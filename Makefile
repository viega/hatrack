ENV      := /usr/bin/env
CC       := cc
#OPT      := -Ofast -flto
OPT      := -DLOWHAT_MMMALLOC_CTRS
CFLAGS   :=  -g -Wall -Werror ${OPT} -I../include
PROGNAME := test

all: ${PROGNAME} Makefile

# DO NOT MANUALLY EDIT OR REMOVE ANYTHING BELOW THIS LINE.
# SOURCE DEPENDENCIES ARE COMPUTED BY dep.py in scripts folder.
# begin autogenerated content
SRCFILES := lowhat.c lowhat1.c mmm.c test.c xxhash.c
OBJFILES := ${SRCFILES:c=o}
lowhat.o: lowhat.c lowhat.h lowhat1.h lowhat_common.h mmm.h
lowhat1.o: lowhat1.c lowhat1.h lowhat_common.h mmm.h
mmm.o: mmm.c mmm.h
test.o: test.c hash.h xxhash.h lowhat.h lowhat1.h lowhat_common.h mmm.h
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
