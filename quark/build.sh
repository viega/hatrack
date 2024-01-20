#!/bin/sh
ARCH=$(uname -m)
${CC:-musl-gcc} -c ${ARCH}.s
${CC:-musl-gcc} -c quark.c
ar rs ../libquark.a ${ARCH}.o quark.o
