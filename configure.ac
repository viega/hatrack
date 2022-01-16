#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.69])
AC_INIT([hatrack],[0.9],[john@zork.org])
AC_CONFIG_AUX_DIR([autotools])
AM_INIT_AUTOMAKE([subdir-objects foreign])
AM_SILENT_RULES([yes])
AC_CONFIG_COMMANDS([default],[],[])
AC_COPYRIGHT([Copyright 2021-2022, John Viega])
AC_CONFIG_SRCDIR([src/refhat.c])
AC_CONFIG_HEADERS([include/hatrack/config.h])
AC_CHECK_HEADERS([include/hatrack.h])

AC_PROG_RANLIB

AC_PROG_CC([clang, cc])

AC_CHECK_HEADER_STDBOOL
AC_C_INLINE
AC_TYPE_INT64_T
AC_TYPE_SIZE_T
AC_TYPE_UINT32_T
AC_TYPE_UINT64_T
AC_TYPE_UINT8_T

# Autconf doesn't seem to have an option to check for C11 unfortunately :/

# Checks for header files.
AC_CHECK_HEADERS([fcntl.h stdint.h unistd.h stdatomic.h pthread.h stdalign.h])

# Checks for C11's typed enums
# AC_MSG_CHECKING([C11 typed enums])
AC_CACHE_CHECK([for C11 typed enums],
               [ac_cv_c11enum],
	       [AC_COMPILE_IFELSE([
	       AC_LANG_PROGRAM([#include <stdint.h>
			        enum : uint64_t { FIRST_ITEM };])],
 	 		       [ac_cv_c11enum=yes], 	       			                               [ac_cv_c11enum=no])])

if test $ac_cv_c11enum = yes; then
  AC_DEFINE([HAVE_C11_ENUMS], [1], [Defined when C11 enums are present])
fi

AC_CHECK_LIB([pthread], [pthread_create], [LDFLAGS="${LDFLAGS} -pthread"])
AC_CHECK_LIB([atomic], [__atomic_load_16], [LDFLAGS="${LDFLAGS} -latomic"])


# Checks for library functions.
AC_CHECK_FUNCS([clock_gettime memset strstr])

AC_PATH_PROG([PATH_TO_ENV], [env], [])

AC_CONFIG_FILES([Makefile])
AC_OUTPUT

