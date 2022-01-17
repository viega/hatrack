#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.69])
AC_INIT([hatrack],[0.9],[john@zork.org])
AC_CONFIG_AUX_DIR([autotools])
AM_INIT_AUTOMAKE([subdir-objects foreign])
AM_SILENT_RULES([yes])
AC_CONFIG_COMMANDS([default],[],[])
AC_COPYRIGHT([Copyright 2021-2022, John Viega])


AC_LANG([C])
AC_PROG_CC(clang cc gcc)

AC_PROG_RANLIB

AC_CONFIG_SRCDIR([src/refhat.c])
AC_CONFIG_HEADERS([include/hatrack/config.h])
AC_CHECK_HEADERS([include/hatrack.h])

AC_CHECK_HEADER_STDBOOL
AC_C_INLINE
AC_TYPE_INT64_T
AC_TYPE_SIZE_T
AC_TYPE_UINT32_T
AC_TYPE_UINT64_T
AC_TYPE_UINT8_T

AC_CACHE_CHECK([for __int128_t],
              [ac_cv_i128],
	      [AC_COMPILE_IFELSE([
	       AC_LANG_PROGRAM([__int128_t x;])],
	           [ac_cv_i128=yes],
 	           [ac_cv_i128=no])])

if test $ac_cv_i128 = yes; then
  AC_DEFINE([HAVE___INT128_T], [1], [Defined when __int128_t exists])
fi

# Autconf doesn't seem to have an option to check for C11 unfortunately :/

# Checks for header files.
AC_CHECK_HEADERS([fcntl.h stdint.h unistd.h stdatomic.h pthread.h stdalign.h])


AC_CHECK_LIB([pthread], [pthread_create], [LDFLAGS="${LDFLAGS} -pthread"])
AC_CHECK_LIB([atomic], [__atomic_load_16], [LDFLAGS="${LDFLAGS} -latomic"])

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

AC_CACHE_CHECK([to see if atomic_fetch_add(ptr, 1) adds sizeof(ptr)],
               [ac_cv_afa_ptr],
	       [AC_RUN_IFELSE([
	       AC_LANG_PROGRAM([[#include <stdatomic.h>
	                        typedef struct s { int a; int b; int c; } foo_t;
	                       _Atomic(foo_t *) x = ATOMIC_VAR_INIT(NULL);]],
	       [[atomic_fetch_add(&x, 1);
	        if ((intptr_t)(atomic_load(&x)) == sizeof(foo_t)) { return 0; }
		return 1;]] )],
		[ac_cv_afa_ptr=yes],
		[ac_cv_afa_ptr=no])])

if test $ac_cv_afa_ptr = yes; then
  AC_DEFINE([SANE_FETCH_ADD_PTR_SEMANTICS], [1], [Defined when atomic_fetch_add(ptr, 1) adds sizeof(ptr)])
fi


# Checks for library functions.
AC_CHECK_FUNCS([clock_gettime memset strstr])

AC_PATH_PROG([PATH_TO_ENV], [env], [])

AC_CONFIG_FILES([Makefile])
AC_OUTPUT

