#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.71])
AC_INIT([hatrack],[0.9],[john@zork.org])
AC_CONFIG_AUX_DIR([autotools])
AM_INIT_AUTOMAKE([subdir-objects foreign])
AC_CONFIG_COMMANDS([default],[],[])
AC_PROG_RANLIB
AC_COPYRIGHT([Copyright 2021-2022, John Viega])
AC_CONFIG_SRCDIR([src/refhat.c])
AC_CONFIG_HEADERS([include/config.h])

# Checks for programs.
#AC_PROG_CC

# Checks for libraries.
#AC_CHECK_LIB([hoard], [malloc])

# Checks for header files.
AC_CHECK_HEADERS([fcntl.h stdint.h unistd.h stdatomic.h pthread.h stdalign.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_CHECK_HEADER_STDBOOL
AC_C_INLINE
AC_TYPE_INT64_T
AC_TYPE_SIZE_T
AC_TYPE_UINT32_T
AC_TYPE_UINT64_T
AC_TYPE_UINT8_T

# Checks for library functions.
AC_FUNC_MALLOC
AC_FUNC_REALLOC
AC_CHECK_FUNCS([clock_gettime memset strstr])
AC_CONFIG_FILES([Makefile])
AC_OUTPUT
