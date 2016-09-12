#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.60])
AC_INIT([libsyncio], [0.2.1-0], [dprokpotsev@yandex-team.ru])
AM_INIT_AUTOMAKE([foreign -Wall])
AC_CONFIG_SRCDIR([include/syncio/syncio.h])
AC_CONFIG_HEADER([config.h])
AC_CONFIG_MACRO_DIR([m4])
m4_pattern_allow([AM_PROG_AR])

AC_LANG_CPLUSPLUS

# Checks for programs.
AC_PROG_CXX
AC_PROG_CC
AM_PROG_AS
AM_PROG_AR
AC_PROG_LIBTOOL

# Checks for header files.
AC_CHECK_HEADERS([arpa/inet.h fcntl.h netdb.h netinet/in.h stddef.h stdint.h stdlib.h string.h sys/socket.h unistd.h])

# Check for library functions.
AC_CHECK_FUNCS([accept4])

AC_ARG_ENABLE([debug], AS_HELP_STRING([--enable-debug], [dump all i/o activity to an internal circular buffer]))
AM_CONDITIONAL([ENABLE_DEBUG], [test x"$enable_debug" = xyes])

AC_CONFIG_FILES([Makefile])
AC_OUTPUT
