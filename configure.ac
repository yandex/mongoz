# Mode: -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.65])
AC_INIT([mongoz], [0.1.6], [dprokoptsev@yandex-team.ru])
AM_INIT_AUTOMAKE([foreign -Wall])
AC_CONFIG_SRCDIR([src/main.cpp])
AC_CONFIG_HEADERS([config.h])
AC_CONFIG_MACRO_DIR([m4])

# Checks for programs.
AC_PROG_CXX
AC_PROG_CC
AM_PROG_AS
m4_ifdef([AM_PROG_AR], [AM_PROG_AR])
AC_PROG_LIBTOOL
AC_LANG_CPLUSPLUS

# Checks for libraries.
AC_CHECK_LIB(rt, clock_gettime)
if test "x$ac_cv_lib_rt_clock_gettime" != "xyes"; then
    AC_MSG_ERROR([mongoz requires librt.])
fi

# Checks for header files.
AC_CHECK_HEADERS(
    [arpa/inet.h fcntl.h netdb.h netinet/in.h stddef.h stdint.h stdlib.h string.h sys/socket.h sys/time.h syslog.h unistd.h],
    [], [AC_MSG_ERROR([cannot find all required headers])]
)
AC_CHECK_HEADERS([valgrind/valgrind.h])
AC_CHECK_HEADERS([google/profiler.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_HEADER_STDBOOL
AC_C_INLINE
AC_TYPE_INT32_T
AC_TYPE_INT64_T
AC_TYPE_INT8_T
AC_TYPE_SIZE_T
AC_TYPE_SSIZE_T
AC_TYPE_UINT32_T
AC_TYPE_UINT64_T
AC_TYPE_UINT8_T

# Checks for library functions.
AC_FUNC_FORK
AC_CHECK_FUNCS([dup2 gettimeofday localtime_r memset select socket strerror tzset accept4])

AC_CACHE_CHECK([whether compiler supports C++11], [mongoz_cv_cxx11_supported], [
    mongoz_saved_CXXFLAGS="$CXXFLAGS"
    CXXFLAGS="$CXXFLAGS -std=c++0x"
    AC_TRY_COMPILE([
        template<class... Args> void foo(Args&&...) {}    // Check for variadic templates
                                                          // and rvalue references
        void bar() { foo([](int x) { return x + 1; }); }  // Check for lambdas
    ], [], [mongoz_cv_cxx11_supported=yes], [mongoz_cv_cxx11_supported=no])
    CXXFLAGS="$mongoz_saved_CXXFLAGS"
])
if test "x$mongoz_cv_cxx11_supported" != "xyes"; then
    AC_MSG_ERROR([mongoz requires compiler supporting C++11. Please upgrade.])
fi


AC_CACHE_CHECK([the value of CLOCK_MONOTONIC_RAW], [mongoz_cv_clock_monotonic_raw_value], [
    AC_TRY_LINK([
        #include <stdio.h>
        #include <linux/time.h>
    ], [printf("%d\n", CLOCK_MONOTONIC_RAW);],
    [mongoz_cv_clock_monotonic_raw_value=`./conftest$ac_exeext`], [mongoz_cv_clock_monotonic_raw_value=undefined])
])
if test "x$mongoz_cv_clock_monotonic_raw_value" != "xundefined"; then
    AC_DEFINE_UNQUOTED([CLOCK_MONOTONIC_RAW_VALUE], [$mongoz_cv_clock_monotonic_raw_value], [Define to the value of CLOCK_MONOTONIC_RAW clock ID.])
    AC_CACHE_CHECK([whether CLOCK_MONOTONIC_RAW is supported], [mongoz_cv_clock_monotonic_raw_useful], [
        AC_TRY_RUN([
            #include <time.h>
            int main() {
                struct timespec ts;
                return (!clock_gettime(CLOCK_MONOTONIC_RAW_VALUE, &ts)) ? 0 : 1;
            }
        ], [mongoz_cv_clock_monotonic_raw_useful=yes], [mongoz_cv_clock_monotonic_raw_useful=no])
    ])
    if test "x$mongoz_cv_clock_monotonic_raw_useful" = "xyes"; then
        AC_DEFINE([HAVE_CLOCK_MONOTONIC_RAW], [1], [Define to 1 if clock_gettime(CLOCK_MONOTONIC_RAW) is supported.])
    fi
fi

AC_CHECK_FUNCS([epoll_create epoll_ctl epoll_wait])
if test "x$ac_cv_func_epoll_create"   != "xyes" \
    || test "x$ac_cv_func_epoll_ctl"  != "xyes" \
    || test "x$ac_cv_func_epoll_wait" != "xyes"
then
    AC_MSG_ERROR([mongoz requires epoll.])
fi

AC_ARG_ENABLE([debug], [AS_HELP_STRING([--enable-debug], [turn on debugging])], [mongoz_debug=yes], [mongoz_debug=no])
AC_ARG_ENABLE([profiling], [AS_HELP_STRING([--enable-profiling], [turn on CPU profiling])], [mongoz_cpuprofile=yes], [mongoz_cpuprofile=no])
AM_CONDITIONAL([DEBUG], [ test "x$mongoz_debug" = "xyes" ])
AM_CONDITIONAL([CPUPROFILE], [ test "x$mongoz_cpuprofile" = "xyes" ])

if test "x$mongoz_cpuprofile" = "xyes" && test "x$ac_cv_header_google_profiler_h" = "x"; then
    AC_MSG_ERROR([--enable-profiling option requires google/profiler.h header file.])
fi

AC_CONFIG_FILES([Makefile])
AC_OUTPUT
