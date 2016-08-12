ACLOCAL_AMFLAGS = -I m4
AM_CXXFLAGS = -std=c++11 -Wall -Wextra -I$(srcdir)/include

if ENABLE_DEBUG
AM_CXXFLAGS += -DIO_DEBUG -DIO_DEBUG_CIRCULAR
endif

lib_LTLIBRARIES = libsyncio.la
libsyncio_la_SOURCES = \
    src/addr.cpp \
    src/ctx.cpp \
    src/ctx.h \
    src/ctx_x86_64.s \
    src/debug.h \
    src/engine.cpp \
    src/fd.cpp \
    src/helper.h \
    src/log.cpp \
    src/log.h \
    src/mutex.cpp \
    src/mutex.h \
    src/poller.cpp \
    src/poller.h \
    src/sched.cpp \
    src/scheduler.h \
    src/stream.cpp \
    src/task.cpp \
    src/tls.cpp \
    src/tls.h \
    src/unwind-cxxabi.cpp \
    src/valgrind.h \
    src/wait.cpp \
    src/wait.h
libsyncio_la_LIBADD = -lrt
libsyncio_la_LDFLAGS = -version-info 1:0:1

hdrdir = $(includedir)/syncio
hdr_HEADERS = \
    include/syncio/addr.h \
    include/syncio/algorithm.h \
    include/syncio/debug.h \
    include/syncio/engine.h \
    include/syncio/error.h \
    include/syncio/fd.h \
    include/syncio/id.h \
    include/syncio/mutex.h \
    include/syncio/stream.h \
    include/syncio/syncio.h \
    include/syncio/task.h \
    include/syncio/time.h

impldir = $(hdrdir)/impl
impl_HEADERS = \
    include/syncio/impl/bind.h \
    include/syncio/impl/future.h \
    include/syncio/impl/utility.h

check_PROGRAMS = smoke smoke-http stack-overflow
TESTS = smoke smoke-http stack-overflow

smoke_SOURCES = tests/smoke.cpp
smoke_CXXFLAGS = ${AM_CXXFLAGS} -pthread
smoke_LDADD = libsyncio.la

smoke_http_SOURCES = tests/smoke-http.cpp
smoke_http_LDADD = libsyncio.la

stack_overflow_SOURCES = tests/stack-overflow.cpp
stack_overflow_LDADD = libsyncio.la
