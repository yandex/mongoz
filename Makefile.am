ACLOCAL_AMFLAGS = -I m4

sbin_PROGRAMS = mongoz

mongoz_SOURCES = \
    src/auth.cpp \
    src/write.cpp \
    src/shard.cpp \
    src/backend.cpp \
    src/http.cpp \
    src/cache.cpp \
    src/monitor.cpp \
    src/config.cpp \
    src/read.cpp \
    src/session.cpp \
    src/main.cpp \
    \
    contrib/bson/src/bson.cpp \
    \
    contrib/syncio/src/ctx.cpp \
    contrib/syncio/src/stream.cpp \
    contrib/syncio/src/unwind-cxxabi.cpp \
    contrib/syncio/src/task.cpp \
    contrib/syncio/src/log.cpp \
    contrib/syncio/src/tls.cpp \
    contrib/syncio/src/engine.cpp \
    contrib/syncio/src/wait.cpp \
    contrib/syncio/src/sched.cpp \
    contrib/syncio/src/addr.cpp \
    contrib/syncio/src/mutex.cpp \
    contrib/syncio/src/fd.cpp \
    contrib/syncio/src/poller.cpp \
    contrib/syncio/src/ctx_x86_64.s


dist_noinst_HEADERS = \
    src/cursor_storage.h \
    src/shard.h \
    src/parallel.h \
    src/lazy.h \
    src/monitor.h \
    src/write.h \
    src/proto.h \
    src/auth.h \
    src/http.h \
    src/read.h \
    src/clock.h \
    src/version.h \
    src/cache.h \
    src/session.h \
    src/log.h \
    src/sorted_vector.h \
    src/error.h \
    src/backend.h \
    src/operations.h \
    src/config.h \
    src/utility.h \
    src/options.h \
    \
    contrib/bson/include/bson/bson.h \
    contrib/bson/include/bson/bson11.h \
    contrib/bson/include/bson/types.h \
    contrib/bson/include/bson/ordering.h \
    \
    contrib/syncio/src/scheduler.h \
    contrib/syncio/src/valgrind.h \
    contrib/syncio/src/poller.h \
    contrib/syncio/src/tls.h \
    contrib/syncio/src/wait.h \
    contrib/syncio/src/mutex.h \
    contrib/syncio/src/helper.h \
    contrib/syncio/src/debug.h \
    contrib/syncio/src/log.h \
    contrib/syncio/src/ctx.h \
    \
    contrib/syncio/include/syncio/addr.h \
    contrib/syncio/include/syncio/impl/bind.h \
    contrib/syncio/include/syncio/impl/future.h \
    contrib/syncio/include/syncio/impl/utility.h \
    contrib/syncio/include/syncio/time.h \
    contrib/syncio/include/syncio/fd.h \
    contrib/syncio/include/syncio/id.h \
    contrib/syncio/include/syncio/syncio.h \
    contrib/syncio/include/syncio/mutex.h \
    contrib/syncio/include/syncio/algorithm.h \
    contrib/syncio/include/syncio/stream.h \
    contrib/syncio/include/syncio/debug.h \
    contrib/syncio/include/syncio/error.h \
    contrib/syncio/include/syncio/engine.h \
    contrib/syncio/include/syncio/task.h

mongoz_CXXFLAGS = -I$(srcdir)/contrib/bson/include -I$(srcdir)/contrib/syncio/include -std=c++0x
mongoz_LDFLAGS = -lpthread -lcrypto

if DEBUG
mongoz_CXXFLAGS += -DIO_DEBUG -DIO_DEBUG_CIRCULAR
endif

if CPUPROFILE
mongoz_CXXFLAGS += -DCPUPROFILE
mongoz_LDFLAGS += -lprofiler
endif

dist_man8_MANS = mongoz.8

mongoz.8: $(srcdir)/manpage
	sed "s/@@DATE@@/`git log --no-walk --format='%ad' --date=short $(srcdir)/manpage`/g" < $(srcdir)/manpage > mongoz.8

clean-local:
	rm -f mongoz.8

EXTRA_DIST = manpage
