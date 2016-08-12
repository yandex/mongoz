#pragma once

#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <sys/socket.h>
#include <sys/types.h>
#include <fcntl.h>

namespace io {
namespace platform {

#if defined(HAVE_ACCEPT4) && defined(SOCK_NONBLOCK) && defined(SOCK_CLOEXEC)

int socket(int af, int type, int proto) { return ::socket(af, type | SOCK_NONBLOCK | SOCK_CLOEXEC, proto); }
int accept(int fd) { return ::accept4(fd, 0, 0, SOCK_NONBLOCK | SOCK_CLOEXEC); }

#else

int setup(int fd)
{
    if (fd != -1) {
        fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_NONBLOCK);
        fcntl(fd, F_SETFD, fcntl(fd, F_GETFD) | O_CLOEXEC);
    }
    return fd;
}

int accept(int fd) { return setup(::accept(fd, 0, 0)); }
int socket(int af, int type, int proto) { return setup(::socket(af, type, proto)); }

#endif

}} // namespace io::platform
