/**
 * fd.cpp -- 
 *
 * This file is part of mongoz, a more sound implementation
 * of mongodb sharding server.
 *
 * Copyright (c) 2016 YANDEX LLC
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#include "platform.h"
#include "poller.h"
#include "log.h"
#include <syncio/error.h>
#include <syncio/addr.h>
#include <syncio/fd.h>
#include <vector>
#include <mutex>
#include <memory>
#include <string>
#include <stdexcept>
#include <cstring>
#include <cassert>
#include <sstream>

#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>

namespace io {

fd::fd(int fd): fd_(fd)
{
    if (fd != -1) {
        dirs_ = impl::Poller::current().getDirections(fd);
        dirs_->read.reset(fd);
        dirs_->write.reset(fd);
        impl::Poller::current().add(dirs_);
    } else {
        dirs_ = 0;
    }
}

fd::~fd()
{
    close();
}

namespace impl {


typedef ssize_t (*IoFunc)(int, void*, size_t);
enum class IoMode { Some, All };

ssize_t doIO(IoFunc func, int fd, void* buf, size_t size, timeout timeout, Direction& dir, IoMode mode, const char* IFDEBUG(funcname))
{
    LOG_IO("entering " << funcname << "(" << fd << ", " << buf << ", " << size << " bytes)");
    Direction::Lock lock(dir);
    
    char* pos = (char*) buf;
    char* end = pos + size;
    
    while (pos != end) {
        
        ssize_t chunk = (*func)(fd, pos, end - pos);
        
        if (chunk > 0) {
            pos += chunk;
            continue;
        } else if (chunk == 0) {
            break;
        } else if (errno == EINTR) {
            continue;
        } else if (errno != EAGAIN && errno != EWOULDBLOCK) {
            if (pos != (char*) buf) {
                break;
            } else {
                int err = -errno;
                LOG_IO("leaving " << funcname << "(" << fd << ") = " << err);
                return err;
            }
        }
        
        // errno == EAGAIN || errno == EWOULDBLOCK
        if (mode == IoMode::All || pos == (char*) buf) {
            LOG_IO("suspending from " << funcname << "(" << fd << ")");
            if (dir.suspend(timeout)) {
                LOG_IO("resuming " << funcname << "(" << fd << "); timeout");
                if (pos != (char*) buf) {
                    break;
                } else {
                    LOG_IO("leaving " << funcname << "(" << fd << ") = " << -ETIMEDOUT);
                    return -ETIMEDOUT;
                }
            }
            LOG_IO("resuming " << funcname << "(" << fd << ")");
        } else {
            break;
        }
    }
    
    LOG_IO("leaving " << funcname << "(" << fd << ") = " << (pos - (char*) buf));;
    return pos - (char*) buf;
}

} // namespace impl

ssize_t fd::read(void* buf, size_t size, timeout timeout)
{
    assert(dirs_);
    return impl::doIO(&::read, fd_, buf, size, timeout, dirs_->read, impl::IoMode::Some, "read");
}

ssize_t fd::read_all(void* buf, size_t size, timeout timeout)
{
    assert(dirs_);
    return impl::doIO(&::read, fd_, buf, size, timeout, dirs_->read, impl::IoMode::All, "readAll");
}

ssize_t fd::write(const void* buf, size_t size, timeout timeout)
{
    assert(dirs_);
    return impl::doIO((impl::IoFunc) &::write, fd_, (void*) buf, size, timeout, dirs_->write, impl::IoMode::All, "write");
}


fd connect(const addr& addr, timeout timeout)
{
    auto bail = [&addr](int err) {
        std::ostringstream msg;
        msg << "cannot connect to " << addr;
        throw io::error(msg.str(), err);
    };
    
    LOG_IO("entering connect(" << addr << ")");

    int fd = platform::socket(addr.af(), addr.socktype(), addr.proto());
    if (fd == -1)
        bail(errno);

    io::fd ret(fd);
    impl::Direction::Lock lock(ret.dirs_->write);
    int err = ::connect(fd, addr.sockaddr(), addr.addrlen());
    if (err == 0)
        return ret;

    if (errno == EINPROGRESS) {

        LOG_IO("suspending from connect()");
        if (ret.dirs_->write.suspend(timeout)) {
            LOG_IO("resuming connect(); timeout");
            bail(ETIMEDOUT);
        } else {
            LOG_IO("resuming connect()");
            socklen_t len = sizeof(ret);
            ::getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len);
            if (err == 0) {
                LOG_IO("connection successful; leaving connect(" << addr << ") = " << fd);
                return ret;
            } else {
                LOG_IO("connection failed: " << strerror(err));
                bail(err);
            }
        }   
    } else {
        err = errno;
        LOG_IO("connection failed: " << strerror(err));
        bail(err);
    }
    
    assert(!"should never reach here");
}

fd listen(const addr& where)
{
    int fd = -1;
    auto bail = [&where, &fd](int err) {
        std::ostringstream msg;
        msg << "cannot listen on " << where;
        if (fd != -1)
            ::close(fd);
        throw io::error(msg.str(), err);
    };
   
    fd = platform::socket(where.af(), where.socktype(), where.proto());
    if (fd == -1)
        bail(errno);
    
    int flag = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &flag, sizeof(flag)))
        bail(errno);
    
    if (where.af() == AF_INET6 && setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &flag, sizeof(flag)))
        bail(errno);

    if (bind(fd, where.sockaddr(), where.addrlen()))
        bail(errno);

    if (::listen(fd, SOMAXCONN))
        bail(errno);

    return io::fd(fd);
}

fd fd::accept(timeout timeout)
{
    LOG_IO("entering accept(" << fd_ << ")");
    assert(dirs_);
    impl::Direction::Lock lock(dirs_->read);    
    for (;;) {
       
        int fd = platform::accept(fd_);
        if (fd != -1) {
            LOG_IO("leaving accept(" << fd_ << ") = " << fd);
            return io::fd(fd);
        }
        
        if (errno == EINTR || errno == ECONNABORTED || errno == ENETDOWN ||
            errno == EPROTO || errno == ENOPROTOOPT || errno == EHOSTDOWN ||
            errno == ENONET || errno == EHOSTUNREACH || errno == EOPNOTSUPP ||
            errno == ENETUNREACH
        ) {
            continue;
        } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
            LOG_IO("suspending from accept(" << fd_ << ")");
            if (dirs_->read.suspend(timeout)) {
                LOG_IO("resuming accept(" << fd_ << "); timeout");
                errno = ETIMEDOUT;
                return io::fd();
            }
            LOG_IO("resuming accept(" << fd_ << ")");
        } else {
            throw io::error("cannot accept() a new connection", errno);
        }
    }
    
}


addr fd::getsockname() const
{
    addr::Union u;
    socklen_t len = sizeof(u);
    if (!::getsockname(fd_, &u.generic_, &len))
        return addr(u.generic_.sa_family, 0, 0, &u.generic_, len);
    else
        throw io::error("getsockname()", errno);
}


addr fd::getpeername() const
{
    addr::Union u;
    socklen_t len = sizeof(u);
    if (!::getpeername(fd_, &u.generic_, &len))
        return addr(u.generic_.sa_family, 0, 0, &u.generic_, len);
    else
        throw io::error("getpeername()", errno);
}


void fd::close()
{
    if (fd_ != -1) {
        impl::Poller::current().remove(fd_);
        ::close(fd_);
        fd_ = -1;

        dirs_->read.wakeup();
        dirs_->write.wakeup();
        dirs_ = nullptr;
    }
}


} // namespace io
