/**
 * poller.cpp -- I/O multiplexing routines
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

#include "poller.h"
#include "scheduler.h"
#include "log.h"
#include <syncio/error.h>
#include <memory>
#include <cstring>
#include <unistd.h>
#include <fcntl.h>
#include <sys/epoll.h>

namespace io { namespace impl {

void Direction::lock()
{
    mutex_.lock();
    waiting_.lock();
}

void Direction::unlock() 
{
    waiting_.unlock();
    mutex_.unlock();
}

int Direction::suspend(timeout timeout)
{
    return Scheduler::current()->stepDownCurrent(&waiting_, timeout);
}

void Direction::wakeup()
{
    waiting_.scheduleAll();
}

Directions* Directions::get(int fd)
{
    static std::mutex mutex;
    static std::vector< std::unique_ptr<Directions> > v;
    
    std::unique_lock<decltype(mutex)> lock(mutex);
    
    if (v.size() <= (unsigned) fd)
        v.resize(fd + 1);
    std::unique_ptr<Directions>& d = v[fd];
    if (!d.get())
        d.reset(new Directions);
    return d.get();
}


Poller& Poller::current() { return Scheduler::current()->poller(); }

Poller::Poller(): fd_(-1), pipeRd_(-1), pipeWr_(-1)
{
    try {
        fd_ = epoll_create(16); // size is ignored, must be >0
        if (fd_ == -1)
            throw io::error("epoll_create()", errno);
        
        int pipe[2];
        if (::pipe(pipe))
            throw io::error("pipe()", errno);
        
        pipeRd_ = pipe[0];
        pipeWr_ = pipe[1];
        
        fcntl(pipeRd_, F_SETFL, fcntl(pipeRd_, F_GETFL) | O_NONBLOCK);
        
        struct epoll_event evt;
        evt.events = EPOLLIN | EPOLLOUT;
        evt.data.ptr = &pipeRd_;
        if (epoll_ctl(fd_, EPOLL_CTL_ADD, pipeRd_, &evt))
            throw io::error("epoll_ctl(ADD)", errno);
    }
    catch (...) {
        ::close(fd_);
        ::close(pipeRd_);
        ::close(pipeWr_);
        throw;
    }
}

Poller::~Poller()
{
    ::close(fd_);
    ::close(pipeRd_);
    ::close(pipeWr_);
}

void Poller::add(int fd, Directions* dirs)
{
    LOG_IO("adding fd " << fd << " to poller " << this);
    struct epoll_event evt;
    evt.events = EPOLLIN | EPOLLOUT | EPOLLET;
    evt.data.ptr = dirs;
    if (epoll_ctl(fd_, EPOLL_CTL_ADD, fd, &evt))
        throw io::error("epoll_ctl(ADD)", errno);
}

void Poller::remove(int fd)
{
    LOG_IO("removing fd " << fd << " from poller " << this);
    struct epoll_event evt;
    if (epoll_ctl(fd_, EPOLL_CTL_DEL, fd, &evt))
        throw io::error("epoll_ctl(DEL)", errno);
}

void Poller::wait(timeout timeout)
{
    struct epoll_event evts[32];
    int cnt = -1;
    LOG_IO("waiting for I/O events until " << timeout);
    while (cnt < 0) {
        
        int to;
        if (!timeout.finite())
            to = -1;
        else if (timeout.expired())
            to = 0;
        else
            to = (timeout.micro() - io::timeout::now() + 999) / 1000;
    
        cnt = epoll_wait(fd_, evts, sizeof(evts)/sizeof(*evts), to);
        if (cnt < 0 && errno != EINTR)
            throw io::error("epoll_wait()", errno);
    }
    
    for (struct epoll_event* evt = evts; cnt--; ++evt) {
        if (evt->data.ptr == &pipeRd_) {
            LOG_IO("waked up from another thread");
            char buf[64];
            if (::read(pipeRd_, buf, sizeof(buf)) < 0 && errno != EAGAIN && errno != EINTR)
                throw io::error("read(pollwakeup)", errno);
        } else {
            Directions* dirs = (Directions*) evt->data.ptr;
            if (evt->events & EPOLLIN) {
                LOG_IO("fd " << dirs->read.fd() << " ready for read");
                dirs->read.wakeup();
            }
            if (evt->events & EPOLLOUT) {
                LOG_IO("fd " << dirs->write.fd() << " ready for write");
                dirs->write.wakeup();
            }
            if (evt->events & EPOLLHUP) {
                LOG_IO("fd " << dirs->read.fd() << " hangup");
                dirs->read.wakeup();
                dirs->write.wakeup();
            }
            if (evt->events & EPOLLERR) {
                LOG_IO("fd " << dirs->read.fd() << " error");
                dirs->read.wakeup();
                dirs->write.wakeup();
            }
        }
    }
}

void Poller::wakeup()
{
    LOG_IO("waking up I/O thread");
    while (::write(pipeWr_, " ", 1) < 0) {
        if (errno != EAGAIN && errno != EINTR)
            throw io::error("write(pollwakeup)", errno);
    }
}
    
}} // namespace io::impl
