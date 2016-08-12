/**
 * poller.h -- I/O multiplexing routines
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

#pragma once

#include "mutex.h"
#include "wait.h"
#include <syncio/time.h>
#include <mutex>

namespace io { namespace impl {

class Direction {
public:
    Direction(): fd_(-1) {}
    void reset(int fd) { fd_ = fd; }
    int fd() const { return fd_; }
    
    void lock();
    void unlock();
    typedef std::unique_lock<Direction> Lock;
    
    int suspend(timeout timeout);
    void wakeup();    
    
private:
    int fd_;
    Mutex mutex_;
    WaitQueue waiting_;
};

struct Directions {
    Direction read;
    Direction write;
    
    static Directions* get(int fd);
};

class Poller {
public:
    Poller();
    ~Poller();
    
    static Poller& current();
    
    void add(int fd, Directions* dirs);
    void remove(int fd);
    void wait(timeout timeout);
    void wakeup();
    
private:
    int fd_;
    int pipeRd_;
    int pipeWr_;
};

}} // namespace io::impl
