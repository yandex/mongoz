/**
 * fd.h -- a wrapper class for file descriptor
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

#include "time.h"
#include <string>
#include <utility>

namespace io {

namespace impl {
    struct Directions;
} // namespace impl

class fd;
class addr;

fd connect(const addr& addr, timeout t = timeout());
fd listen(const addr& where);

class fd {
public:
    fd(): fd_(-1), dirs_(0) {}
    explicit fd(int fd);
    ~fd();
    
    explicit operator bool() const { return fd_ != -1; }
    
    fd(fd&& rhs): fd_(rhs.fd_), dirs_(rhs.dirs_) { rhs.fd_ = -1; rhs.dirs_ = 0; }
    fd& operator = (fd&& rhs) { close(); std::swap(fd_, rhs.fd_); std::swap(dirs_, rhs.dirs_); return *this; }
    
    fd accept(timeout t = timeout());
    
    ssize_t read(void* buf, size_t size, timeout t = timeout());
    ssize_t read_all(void* buf, size_t size, timeout t = timeout());
    ssize_t write(const void* buf, size_t size, timeout t = timeout());
    
    addr getsockname() const;
    addr getpeername() const;
    
    int get() const { return fd_; }
    
    void close();

private:
    int fd_;
    impl::Directions* dirs_;
    
    fd(const fd&) = delete;
    fd& operator = (const fd&) = delete;
    
    friend fd connect(const addr& addr, timeout t);
    friend fd listen(const addr& where);
};

} // namespace io
