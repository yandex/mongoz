/**
 * stream.cpp -- std::iostream utilizing syncio for its operations
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

#include "log.h"
#include <syncio/stream.h>
#include <syncio/fd.h>
#include <vector>
#include <streambuf>
#include <cstring>
#include <cerrno>

namespace io {

namespace {
class BackendImpl: public stream::backend {
public:
    explicit BackendImpl(io::fd fd): fd_(std::move(fd)) {}
    ssize_t read(void* data, size_t size) override { return fd_.read(data, size); }
    ssize_t write(const void* data, size_t size) override { return fd_.write(data, size); }
    const io::fd* fd() const override { return &fd_; }
private:
    io::fd fd_;
};
} // namespace

struct streambuf::Impl {
    static const size_t BUFSIZE = 2048;
    
    std::unique_ptr<stream::backend> backend;
    std::vector<char> rdbuf;
    std::vector<char> wrbuf;
    
    Impl(std::unique_ptr<stream::backend> be): backend(std::move(be)) {}
    Impl(io::fd&& fd): backend(new BackendImpl(std::move(fd))) {}
};

streambuf::streambuf(std::unique_ptr<stream::backend> backend): impl_(new Impl(std::move(backend))) {}
streambuf::streambuf(io::fd&& fd): impl_(new Impl(std::move(fd))) {}
streambuf::~streambuf() {}
    
int streambuf::overflow(int c)
{
    ssize_t ret = 1;
    if (impl_->wrbuf.empty()) {
        impl_->wrbuf.resize(Impl::BUFSIZE);
    } else if (pptr() != &impl_->wrbuf[0]) {
        ret = impl_->backend->write(&impl_->wrbuf[0], pptr() - &impl_->wrbuf[0]);
    }

    char* begin = &impl_->wrbuf[0];
    if (c != traits_type::eof())
        *begin++ = (char) c;

    setp(begin, &impl_->wrbuf.back() + 1);
    return (ret <= 0) ? traits_type::eof() : 0;
}

int streambuf::underflow()
{
    if (impl_->rdbuf.empty())
        impl_->rdbuf.resize(Impl::BUFSIZE);

    ssize_t ret = impl_->backend->read(&impl_->rdbuf[0], impl_->rdbuf.size());
    if (ret > 0) {
        char* p = &impl_->rdbuf[0];
        setg(p, p, p + ret);
        return (unsigned char) *p;
    } else {
        return traits_type::eof();
    }
}

int streambuf::sync()
{
    if (impl_->wrbuf.empty() || pptr() == &impl_->wrbuf[0])
        return 0;

    size_t ret = impl_->backend->write(&impl_->wrbuf[0], pptr() - &impl_->wrbuf[0]);
    setp(&impl_->wrbuf.front(), &impl_->wrbuf.back() + 1);
    return (ret > 0) ? 0 : EOF;
}

std::streamsize streambuf::xsgetn(char* buf, std::streamsize size) { return std::streambuf::xsgetn(buf, size); }
std::streamsize streambuf::xsputn(const char* buf, std::streamsize size) { return std::streambuf::xsputn(buf, size); }
const io::fd* streambuf::fd() const { return impl_->backend->fd(); }


stream::stream() {}
stream::stream(std::unique_ptr<stream::backend> backend): buf_(new streambuf(std::move(backend))) { rdbuf(buf_.get()); }
stream::stream(io::fd&& fd): buf_(new streambuf(std::move(fd))) { rdbuf(buf_.get()); }
stream::stream(stream&& rhs): buf_(std::move(rhs.buf_)) { rdbuf(buf_.get()); rhs.rdbuf(0); }
stream& stream::operator = (stream&& rhs) { buf_ = std::move(rhs.buf_); rdbuf(buf_.get()); rhs.rdbuf(0); return *this; }
stream::~stream() {}

const fd* stream::fd() const { return buf_->fd(); }

} // namespace io
