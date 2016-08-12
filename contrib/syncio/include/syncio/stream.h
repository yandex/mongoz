/**
 * stream.h -- std::iostream utilizing syncio for its operations
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

#include <iostream>
#include <streambuf>
#include <memory>

namespace io {

class fd;
class streambuf;

class stream: public std::iostream {
public:
    class backend {
    public:
        virtual ~backend() {}
        virtual ssize_t read(void* data, size_t size) = 0;
        virtual ssize_t write(const void* data, size_t size) = 0;
        virtual const io::fd* fd() const = 0;
    };
    
    stream();
    explicit stream(io::fd&& fd); // takes ownership of `fd'
    explicit stream(std::unique_ptr<backend> backend);
    stream(const stream&) = delete;
    stream& operator = (const stream&) = delete;    
    stream(stream&& stream);
    stream& operator = (stream&& stream);
    ~stream();
    
    const io::fd* fd() const; // For logging and debugging purposes

private:
    std::unique_ptr<streambuf> buf_;
};


class streambuf: public std::streambuf {
public:
    explicit streambuf(io::fd&& fd);
    explicit streambuf(std::unique_ptr<stream::backend> backend);
    streambuf(const streambuf&) = delete;
    streambuf& operator = (const streambuf&) = delete;
    ~streambuf();

    const io::fd* fd() const; // For logging and debugging purposes

protected:
    int overflow(int c) override;
    int underflow() override;
    int sync() override;
    std::streamsize xsgetn(char* buf, std::streamsize size) override;
    std::streamsize xsputn(const char* buf, std::streamsize size) override;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace io
