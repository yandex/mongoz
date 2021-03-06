/**
 * ctx.h -- context-switching interfaces
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

#include "valgrind.h"
#include <cstdlib>
#include <vector>

namespace io { namespace impl {

class Stack {
public:
    explicit Stack(size_t size);
    ~Stack();
    
    void* ptr() const { return ptr_; }
    size_t size() const { return size_; }
    
private:
    void* ptr_;
    size_t size_;
    IFVALGRIND( unsigned valgrindId_ );
    
    Stack(const Stack&) = delete;
    Stack& operator = (const Stack&) = delete;
};

size_t defaultStackSize();

class Context {
public:
    Context();
    Context(void (*start)(void*), void* arg, Stack& stack);
    bool onCPU() const;
    static int swap(Context* outgoing, Context* incoming);

    int run() { return swap(0, this); }
    
    Context(const Context&) = default;
    Context& operator = (const Context&) = default;

private:
    std::vector<char> data_;
};


}} // namespace io::impl
