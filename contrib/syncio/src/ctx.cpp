/**
 * ctx.cpp -- middle layer of context switching
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

#include "ctx.h"
#include "valgrind.h"
#include <algorithm>
#include <stdexcept>

#include <sys/mman.h>
#include <string.h>
#include <unistd.h>

namespace io { namespace impl {

#ifdef IO_DEBUG

namespace {

size_t pageSize()
{
    // We assume that this will never fail, otherwise mprotect() will return ENOMEM
    static const size_t PAGE_SIZE = sysconf(_SC_PAGESIZE);
    return PAGE_SIZE;
}

} // namespace

void* allocateStack(size_t size)
{
    void* p = mmap(nullptr, size + pageSize(),
        PROT_READ | PROT_WRITE,
        MAP_PRIVATE | MAP_ANONYMOUS | MAP_STACK,
        /*fd =*/-1, /*offset =*/0);
    if (p == MAP_FAILED)
        return 0;

    if (mprotect(p, pageSize(), PROT_NONE)) {
        int err = errno;
        munmap(p, size + pageSize());
        throw std::runtime_error(std::string("stack mprotect: ") + strerror(err));
    }

    return static_cast<char*>(p) + pageSize();    
}

void freeStack(void* ptr, size_t size)
{
    munmap(static_cast<char*>(ptr) - pageSize(), size + pageSize());
}

#else

void* allocateStack(size_t size) { return malloc(size); }
void freeStack(void* ptr, size_t /*size*/) { free(ptr); }

#endif

extern "C" {
    size_t __io_ctx_size();
    void __io_ctx_init(char* ctx, void (*start)(void*), void* arg, void* stack, size_t stackSize);
    int __io_ctx_swap(char* oldctx, char* newctx);
    bool __io_ctx_on_cpu(const char* ctx);
}

Stack::Stack(size_t size): ptr_(nullptr), size_(size)
{
    if (size_ < sizeof(void(*)(void*)))
        throw std::invalid_argument("requested stack size is too small");

    ptr_ = allocateStack(size_);
    if (!ptr_)
        throw std::bad_alloc();
    
    IFVALGRIND( valgrindId_ = VALGRIND_STACK_REGISTER(ptr_, (char*) ptr_ + size_) );
}

Stack::~Stack()
{
    IFVALGRIND( VALGRIND_STACK_DEREGISTER(valgrindId_) );
    freeStack(ptr_, size_);
}

size_t defaultStackSize() { return 65536; }

Context::Context(): data_(__io_ctx_size(), 0) {}

Context::Context(void (*start)(void*), void* arg, Stack& stack): data_(__io_ctx_size(), 0)
{
    __io_ctx_init(data_.data(), start, arg, stack.ptr(), stack.size());
}

int Context::swap(Context* outgoing, Context* incoming)
{
    return __io_ctx_swap(outgoing ? outgoing->data_.data() : 0, incoming->data_.data());
}

bool Context::onCPU() const { return __io_ctx_on_cpu(data_.data()); }

}} // namespace io::impl
