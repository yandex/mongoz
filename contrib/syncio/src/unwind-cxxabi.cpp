/**
 * unwind-cxxabi.cpp -- Itanium C++ ABI-based implementation of stack unwinding
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
#include "tls.h"
#include "scheduler.h"
#include <cassert>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <memory>
#include <cxxabi.h>

namespace io {
namespace impl {

namespace {

struct EhGlobals {
    static size_t s_tlsID;
    void* data[4];
    EhGlobals() { memset(data, 0, sizeof(data)); }
};

size_t EhGlobals::s_tlsID = static_cast<size_t>(-1);

struct TlsInit {
    TlsInit() {
        EhGlobals::s_tlsID = TLS::add<EhGlobals>();
    }
} s_tlsReg;

abi::__cxa_eh_globals* getGlobals() throw()
{
    thread_local EhGlobals tlsGlobals;
    Scheduler* sched = Scheduler::current();
    Coroutine* c = sched ? sched->currentCoroutine() : 0;
    EhGlobals* g = c ? &c->tls().get<EhGlobals>(EhGlobals::s_tlsID) : &tlsGlobals;
    return reinterpret_cast<abi::__cxa_eh_globals*>(g);
}

extern "C" abi::__cxa_eh_globals* __cxa_get_globals() throw() { return getGlobals(); }
extern "C" abi::__cxa_eh_globals* __cxa_get_globals_fast() throw() { return getGlobals(); }

static const uint64_t MAGIC = 0x4C45434E41434F49ull; // "IOCANCEL"

union UnwindCtx {    
    struct {
        uint64_t magic;
        void (*next)();
    };
    char ex[sizeof(abi::__forced_unwind)];
};

void destruct(void*) {}

} // namespace

void finalizeUnwind(void* unw)
{
    UnwindCtx* ctx = (UnwindCtx*) unw;
    if (ctx->magic != MAGIC)
        return;
    
    void (*next)() = ctx->next;
    abi::__cxa_free_exception(ctx);
    next();
    assert(!"unforceUnwind() argument returns");
}

void enforceUnwind(void (*next)())
{
    UnwindCtx* p = (UnwindCtx*) abi::__cxa_allocate_exception(sizeof(UnwindCtx));
    memset(p, 0, sizeof(*p));
    p->magic = MAGIC;
    p->next = next;
    abi::__cxa_throw(p, (std::type_info*) &typeid(abi::__forced_unwind), &destruct);
    assert(!"__cxa_throw() returns");
}

}} // namespace io::impl
