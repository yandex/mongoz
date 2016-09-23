/**
 * log.cpp -- a few logging facilities
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
#include "scheduler.h"

namespace io {
namespace impl {

#ifdef IO_DEBUG

Log* g_log = new Log;

Log& Log::instance() { return *g_log; }

void LogMessage::putHeader(std::ostream& s, const char* ident)
{
    s << "\033[32;1m" << std::fixed << std::setw(12) << std::setprecision(6) << std::setfill('0')
      << (timeout::now() - Log::startTime()) / 1000000.0
      << " \033[31;1m0x" << std::hex << Log::threadId() << std::dec
      << " \033[33;1m";
    Scheduler* sched = Scheduler::current();
    Coroutine* c = 0;
    if (sched && (c = sched->currentCoroutine()) != 0)
        s << *c;
    else
        s << "(no coroutine)";
    s << " \033[34;1m[" << ident << "]\033[0m ";
}

#endif

} // namespace impl

void log(const std::string& IFDEBUG(str))
{
    LOG_APP(str);
}

} // namespace io


void __io_log_dump()
{
#ifdef IO_DEBUG
    LOG_CORE("--- log dumped ---");
    io::impl::Log::dump();
#endif
}

void __io_log_notice(const char* IFDEBUG(msg))
{
    LOG_CORE("--- " << msg << " ---");
}


#ifdef IO_DEBUG_CIRCULAR
#include <signal.h>

namespace {
struct __io_log_once {
    static void dump(int) { __io_log_dump(); }
    static void notice(int) { __io_log_notice("SIGUSR1"); }
    
    __io_log_once() {

        struct sigaction sa;

        memset(&sa, 0, sizeof(sa));
        sa.sa_handler = &__io_log_once::notice;
        sa.sa_flags = SA_RESTART;
        sigaction(SIGUSR1, &sa, 0);

        memset(&sa, 0, sizeof(sa));
        sa.sa_handler = &__io_log_once::dump;
        sa.sa_flags = SA_RESTART;
        sigaction(SIGUSR2, &sa, 0);

    }
} __io_log_once_reg;
} // namespace

#endif
