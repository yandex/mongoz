/**
 * log.h -- a few logging facilities
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

#include "debug.h"
#include "valgrind.h"

#ifdef IO_DEBUG

#include <syncio/time.h>
#include <sstream>
#include <utility>
#include <iostream>
#include <iomanip>
#include <atomic>
#include <mutex>
#include <pthread.h>

#include <sys/types.h>
#include <syscall.h>

#ifdef IO_DEBUG_CIRCULAR
#include <fstream>
#include <vector>
#include <cstring>
#endif

namespace io { namespace impl {

enum class LogIdent: size_t {
    CORE = 0, SCHED = 1, IO = 2, APP = 3
};

class Log {
public:
    Log(): start_(timeout::now()), mask_(-1) {}
    static Log& instance();
    
    static ssize_t startTime() { return instance().start_; }
    size_t mask() const { return mask_; }
    
    static size_t threadId()
    {
        return pthread_self();
    }
    
    static void dump()
    {        
#ifdef IO_DEBUG_CIRCULAR
        Log& self = instance();
        std::unique_lock<std::mutex> lock(self.mutex_);
        std::ofstream f("lastlog");
        for (ThreadLocalLog* log: self.logs_)
            log->dump(f);
#endif
    }
    
    void write(const std::string& msg)
    {
#ifdef IO_DEBUG_CIRCULAR
        threadLocal().write(msg);
#else
        std::clog << msg << std::flush;
#endif
    }
    
    std::unique_lock<std::mutex> globalLock() { return std::unique_lock<std::mutex>(mutex_); }
 
    ~Log()
    {
#if defined(IO_DEBUG_CIRCULAR) && defined(HAVE_VALGRIND_VALGRIND_H)
        if (RUNNING_ON_VALGRIND) {

            // Usually these `still reachable' log pages aren't a problem,
            // and their absense may interfere with something trying to write logs
            // while applicaton terminates (and Log::instance() is already gone),
            // so we don't do cleanups in here unless we're running on valgrind
            // and do not want it to report these pages while checking for memleaks.

            std::unique_lock<std::mutex> lock(mutex_);
            for (ThreadLocalLog* log: logs_)
                delete log;
            logs_.clear();
        }
#endif
    }
    
    
private:
    ssize_t start_;
    size_t mask_;
    std::mutex mutex_;
    
#ifdef IO_DEBUG_CIRCULAR
    struct ThreadLocalLog {
        std::vector<char> buffer_ = std::vector<char>(16*1024*1024, '\0');
        char* bufpos_ = &buffer_.front();
        bool buffull_ = false;
        
        void write(const std::string& msg)
        {
            const char* msgbeg = msg.c_str();
            const char* msgend = msgbeg + msg.size();

            const char* bufend = &buffer_.front() + buffer_.size();
            while (msgbeg != msgend) {
                if (bufpos_ == bufend) {
                    buffull_ = true;
                    bufpos_ = &buffer_.front();
                }
                ssize_t chunk = std::min(msgend - msgbeg, bufend - bufpos_);
                memcpy(bufpos_, msgbeg, chunk);
                msgbeg += chunk, bufpos_ += chunk;
            }
        }
        
        void dump(std::ostream& f)
        {
            if (buffull_) {
                f.write(bufpos_, &buffer_.front() + buffer_.size() - bufpos_);
            }
            f.write(&buffer_.front(), bufpos_ - &buffer_.front());
            bufpos_ = &buffer_.front();
            buffull_ = false;
        }
    };
    
    std::vector<ThreadLocalLog*> logs_;
    
    ThreadLocalLog& threadLocal()
    {
        thread_local ThreadLocalLog* log = 0;
        if (!log) {
            log = new ThreadLocalLog;
            std::unique_lock<std::mutex> lock(mutex_);
            logs_.push_back(log);
        }
        return *log;
    }
    
#endif
};

class LogMessage {
public:
    explicit LogMessage(LogIdent ident): enabled_(Log::instance().mask() & (1 << (size_t) ident))
    {
        static const char* IDENT_NAMES[] = { "core", "sched", "io", "app" };
        
        if (enabled_)
            putHeader(s_, IDENT_NAMES[(size_t) ident]);
    }
    
    template<class T>
    LogMessage& operator << (T&& t) { if (enabled_) s_ << std::forward<T>(t); return *this; }
    
    ~LogMessage()
    {
        if (enabled_) {
            s_ << "\n";
            Log::instance().write(s_.str());
        }
    }
    
private:
    bool enabled_;
    std::ostringstream s_;
    
    void putHeader(std::ostream& s, const char* ident);
};

inline std::ostream& operator << (std::ostream& out, const timeout& to)
{
    if (to.finite()) {
        return out << std::fixed << std::setw(12) << std::setprecision(6) << std::setfill('0')
                   << (to.micro() - Log::startTime()) / 1000000.0;
    } else {
        return out << "+INF";
    }
}

}} // namespace impl

#define LOG_CORE(...)  ::io::impl::LogMessage(::io::impl::LogIdent::CORE)  << __VA_ARGS__
#define LOG_SCHED(...) ::io::impl::LogMessage(::io::impl::LogIdent::SCHED) << __VA_ARGS__
#define LOG_IO(...)    ::io::impl::LogMessage(::io::impl::LogIdent::IO)    << __VA_ARGS__
#define LOG_APP(...)   ::io::impl::LogMessage(::io::impl::LogIdent::APP)   << __VA_ARGS__
#define IFDEBUG(...) __VA_ARGS__

#else

#define LOG_CORE(...)
#define LOG_SCHED(...)
#define LOG_IO(...)
#define LOG_APP(...)
#define IFDEBUG(...)

#endif
