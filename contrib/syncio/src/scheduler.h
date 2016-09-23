/**
 * scheduler.h -- task scheduling routines
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

#include "ctx.h"
#include "wait.h"
#include "debug.h"
#include "tls.h"
#include <syncio/time.h>
#include <functional>
#include <atomic>
#include <map>
#include <thread>
#include <mutex>
#include <exception>
#include <iostream>
#include <cassert>

namespace io { namespace impl {

class Scheduler;

class Coroutine {
public:
    Coroutine();
    explicit Coroutine(std::function<void()> start, size_t stackSize = 0);
    ~Coroutine();
    
    void detach();
    bool completed() const { return completed_; }

    bool onCPU() const { return ctx_.onCPU(); }
    
    void cancel();
    void cancel(std::exception_ptr ex);
    void disableCancellation() { cancellationDisabled_ = true; }
    void enableCancellation() { cancellationDisabled_ = false; }
    
    Scheduler* scheduler() const { return sched_; }
    size_t stamp() const { return stamp_; }
    
    static int waitAny(std::vector<Coroutine*> cs, timeout t);
    static int waitAll(const std::vector<Coroutine*>& cs, timeout t);
    
    TLS& tls() { return tls_; }

    IFDEBUG( friend std::ostream& operator << (std::ostream& out, const Coroutine& c) {
            return out << "coroutine" << c.name_ << " (" << &c << "; usage=" << c.usage_ << "; stamp=" << c.stamp_ << ")"; } )
    IFDEBUG( void rename(const std::string& name) { name_ = " '" + name + "'"; } )
    
    void ref();
    void deref();
    
private:
    static const uint64_t MAGIC = 0x0A426947614D6F49ull; // "IoMaGiC\n"

    uint64_t magic_ = MAGIC;
    std::atomic<size_t> usage_ { 0 };
    std::atomic<bool> scheduled_ { false };
    std::atomic<bool> started_ { false };
    bool completed_ = false;
    Scheduler* sched_;
    std::function<void()> start_;
    const std::function<void()> finish_;
    std::unique_ptr<Stack> stack_;
    Context ctx_;
    std::mutex mutex_;
    std::atomic<size_t> stamp_ { 1 };
    WaitQueue waiters_;
    TLS tls_;
    bool cancellationDisabled_ = false;
    IFDEBUG( std::string name_ );

    struct {
        WaitQueue* queue_ = 0;
        std::function<void()> customStepDown_;
        std::function<void()> customStepIn_;
        timeout timeout_;
        bool cancelled_ = false;
        std::exception_ptr exception_;
    } stepDown_;
    int result_ = 0;
    
    struct {
        Context ctx;
        std::function<void()> stepDown;
    } cleanup_;
    
    static void trampoline(void* c);
    static void cleanup(void* c);
    static void afterUnwind();
    
    void cancelIfNeed();
        
    friend class Scheduler;
};


class Poller;

class Scheduler {
public:
    Scheduler();
    ~Scheduler();
    
    void start(Coroutine* c);
    void run();
    
    int stepDownCurrent(WaitQueue* queue, timeout t = timeout());
    int stepDownCurrent(const std::function<void()>& stepDown, const std::function<void()>& stepIn, timeout t = timeout());
    int schedule(const WaitItem& item, int result = 0);

    static Scheduler* current();
    Coroutine* currentCoroutine();
    
    void deref() { --usage_; }
    
    Poller& poller() { return *poller_; }

    struct Thread {
        Scheduler* sched = 0;
        Coroutine* current = 0;
        Coroutine* last = 0;
        std::unique_ptr<Coroutine> taskSwitch;
        std::unique_ptr<Coroutine> exit;
    };


private:
    std::unique_ptr<Poller> poller_;
    std::deque<Coroutine*> ready_;
    std::mutex mutex_;
    std::atomic<size_t> usage_;

    struct TimedWaitItem {
        WaitItem item;
        io::timeout timeout;
        TimedWaitItem(Coroutine* c, io::timeout to): item(c), timeout(to)
        {
            assert(to.finite());
        }
    };
    struct TimedCmp {
        bool operator()(const TimedWaitItem& a, const TimedWaitItem& b) const
        {
            return b.timeout < a.timeout
                || (b.timeout == a.timeout && b.item.coroutine() < a.item.coroutine());
        }
    };
    std::priority_queue<TimedWaitItem, std::vector<TimedWaitItem>, TimedCmp> timeouts_;
    std::mutex timeoutMutex_;
    
    std::map<std::thread::id, Thread> threads_;
    std::mutex threadsMutex_;
    Thread& thread();

    void taskSwitch();
    void ioWait();
    Coroutine* fetch();
    int doStepDownCurrent();
    void afterStepDown(Coroutine* c);
    int doSchedule(std::unique_lock<std::mutex>& lock, const WaitItem& item, int result);
};

}} // namespace io::impl
