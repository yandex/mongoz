/**
 * sched.cpp -- task scheduling routines
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

#include "scheduler.h"
#include "helper.h"
#include "poller.h"
#include "log.h"
#include <syncio/mutex.h>
#include <cassert>
#include <algorithm>

#include <signal.h>

#include <cxxabi.h>
namespace io { namespace impl {

void enforceUnwind(void (*next)());
void finalizeUnwind(void* unw);

}} // namespace io::impl

namespace {

void callOnce(std::function<void()>& func)
{
    std::function<void()> f = std::move(func);
    func = std::function<void()>();
    if (f)
        f();
}

bool testUncaughtExceptionBroken()
{
    bool ret = false;
    std::thread([&ret]{
        std::exception_ptr ex;
        try { throw 0; }
        catch(int) { ex = std::current_exception(); }
        try { std::rethrow_exception(ex); }
        catch(int) {}
        ret = std::uncaught_exception();
    }).join();
    return ret;
}

bool uncaughtExceptionBroken()
{
    static const bool cached = testUncaughtExceptionBroken();
    return cached;
}

} // namespace

namespace io { namespace impl {

thread_local Scheduler* g_currentScheduler;

Coroutine::Coroutine(): sched_(0), stack_(nullptr) {}

Coroutine::Coroutine(std::function<void()> start, size_t stackSize /* = 0*/):
    sched_(g_currentScheduler), start_(std::move(start)), stack_(new Stack(stackSize ? stackSize : defaultStackSize())),
    ctx_(&Coroutine::trampoline, this, *stack_)
{}

Coroutine::~Coroutine() { magic_ = 0; }

void Coroutine::ref() { ++usage_; }

void Coroutine::deref()
{
    if (!--usage_) {
        LOG_SCHED("deleting " << *this);
        delete this;
    }
}

void Coroutine::trampoline(void* pc)
{
    Coroutine* c = (Coroutine*) pc;
    assert(c->magic_ == Coroutine::MAGIC);

    LOG_SCHED("entering " << *c);
    try {
        c->cancelIfNeed();
        c->start_();
    }
    catch (abi::__forced_unwind& unw) {
        finalizeUnwind(&unw);
        throw;
    }
    cleanup(c);
}

void Coroutine::cleanup(void* pc)
{
    Coroutine* c = (Coroutine*) pc;
    assert(c->magic_ == Coroutine::MAGIC);

    LOG_SCHED(*c << " completed");
    c->completed_ = true;
    c->waiters_.scheduleAll(0);
    c->sched_->deref();
    
    c->cleanup_.stepDown = [c]{
        c->stack_.reset(0);
        c->deref();
    };
    static const std::function<void()> EMPTY;
    c->sched_->stepDownCurrent(c->cleanup_.stepDown, /*stepIn = */ EMPTY);
    assert(!"should never reach here");
}

void Coroutine::afterUnwind()
{
    Coroutine* c = Scheduler::current()->currentCoroutine();
    c->cleanup_.ctx = Context(&Coroutine::cleanup, c, *c->stack_);
    c->cleanup_.ctx.run();
}

void Coroutine::detach() { deref(); }

void Coroutine::cancel(std::exception_ptr ex)
{
    LOG_SCHED("canceling " << *this);
    std::unique_lock<std::mutex> lock(mutex_);
    if (completed_)
        return;
    stepDown_.exception_ = ex;
    stepDown_.cancelled_ = true;
    sched_->schedule(WaitItem(this), -ECANCELED);
}

void Coroutine::cancel() { cancel(std::exception_ptr()); }

void Coroutine::cancelIfNeed()
{
    if (!stepDown_.cancelled_ || cancellationDisabled_)
        return;
    
    stepDown_.cancelled_ = false;
    LOG_SCHED("unwinding stack");
    assert(uncaughtExceptionBroken() || !std::uncaught_exception());

    if (stepDown_.exception_) {
        std::rethrow_exception(stepDown_.exception_);
    } else {
        enforceUnwind(&Coroutine::afterUnwind);
    }
}

thread_local Scheduler::Thread* g_currentThread;

Scheduler::Thread& Scheduler::thread()
{
    Scheduler::Thread* th = g_currentThread;
    if (!th || th->sched != this) {
        std::unique_lock<std::mutex> lock(threadsMutex_);
        if (!(th = g_currentThread) || th->sched != this) {
            th = &threads_[std::this_thread::get_id()];
            th->sched = this;
            th->taskSwitch.reset(new Coroutine([this]{ taskSwitch(); }));
            g_currentThread = th;
        }
    }
    return *th;
}
    
int Coroutine::waitAny(std::vector<Coroutine*> cs, timeout t)
{
    LOG_SCHED("entering waitAny() with " << cs.size() << " coroutines and timeout = " << t);
    std::sort(std::begin(cs), std::end(cs)); // fix order to avoid deadlocks

    Coroutine* self = Scheduler::current()->currentCoroutine();
    std::vector<WaitQueue::Lock> locks;
    for (Coroutine* c: cs) {
        assert(c != self);
        if (c)
            locks.emplace_back(c->waiters_);
    }
    
    if (std::any_of(cs.begin(), cs.end(), [](Coroutine* c) { return !c || c->completed(); })) {
        LOG_SCHED("leaving waitAny(), since there is a completed coroutine already");
        return 0;
    }

    int ret = Scheduler::current()->stepDownCurrent([&cs, self]{
        for (Coroutine* c: cs) {
            LOG_SCHED(*self << " waiting for " << *c);
            c->waiters_.push(self);
            c->waiters_.unlock();
        }
    }, [&cs]{
        for (Coroutine* c: cs)
            c->waiters_.lock();
    }, t);
    
    LOG_SCHED("leaving waitAny()");
    return ret;
}
    
int Coroutine::waitAll(const std::vector<Coroutine*>& cs, timeout t)
{
    LOG_SCHED("entering waitAll() with " << cs.size() << " coroutines and timeout = " << t);
    int ret = 0;
    for (Coroutine* c: cs) {
        if (!c)
            continue;
        WaitQueue::Lock lock(c->waiters_);
        if (!c->completed() && !ret)
            ret = Scheduler::current()->stepDownCurrent(&c->waiters_, t);
    }
    LOG_SCHED("leaving waitAll()");
    return ret;
}

Scheduler* Scheduler::current() { return g_currentScheduler; }

Coroutine* Scheduler::currentCoroutine() { return thread().current; }

Scheduler::Scheduler(): poller_(new Poller), usage_(0) {}
Scheduler::~Scheduler() {}

void Scheduler::start(Coroutine* c)
{
    ++usage_;
    c->ref();
    LOG_SCHED("starting " << *c);
    bool wasStarted = c->started_.exchange(true);
    assert(!wasStarted);
    c->started_ = true;
    c->sched_ = this;
    schedule(WaitItem(c));
}

void Scheduler::run()
{
    LOG_SCHED("starting scheduler");
    (void) uncaughtExceptionBroken(); // check and cache the result
    g_currentScheduler = this;
    Thread& th = thread();
    th.exit.reset(new Coroutine);
    th.exit->sched_ = this;
    th.exit->ref();
    th.last = th.exit.get();
    Context::swap(&th.exit->ctx_, &th.taskSwitch->ctx_);
    g_currentScheduler = 0;
    LOG_SCHED("scheduler exited");
}

int Scheduler::schedule(const WaitItem& item, int result /* = 0 */)
{
    std::unique_lock<std::mutex> lock(mutex_);
    return doSchedule(lock, item, result);
}

int Scheduler::doSchedule(std::unique_lock<std::mutex>&, const WaitItem& item, int result)
{
    Coroutine* c = item.coroutine();
    assert(c->magic_ == Coroutine::MAGIC);

    // Allow several WaitItems with the same coroutine to be queued simultaneously
    size_t oldstamp = item.stamp();
    if (!c->stamp_.compare_exchange_strong(oldstamp, item.stamp() + 1)) {
        LOG_SCHED(*c << " already executed (stamp=" << item.stamp() << ")");
        return -EALREADY;
    }
    
    if (c->scheduled_.exchange(true)) {
        LOG_SCHED(*c << " already scheduled for execution");
        return -EALREADY;
    }
        
    LOG_SCHED("scheduling " << *c << " for execution (stamp=" << item.stamp() << ")");
    assert(c->scheduler() == this);
    assert(thread().current != c);
    
    c->result_ = result;
    bool wasEmpty = ready_.empty();
    ready_.push_back(c);
    if (wasEmpty)
        poller_->wakeup();
    
    return 0;
}

int Scheduler::stepDownCurrent(WaitQueue* queue, timeout timeout /* = timeout() */)
{
    Thread& th = thread();
    th.current->stepDown_.queue_ = queue;
    th.current->stepDown_.timeout_ = timeout;
    
    LOG_SCHED(*th.current << " stepped down (stamp=" << th.current->stamp_ << ") with wait queue " << queue << " and timeout = " << timeout);
    return doStepDownCurrent();
}

int Scheduler::stepDownCurrent(
    const std::function<void()>& stepDown,
    const std::function<void()>& stepIn,
    timeout timeout /* = timeout() */
){
    // NB: these std::functions must be passed by reference, otherwise
    //     memory leak will occur
    
    Thread& th = thread();
    th.current->stepDown_.customStepDown_ = stepDown;
    th.current->stepDown_.customStepIn_ = stepIn;
    th.current->stepDown_.timeout_ = timeout;

    LOG_SCHED(*th.current << " stepped down (stamp=" << th.current->stamp_ << ") with custom stepdown policy and timeout = " << timeout);    
    return doStepDownCurrent();
}

int Scheduler::doStepDownCurrent()
{
    sys::impl::assertUnlocked(); // check for not holding any std::mutex-like locks

    Thread& th = thread();
    th.last = th.current;
    th.current = 0;
    
    Coroutine* c = th.last;
    Context::swap(&th.last->ctx_, &th.taskSwitch->ctx_);
    
    if (c->stepDown_.queue_) {
        c->stepDown_.queue_->lock();
        c->stepDown_.queue_ = 0;
    } else {
        callOnce(c->stepDown_.customStepIn_);
    }
    
    c->cancelIfNeed();
    return c->result_;
}

Coroutine* Scheduler::fetch()
{
    std::unique_lock<std::mutex> lock(mutex_);
    if (!ready_.empty()) {
        Coroutine* next = ready_.front();
        ready_.pop_front();
        return next;
    } else {
        return 0;
    }
}

void Scheduler::afterStepDown(Coroutine* c)
{
    if (c->stepDown_.timeout_.finite()) {
        timeout t = c->stepDown_.timeout_;
        c->stepDown_.timeout_ = timeout();
        std::unique_lock<std::mutex> lock(timeoutMutex_);
        timeouts_.push(TimedWaitItem(c, t));
    }
    if (c->stepDown_.queue_) {
        c->stepDown_.queue_->push(c);
        c->stepDown_.queue_->unlock();
    } else {
        callOnce(c->stepDown_.customStepDown_);
    }    
}

void Scheduler::taskSwitch()
{
    Thread& th = thread();
    
    for (;;) {
        Coroutine* c = 0;
        while ((c = fetch()) == 0) {
            if (usage_) {
                ioWait();
            } else {
                poller_->wakeup();
                Context::swap(&th.taskSwitch->ctx_, &th.exit->ctx_);
            }
        }
        LOG_SCHED("switching to " << *c);

        assert(c->magic_ == Coroutine::MAGIC);
        assert(c->scheduled_);
        
        th.current = c;
        c->ref();
        c->mutex_.lock();
        c->scheduled_ = false;
        Context::swap(&th.taskSwitch->ctx_, &c->ctx_);
        afterStepDown(c);
        c->mutex_.unlock();
        c->deref();
    }
}


void Scheduler::ioWait()
{    
    timeout t;
    {
        std::unique_lock<std::mutex> lock(timeoutMutex_);
        if (!timeouts_.empty())
            t = timeouts_.top().timeout;
    }
    
    poller_->wait(t);
    
    {
        std::unique_lock<std::mutex> lock1(timeoutMutex_);
        std::unique_lock<std::mutex> lock2(mutex_);
        ssize_t now = timeout::now();
        while (!timeouts_.empty() && timeouts_.top().timeout.micro() <= now) {
            doSchedule(lock2, timeouts_.top().item, -ETIMEDOUT);
            timeouts_.pop();
        }
    }
}


}} // namespace io::impl
