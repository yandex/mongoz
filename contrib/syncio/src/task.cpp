/**
 * task.cpp -- implementation of public Task class
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
#include "debug.h"
#include <syncio/task.h>
#include <syncio/id.h>

namespace io {
namespace impl {

TaskBase::~TaskBase()
{
    if (!empty() && !completed()) {
        cancel();
        Coroutine* current = Scheduler::current()->currentCoroutine();
        current->disableCancellation(); // cannot throw anything out of a destructor
        io::wait(*this);
        current->enableCancellation();
    }
    detach();
}

bool TaskBase::completed() const { return !c_ || c_->completed(); }

void TaskBase::detach()
{
    if (c_) {
        c_->deref();
        c_ = 0;
    }
}

void TaskBase::cancel() { if (c_) c_->cancel(); }
void TaskBase::cancel(std::exception_ptr ex) { if (c_) c_->cancel(ex); }

task_id TaskBase::id() const { return impl::TaskHelper::id(c_); }

TaskBase& TaskBase::rename(const std::string& IFDEBUG(name))
{
    IFDEBUG(c_->rename(name));
    return *this;
}

Coroutine* spawn(std::function<void()> start, size_t stackSize /* = 0 */)
{
    std::unique_ptr<impl::Coroutine> c(new impl::Coroutine(std::move(start), stackSize));
    c->ref();
    impl::Scheduler* s = impl::Scheduler::current();
    if (s)
        s->start(c.get());
    return c.release();
}

} // namspace impl


int wait_any(const std::vector<impl::TaskBase*>& tasks, timeout timeout)
{
    return impl::Coroutine::waitAny(impl::TaskHelper::getImpls(tasks), timeout);
}

int wait_all(const std::vector<impl::TaskBase*>& tasks, timeout timeout)
{
    return impl::Coroutine::waitAll(impl::TaskHelper::getImpls(tasks), timeout);
}

void sleep(timeout timeout)
{
    static const std::function<void()> EMPTY;
    impl::Scheduler::current()->stepDownCurrent(EMPTY, EMPTY, timeout);
}

task_id current_task()
{
    return impl::TaskHelper::id(impl::Scheduler::current()->currentCoroutine());
}

} // namespace io
