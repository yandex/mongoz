/**
 * condvar.h -- syncio based condition variable
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

#include <syncio/condvar.h>
#include "wait.h"
#include "scheduler.h"

namespace io { namespace impl {

class CondVar
{
public:
    void notifyOne() { queue_.scheduleOne(); }

    void notifyAll() { queue_.scheduleAll(); }

    std::cv_status wait(std::unique_lock<mutex>& lock, timeout timeout)
    {
        WaitQueue::Lock queueLock(queue_);
        Scheduler* s = Scheduler::current();
        Coroutine* c = s->currentCoroutine();

        int result = s->stepDownCurrent([this, &queueLock, &lock, c]{
            queue_.push(c);
            queueLock.unlock();
            lock.unlock();
        }, [&queueLock, &lock]{
            lock.lock();
            queueLock.lock();
        }, timeout);

        if (result == -ETIMEDOUT) {
            return std::cv_status::timeout;
        }
        return std::cv_status::no_timeout;
    }

private:
    WaitQueue queue_;
};

}} // namespace io::impl
