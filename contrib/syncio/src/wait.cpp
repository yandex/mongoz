/**
 * wait.cpp -- a syncio wait queue
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

#include "wait.h"
#include "scheduler.h"
#include "log.h"
#include <cassert>

namespace io { namespace impl {

WaitItem::WaitItem(Coroutine* c):
    c_(c), stamp_(c->stamp())
{
    c_->ref();
}

WaitItem::~WaitItem()
{
    if (c_)
        c_->deref();
}

WaitItem::WaitItem(const WaitItem& it): c_(it.c_), stamp_(it.stamp_)
{
    if (c_)
        c_->ref();
}

WaitItem& WaitItem::operator = (WaitItem&& it)
{
    std::swap(c_, it.c_);
    std::swap(stamp_, it.stamp_);
    return *this;
}

int WaitItem::schedule(int result /* = 0 */)
{
    if (c_)
        return c_->scheduler()->schedule(*this, result);
    else
        return -EINVAL;
}

void WaitQueue::push(Coroutine* c)
{
    assert(locked_);
    q_.emplace(c);
}

void WaitQueue::scheduleOne(int result)
{
    Lock lock(*this);
    while (!q_.empty()) {
        WaitItem item = std::move(q_.front());
        q_.pop();
        if (!item.schedule(result))
            return;
    }
}

void WaitQueue::scheduleAll(int result)
{
    Lock lock(*this);
    while (!q_.empty()) {
        q_.front().schedule(result);
        q_.pop();
    }
}

}} // namespace io::impl
