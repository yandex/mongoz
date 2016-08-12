/**
 * wait.h -- a syncio wait queue
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

#include <queue>
#include <functional>
#include <mutex>
#include <utility>

namespace io { namespace impl {

class Coroutine;
class Scheduler;

class WaitItem {
public:
    WaitItem(): c_(0), stamp_(0) {}
    explicit WaitItem(Coroutine* c);
    WaitItem(const WaitItem& it);
    WaitItem& operator = (WaitItem&& it);
    WaitItem(WaitItem&& it): c_(0), stamp_(0) { *this = std::move(it); }
    WaitItem& operator = (const WaitItem& it) { return *this = WaitItem(it); }
    ~WaitItem();
    
    Coroutine* coroutine() const { return c_; }
    size_t stamp() const { return stamp_; }

    int schedule(int result = 0);
    
private:
    Coroutine* c_;
    size_t stamp_;    
};

class WaitQueue {
public:
    void push(Coroutine* c);
    void scheduleOne(int result = 0);
    void scheduleAll(int result = 0);
    
    void lock() { mutex_.lock(); locked_ = true; }
    void unlock() { locked_ = false; mutex_.unlock(); }
    typedef std::unique_lock<WaitQueue> Lock;

private:
    std::queue<WaitItem> q_;
    std::mutex mutex_;
    bool locked_ = false;
};

}} // namespace io::impl
