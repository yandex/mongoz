/**
 * condvar.h -- conditional synchronization interface for syncio
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

#include "mutex.h"
#include "time.h"
#include <condition_variable>
#include <memory>
#include <mutex>

namespace io {
namespace impl { class CondVar; }

class condition_variable {
public:
    condition_variable();
    condition_variable(const condition_variable&) = delete;
    condition_variable& operator = (const condition_variable&) = delete;
    ~condition_variable();

    void notify_one();
    void notify_all();

    void wait(std::unique_lock<mutex>& lock);
    template<class Predicate>
    void wait(std::unique_lock<mutex>& lock, Predicate pred);

    std::cv_status wait_until(std::unique_lock<mutex>& lock, timeout timeout);
    template<class Predicate>
    bool wait_until(std::unique_lock<mutex>& lock, timeout timeout, Predicate pred);

    std::cv_status wait_for(std::unique_lock<mutex>& lock, timeout timeout);
    template<class Predicate>
    bool wait_for(std::unique_lock<mutex>& lock, timeout timeout, Predicate pred);

private:
    std::unique_ptr<impl::CondVar> impl_;
};

template<class Predicate>
void condition_variable::wait(std::unique_lock<mutex>& lock, Predicate pred)
{
    while (!pred()) {
        wait(lock);
    }
}

template<class Predicate>
bool condition_variable::wait_until(
        std::unique_lock<mutex>& lock,
        timeout timeout,
        Predicate pred)
{
    while (!pred()) {
        if (wait_until(lock, timeout) == std::cv_status::timeout){
            return pred();
        }
    }
    return true;
}

inline std::cv_status condition_variable::wait_for(
        std::unique_lock<mutex>& lock,
        timeout timeout)
{
    return wait_until(lock, timeout);
}

template<class Predicate>
bool condition_variable::wait_for(
        std::unique_lock<mutex>& lock,
        timeout timeout,
        Predicate pred)
{
    return wait_until(lock, timeout, pred);
}

} // namespace io
