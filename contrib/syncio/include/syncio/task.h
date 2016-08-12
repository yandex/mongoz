/**
 * task.h -- a definition of task, the main I/O scheduling unit.
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

#include "id.h"
#include "impl/utility.h"
#include "impl/bind.h"
#include "impl/future.h"
#include <utility>
#include <exception>
#include <functional>
#include <vector>

namespace io {

namespace impl {

class Coroutine;
struct TaskHelper;

Coroutine* spawn(std::function<void()> start, size_t stackSize);

class TaskBase {
public:
    TaskBase(): c_(0) {}
    TaskBase(const TaskBase&) = delete;
    TaskBase& operator = (const TaskBase&) = delete;
    TaskBase(TaskBase&& t): c_(0) { *this = std::move(t); }
    TaskBase& operator = (TaskBase&& t) { std::swap(c_, t.c_); return *this; }
    ~TaskBase();
    
    bool empty() const { return !c_; }
    explicit operator bool() const { return !empty(); }
    
    bool completed() const;
    
    task_id id() const;
    
    void detach();
    void cancel();
    void cancel(std::exception_ptr ex);
    
    TaskBase& rename(const std::string& name);

protected:
    impl::Coroutine* c_;
    
    friend struct impl::TaskHelper;    
};

} // namespace impl


int wait_any(const std::vector<impl::TaskBase*>& tasks, timeout t = timeout());
int wait_all(const std::vector<impl::TaskBase*>& tasks, timeout t = timeout());

template<class... Args>
int wait_any(impl::TaskBase& first, Args&&... rest)
{
    std::vector<impl::TaskBase*> v;
    timeout t = impl::populateVector(v, first, std::forward<Args>(rest)...);
    return wait_any(v, t);
}

template<class... Args>
int wait_all(impl::TaskBase& first, Args&&... rest)
{
    std::vector<impl::TaskBase*> v;
    timeout t = impl::populateVector(v, first, std::forward<Args>(rest)...);
    return wait_all(v, t);
}

inline int wait(impl::TaskBase& task, timeout t = timeout()) { return wait_all(task, t); }

template<class T>
class task: public impl::TaskBase {
public:
    task(): TaskBase() {}

    bool succeeded() const { return completed() && !!body_ && body_->succeeded(); }
    bool failed() const { return completed() && !!body_ && body_->failed(); }
    
    T get()
    {
        assert(completed() && !!body_);
        return body_->get();
    }
    
    static task make(impl::OneTimeFunction<T> start, size_t stackSize)
    {
        task t;
        std::shared_ptr< impl::PackagedTask<T> > body(new impl::PackagedTask<T>(std::move(start)));
        t.body_ = body;
        t.c_ = impl::spawn([body]{ body->run(); }, stackSize);
        return t;
    }

    T join()
    {
        io::wait(*this);
        return get();
    }
    
private:
    std::shared_ptr< impl::PackagedTask<T> > body_;
};

template<class T>
task<T> doSpawn(impl::OneTimeFunction<T> start, size_t stackSize = 0)
{
    return task<T>::make(std::move(start), stackSize);
}

template<class Function, class... Args>
auto spawn(Function&& f, Args&&... args) -> task<decltype(f(std::forward<Args>(args)...))>
{
    typedef decltype(f(std::forward<Args>(args)...)) Ret;
    return doSpawn(impl::OneTimeFunction<Ret>(f, std::forward<Args>(args)...));
}


void sleep(timeout t);

task_id current_task();

} // namespace io

