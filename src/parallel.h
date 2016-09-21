/**
 * parallel.h -- a helper class simplifying early retransmissions
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

#include "error.h"
#include <syncio/syncio.h>
#include <type_traits>
#include <cerrno>


template<class T>
class TaskPool {
public:
    void add(io::task<T>& task) { tasks_.push_back(&task); }
    
    io::task<T>* wait(io::timeout timeout)
    {
        for (;;) {
            io::wait_any(tasks_, timeout);
            auto i = std::find_if(tasks_.begin(), tasks_.end(), [](io::impl::TaskBase* t) { return t->completed(); });
            if (i == tasks_.end())
                return 0;
            
            io::task<T>& t = *static_cast<io::task<T>*>(*i);
            *i = tasks_.back();
            tasks_.pop_back();
            
            if (t.succeeded())
                return &t;
            else if (t.failed()) {
                try { t.get(); }
                catch (errors::BackendClientError&) { return &t; }
                catch (std::exception&) {
                    if (tasks_.empty())
                        return &t;
                }
            }
        }
    }

    bool empty() const { return tasks_.empty(); }
    
private:
    std::vector<io::impl::TaskBase*> tasks_;
};
