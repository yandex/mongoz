/**
 * helper.h -- definition of TaskHelper, a bridge between syncio public
 *             interface and implementation
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

#include <syncio/id.h>
#include <syncio/task.h>

namespace io { namespace impl {

struct TaskHelper {
        
    static std::vector<Coroutine*> getImpls(const std::vector<TaskBase*>& tasks)
    {
        std::vector<Coroutine*> cs;
        cs.reserve(tasks.size());
        for (TaskBase* t: tasks)
            cs.push_back(t->c_);
        return cs;
    }
    
    static Coroutine* getImpl(TaskBase& t) { return t.c_; }
    
    static task_id id(Coroutine* c) { return task_id(c); }
    
};

}} // namespace io::impl
