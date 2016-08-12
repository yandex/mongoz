/**
 * algorithm.h -- a collection of parallel I/O alporitms
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

#include "task.h"

namespace io {

template<class Iter, class Callable>
inline void for_each(Iter begin, Iter end, Callable c)
{
    std::vector< task<void> > tasks;
    for (; begin != end; ++begin)
        tasks.push_back(spawn(c, *begin));
    for (task<void>& t: tasks)
        wait(t);
    for (task<void>& t: tasks)
        t.get();
}

template<class Iter1, class Iter2, class Callable>
inline Iter2 transform(Iter1 begin, Iter1 end, Iter2 dest, Callable c)
{
    typedef decltype(c(*begin)) Value;
    std::vector< task<Value> > tasks;
    for (; begin != end; ++begin)
        tasks.push_back(spawn(c, *begin));
    for (task<Value>& t: tasks)
        wait(t);
    for (task<Value>& t: tasks) {
        *dest = t.get();
        ++dest;
    }
    return dest;
}

}
