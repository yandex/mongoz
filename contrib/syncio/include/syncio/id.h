/**
 * id.h -- a task identifier
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

#include <functional>

namespace io {

namespace impl {
    class Coroutine;
    struct TaskHelper;
}

class task_id;

} // namespace io

namespace std {
template<>
struct hash<io::task_id> {
    typedef io::task_id argument_type;
    typedef size_t result_type;

    size_t operator()(const io::task_id& t) const;
};
} // namespace std


namespace io {

class task_id {
public:
    task_id(): c_(0) {}
    
    bool operator == (const task_id& id) const { return c_ == id.c_; }
    bool operator != (const task_id& id) const { return c_ != id.c_; }
    bool operator <  (const task_id& id) const { return c_ <  id.c_; }
    bool operator <= (const task_id& id) const { return c_ <= id.c_; }
    bool operator >  (const task_id& id) const { return c_ >  id.c_; }
    bool operator >= (const task_id& id) const { return c_ >= id.c_; }
    
private:
    impl::Coroutine* c_;
    
    explicit task_id(impl::Coroutine* c): c_(c) {}
    
    friend struct impl::TaskHelper;
    template<class T> friend struct std::hash;
};

} // namespace io

inline size_t std::hash<io::task_id>::operator()(const io::task_id& t) const
{
    return std::hash<void*>()(t.c_);
}
