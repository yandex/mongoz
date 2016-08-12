/**
 * tls.h -- task-local storage
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
#include <vector>
#include <cassert>
#include <cstdlib>
#include <stdint.h>

namespace io {
namespace impl {

class TLS {
public:
    template<class T> static size_t add() { return doAdd(sizeof(T)); }
    
    TLS(): data_(size(), '\0') {}
    
    ~TLS()
    {
        for (size_t id: objects_) {
            Header& h = *reinterpret_cast<Header*>(data_.data() + id);
            void* obj = data_.data() + id + sizeof(Header);
            if (h.destructor)
                (*h.destructor)(obj);
        }
    }
    
    template<class T>
    T& get(size_t id)
    {
        assert(id + sizeof(Header) + sizeof(T) <= data_.size());
        
        Header& h = *reinterpret_cast<Header*>(data_.data() + id);
        T& ret = *reinterpret_cast<T*>(data_.data() + id + sizeof(Header));
        
        if (!h.destructor) {
            // ensure push_back will not throw
            objects_.push_back(id);
            objects_.pop_back();

            new(&ret) T();
            h.destructor = &TLS::destruct<T>;
            objects_.push_back(id);
        }
        return ret;
    }
    
private:
    struct Header {
        void (*destructor)(void*);
    };
    
    template<class T> static void destruct(void* p) { reinterpret_cast<T*>(p)->~T(); }
    static size_t size();
    
    std::vector<char> data_;
    std::vector<size_t> objects_;

    static size_t doAdd(size_t objsize);
};

} // namespace impl
} // namespace io
