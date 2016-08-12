/**
 * lazy.h -- a wrapper for lazy evaluation
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

#include <syncio/syncio.h>
#include <atomic>
#include <functional>

template<class T>
class Lazy {
public:
    class Reference {
    public:
        Reference(const T* value, io::shared_lock<io::sys::shared_mutex> lock): value_(value), lock_(std::move(lock)) {}
        bool exists() const { return !!value_; }
        const T& value() const { return *value_; }
    private:
        const T* value_;
        io::shared_lock<io::sys::shared_mutex> lock_;
    };
    
    explicit Lazy(std::function<T()> f): hasValue_(false), f_(std::move(f)) {}
    
    void clear() { hasValue_ = false; }
    
    Reference get() const
    {
        for (;;) {
            {
                io::shared_lock<io::sys::shared_mutex> shlock(mutex_);
                if (hasValue_)
                    return Reference(&value_, std::move(shlock));
            }

            T t = f_();
            std::unique_lock<io::sys::shared_mutex> lock(mutex_);
            value_ = std::move(t);
            hasValue_ = true;
        }
    }
    
    Reference cached() const
    {
        io::shared_lock<io::sys::shared_mutex> shlock(mutex_);
        return Reference(hasValue_ ? &value_ : 0, std::move(shlock));
    }
    
    void assign(T t)
    {
        std::unique_lock<io::sys::shared_mutex> lock(mutex_);
        value_ = std::move(t);
        hasValue_ = true;        
    }
    
private:
    mutable T value_;
    mutable std::atomic<bool> hasValue_;
    std::function<T()> f_;
    mutable io::sys::shared_mutex mutex_;
};
