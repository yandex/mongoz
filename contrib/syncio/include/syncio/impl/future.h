/**
 * future.h -- a lightweight std::packaged_task<> replacement, which
 *             can handle moveable types
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

#include "bind.h"
#include <exception>
#include <cassert>

namespace io {
namespace impl {

template<class T>
class PackagedTask {
public:
    explicit PackagedTask(OneTimeFunction<T> f): func_(std::move(f)), mode_(NOTHING) {}
    
    ~PackagedTask()
    {
        if (mode_ == VALUE)
            result_.value.~T();
        else if (mode_ == EXCEPTION)
            result_.ex.~exception_ptr();
    }
        
    bool succeeded() const { return mode_ == VALUE; }
    bool failed() const { return mode_ == EXCEPTION; }
    
    T get()
    {
        if (mode_ == VALUE)
            return std::move(result_.value);
        else if (mode_ == EXCEPTION)
            std::rethrow_exception(result_.ex);
        else
            assert(!"future not ready");
    }
    
    void run()
    {
        try { setValue(func_()); }
        catch (std::exception&) { setException(std::current_exception()); }
    }
    
private:
    OneTimeFunction<T> func_;
    enum { NOTHING, VALUE, EXCEPTION } mode_;
    union Union {        
        T value;
        std::exception_ptr ex;
        
        Union() {}
        ~Union() {}
    };
    Union result_;
    
    
    void setValue(T t)
    {
        assert(mode_ == NOTHING);
        new (&result_.value) T(std::move(t));
        mode_ = VALUE;
    }
    
    void setException(std::exception_ptr ex)
    {
        assert(mode_ == NOTHING);
        new (&result_.ex) std::exception_ptr(std::move(ex));
        mode_ = EXCEPTION;
    }

};


template<>
class PackagedTask<void> {
public:
    explicit PackagedTask(OneTimeFunction<void> f): func_(std::move(f)), mode_(NOTHING) {}
    
    ~PackagedTask()
    {
        if (mode_ == EXCEPTION)
            result_.ex.~exception_ptr();
    }
        
    bool succeeded() const { return mode_ == VALUE; }
    bool failed() const { return mode_ == EXCEPTION; }
    
    void get()
    {
        if (mode_ == VALUE)
            return;
        else if (mode_ == EXCEPTION)
            std::rethrow_exception(result_.ex);
        else
            assert(!"future not ready");
    }
    
    void run()
    {
        try {
            func_();
            setValue();
        }
        catch (std::exception&) { setException(std::current_exception()); }
    }
    
private:
    OneTimeFunction<void> func_;
    enum { NOTHING, VALUE, EXCEPTION } mode_;
    union Union {        
        std::exception_ptr ex;
        
        Union() {}
        ~Union() {}
    };
    Union result_;
    
    
    void setValue()
    {
        assert(mode_ == NOTHING);
        mode_ = VALUE;
    }
    
    void setException(std::exception_ptr ex)
    {
        assert(mode_ == NOTHING);
        new (&result_.ex) std::exception_ptr(std::move(ex));
        mode_ = EXCEPTION;
    }
};



} // namespace impl
} // namespace io
