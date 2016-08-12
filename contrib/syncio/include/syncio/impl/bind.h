/**
 * bind.h -- a replacement of std::bind(), which can capture moveable types
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

#include <utility>
#include <memory>

namespace io {
namespace impl {

template<class... Args> struct Tuple;

template<> struct Tuple<> {};

template<class HeadT, class... TailT>
struct Tuple<HeadT, TailT...> {
    typedef HeadT Head;
    typedef Tuple<TailT...> Tail;
    Head head;
    Tail tail;
};

inline Tuple<> make_tuple() { return Tuple<>{}; }

template<class Head, class... Tail>
inline Tuple<Head, Tail...> make_tuple(Head&& head, Tail&&... tail)
{
    return Tuple<Head, Tail...> {
        std::forward<Head>(head),
        make_tuple(std::forward<Tail>(tail)...)
    };
}


template<class Ret, class Callable, class Tuple>
struct Caller {    
    template<class... Args>
    static Ret call(Callable c, Tuple& t, Args&&... args)
    {
        return Caller<Ret, Callable, typename Tuple::Tail>::call(
            c, t.tail, std::forward<typename Tuple::Head>(t.head), std::forward<Args>(args)...);
    }
};

template<class Ret, class Callable>
struct Caller< Ret, Callable, Tuple<> > {
    template<class... Args>
    static Ret call(Callable c, Tuple<>&, Args&&... args)
    {
        return c(std::forward<Args>(args)...);
    }
};


template<class Ret>
class OneTimeFunction {
public:
    OneTimeFunction() {}
    
    template<class Callable, class... Args>
    /*implicit*/ OneTimeFunction(Callable c, Args&&... args):
        impl_(new Impl<Callable, Args...>(c, std::forward<Args>(args)...)) {}
    
    Ret operator()()
    {
        std::unique_ptr<ImplBase> tmp = std::move(impl_);
        impl_.reset(0);
        return (*tmp)();
    }
    
    explicit operator bool() const { return !!impl_.get(); }
        
private:
    struct ImplBase {
        virtual ~ImplBase() {}
        virtual Ret operator()() = 0;
    };
    
    template<class Callable, class... Args>
    struct Impl: public ImplBase {
        Callable c_;
        Tuple<Args...> args_;
        
        Impl(Callable c, Args&&... args): c_(c), args_(make_tuple(std::forward<Args>(args)...)) {}
        
        Ret operator()() override { return Caller< Ret, Callable, Tuple<Args...> >::call(c_, args_); }
    };
    
    std::unique_ptr<ImplBase> impl_;
};


}} // namespace io::impl
