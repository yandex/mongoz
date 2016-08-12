/**
 * ordering.h -- a few helpers simplifying definition of totally-ordered types
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

namespace bson {
namespace impl {

struct OrderedBase {
    template<template<class> class Cmp, class T>
    static bool cmp(const T& a, const T& b)
    {
        return T::template cmp<Cmp>(a, b);
    }
};

template<class T>
struct Ordered {
    friend bool operator == (const T& lhs, const T& rhs) { return OrderedBase::cmp<std::equal_to     >(lhs, rhs); }
    friend bool operator != (const T& lhs, const T& rhs) { return OrderedBase::cmp<std::not_equal_to >(lhs, rhs); }
    friend bool operator <  (const T& lhs, const T& rhs) { return OrderedBase::cmp<std::less         >(lhs, rhs); }
    friend bool operator <= (const T& lhs, const T& rhs) { return OrderedBase::cmp<std::less_equal   >(lhs, rhs); }
    friend bool operator >  (const T& lhs, const T& rhs) { return OrderedBase::cmp<std::greater      >(lhs, rhs); }
    friend bool operator >= (const T& lhs, const T& rhs) { return OrderedBase::cmp<std::greater_equal>(lhs, rhs); }
};

template<class T>
struct Equal {
    friend bool operator == (const T&, const T&) { return true;  }
    friend bool operator != (const T&, const T&) { return false; }
    friend bool operator <  (const T&, const T&) { return false; }
    friend bool operator <= (const T&, const T&) { return true;  }
    friend bool operator >  (const T&, const T&) { return false; }
    friend bool operator >= (const T&, const T&) { return true;  }
};

} // namespace impl
} // namespace bson
