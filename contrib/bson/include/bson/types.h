/**
 * types.h -- a collecton of BSON-related types
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

#include "ordering.h"
#include <cstring>
#include <cstdint>
#include <string>
#include <iosfwd>

namespace bson {

class Null: public impl::Equal<Null> {};

class ObjectID: public impl::Ordered<ObjectID> {
public:
    ObjectID() { ::memset(data_, 0, DataSize); }

    ObjectID(const char* hex);

    ObjectID(const std::string& hex);

    std::string toString() const;

    time_t getTimestamp() const;

    static ObjectID generate();

    static ObjectID minIdForTimestamp(time_t);

    static ObjectID maxIdForTimestamp(time_t);

private:
    ObjectID(uint32_t time, uint64_t counter);

    enum { DataSize = 12 };
    union {
        struct {
            uint32_t time_;
            uint64_t counter_;
        } __attribute__((packed));
        char data_[DataSize];
    };
    
    friend class impl::OrderedBase;
    template<template<class> class Cmp>
    static bool cmp(const ObjectID& a, const ObjectID& b) { return Cmp<int>()(::memcmp(a.data_, b.data_, DataSize), 0); }
    
    friend std::ostream& operator << (std::ostream& out, const ObjectID&);
};

struct MinKey: public impl::Equal<MinKey> {};
struct MaxKey: public impl::Equal<MaxKey> {};

class Time: public impl::Ordered<Time> {
public:    
    explicit Time(time_t t): milli_(t * 1000) {}
    explicit Time(const struct timeval& tv): milli_(tv.tv_sec*1000 + tv.tv_usec/1000) {}
    
    int64_t milliseconds() const { return milli_; }

private:
    int64_t milli_;
    
    friend class impl::OrderedBase;
    template<template<class> class Cmp>
    static bool cmp(const Time& a, const Time& b) { return Cmp<int64_t>()(a.milliseconds(), b.milliseconds()); }
};

std::ostream& operator << (std::ostream& out, const Time& t);

class Timestamp: public impl::Ordered<Timestamp> {
public:
    Timestamp() { ts_ = 0; }
    Timestamp(uint32_t a, uint32_t b) { first_ = a; second_ = b; }
    
    uint32_t first() const { return first_; }
    uint32_t second() const { return second_; }
    
private:    
    union {
        struct {
            uint32_t second_;
            uint32_t first_;
        } __attribute__((packed));
        uint64_t ts_;
    };

    friend class impl::OrderedBase;
    template<template<class> class Cmp>
    static bool cmp(const Timestamp& a, const Timestamp& b) { return Cmp<uint64_t>()(a.ts_, b.ts_); }
};

std::ostream& operator << (std::ostream& out, const Timestamp&);


struct Any: public impl::Equal<Any> {};
std::ostream& operator << (std::ostream& out, const Any&);


} // namespace bson
