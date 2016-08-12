/**
 * time.h -- a definiton of Timeout, used in many syncio operations
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

#ifdef HAVE_CONFIG_H
#    include <config.h>
#endif

#include <stddef.h>
#include <stdlib.h>
#include <chrono>
#include <limits>
#include <time.h>

namespace io {

class timeout {
public:
    timeout(): tm_(std::numeric_limits<ssize_t>::max()) {}
    
    template<class Rep, class Ratio>
    /*implicit*/ timeout(const std::chrono::duration<Rep, Ratio>& duration): tm_(
        duration == std::chrono::duration<Rep, Ratio>::max()
        ? std::numeric_limits<ssize_t>::max()
        : now() + std::chrono::duration_cast<std::chrono::microseconds>(duration).count()
    ){}
    
    template<class Clock, class Duration>
    /*implicit*/ timeout(const std::chrono::time_point<Clock, Duration>& timePoint):
        tm_(now() + std::chrono::duration_cast<std::chrono::microseconds>(timePoint - Clock::now()).count())
    {}
    
    ssize_t micro() const { return tm_; }
    bool finite() const { return *this != timeout(); }
    bool expired() const { return finite() && now() > tm_; }
    
    static ssize_t now()
    {
        struct timespec ts;
#ifdef HAVE_CLOCK_MONOTONIC_RAW
        clock_gettime(CLOCK_MONOTONIC_RAW_VALUE, &ts);
#else
        clock_gettime(CLOCK_MONOTONIC, &ts);
#endif
        return ts.tv_sec * 1000000 + ts.tv_nsec / 1000;
    }
    
    bool operator == (const timeout& tm) const { return tm_ == tm.tm_; }
    bool operator != (const timeout& tm) const { return tm_ != tm.tm_; }
    bool operator >  (const timeout& tm) const { return tm_ >  tm.tm_; }
    bool operator <  (const timeout& tm) const { return tm_ <  tm.tm_; }
    bool operator >= (const timeout& tm) const { return tm_ >= tm.tm_; }
    bool operator <= (const timeout& tm) const { return tm_ <= tm.tm_; }
    
private:
    ssize_t tm_;
};

} // namespace io

inline std::chrono::hours operator "" _h(unsigned long long x) { return std::chrono::hours(x); }
inline std::chrono::minutes operator "" _min(unsigned long long x) { return std::chrono::minutes(x); }
inline std::chrono::seconds operator "" _s(unsigned long long x) { return std::chrono::seconds(x); }
inline std::chrono::milliseconds operator "" _ms(unsigned long long x) { return std::chrono::milliseconds(x); }
inline std::chrono::microseconds operator "" _us(unsigned long long x) { return std::chrono::microseconds(x); }
