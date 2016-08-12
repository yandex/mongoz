/**
 * clock.h -- a clock with guaranteed microseconds resolution
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

#include <time.h>
#include <chrono>

namespace impl {

template<int ClockType>
class Clock {
public:
    typedef std::chrono::microseconds duration;
    typedef std::chrono::microseconds::rep rep;
    typedef std::chrono::microseconds::period period;
    typedef std::chrono::time_point<Clock> time_point;
    static const bool is_steady = true;

    static time_point now()
    {
        struct timespec ts;
        clock_gettime(ClockType, &ts);
        return time_point(duration(ts.tv_sec * 1000000 + ts.tv_nsec / 1000));
    }
};

} // namespace impl

// A clock which is guaranteed to have microseconds resolution
typedef impl::Clock<CLOCK_REALTIME> WallClock;

// A clock with microseconds resolution not affected by any time adjustments.
// NB: SteadyClock's epoch starts at some undefined point, so don't use
// SteadyClock::now().time_since_epoch().
typedef impl::Clock<
#ifdef HAVE_CLOCK_MONOTONIC_RAW
        CLOCK_MONOTONIC_RAW_VALUE
#else
        CLOCK_MONOTONIC
#endif
    > SteadyClock;
    
