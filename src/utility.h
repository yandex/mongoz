/**
 * utility.h -- a few helper functions
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
#include <string>
#include <sstream>
#include <typeinfo>
#include <chrono>
#include <cxxabi.h>

inline std::string demangledType(const std::type_info& ti)
{
    int status;
    char* cstr = abi::__cxa_demangle(ti.name(), 0, 0, &status);
    try {        
        std::string s(status ? ti.name() : cstr);
        if (cstr) {
            free(cstr);
            cstr = 0;
        }
        return s;
    }
    catch (...) {
        if (cstr)
            free(cstr);
        throw;
    }
}

template<class T>
inline std::string toString(const T& t)
{
    std::ostringstream s;
    if (s << t)
        return s.str();
    else
        throw std::runtime_error("toString() failed");
}

template<class Period> inline const char* timeSuffix();
template<> inline const char* timeSuffix<std::chrono::minutes::period>() { return "min"; }
template<> inline const char* timeSuffix<std::chrono::seconds::period>() { return "s"; }
template<> inline const char* timeSuffix<std::chrono::milliseconds::period>() { return "ms"; }
template<> inline const char* timeSuffix<std::chrono::microseconds::period>() { return "us"; }

template<class Rep, class Period>
inline std::string toString(const std::chrono::duration<Rep, Period>& dur)
{
    return toString(dur.count()) + timeSuffix<Period>();
}

template<class T>
inline T fromString(const std::string& str)
{
    std::istringstream s(str);
    T t;
    if (s >> t)
        return t;
    else
        throw std::runtime_error("cannot parse `" + str + "' as " + demangledType(typeid(T)));
}

template<class T, class... Args>
std::unique_ptr<T> make_unique(Args&&... args)
{
    return std::unique_ptr<T>(new T(std::forward<Args>(args)...));
}
