/**
 * log.h -- logging facilities
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

#include "clock.h"
#include <syncio/syncio.h>
#include <syncio/debug.h>
#include <sstream>
#include <fstream>
#include <cstring>
#include <mutex>
#include <syslog.h>

class LogMessage {
public:
    
    enum { NONE = -4, ERROR = -3, WARN = -2, NOTICE = -1, INFO = 0, DEBUG = 1 };
    
    explicit LogMessage(int level);
    ~LogMessage();
    
    template<class T>
    LogMessage& operator << (T&& t) { s_ << std::forward<T>(t); return *this; }
    
    bool enabled() const { return enabled_; }
    WallClock::time_point time() const { return tm_; }
    int level() const { return level_; }
    std::string text() const { return s_.str(); }
    
private:
    WallClock::time_point tm_;
    int level_;
    bool enabled_;
    std::stringstream s_;
};


class Logger {
public:
    virtual ~Logger() {}
    static Logger*& instance() { static Logger* p = 0; return p; }

    int maxLevel() const { return maxLevel_; }
    void setMaxLevel(int level) { maxLevel_ = level; }
    void put(const LogMessage& msg) { doPut(msg); }
    
protected:
    explicit Logger(int maxLevel): maxLevel_(maxLevel) {}

private /*methods*/:
    virtual void doPut(const LogMessage& msg) = 0;

private /*fields*/:
    int maxLevel_;
};


inline LogMessage::LogMessage(int level):
    level_(level),
    enabled_(Logger::instance() && (level <= Logger::instance()->maxLevel()))
{
    if (enabled_)
        tm_ = WallClock::now();
}

inline LogMessage::~LogMessage()
{
    io::log(s_.str());
    if (enabled_)
        Logger::instance()->put(*this);
}


class LogToNowhere: public Logger {
public:
    LogToNowhere(): Logger(LogMessage::NONE) {}
    void doPut(const LogMessage&) override {}
};


class LogToFile: public Logger {
public:
    explicit LogToFile(int maxLevel, const std::string& filename): Logger(maxLevel), f_(filename.c_str())
    {
        if (!f_)
            throw std::runtime_error("cannot open " + filename);
    }
    
    void doPut(const LogMessage& msg) override
    {
        std::ostringstream s;
        size_t milli = std::chrono::duration_cast<std::chrono::milliseconds>(msg.time().time_since_epoch()).count();
        
        int tz = abs(::timezone);
        char stime[64];
        struct tm tm;
        time_t time = milli / 1000;
        strftime(stime, sizeof(stime), "%Y-%m-%d %H:%M:%S", localtime_r(&time, &tm));
        char* p = stime + strlen(stime);
        snprintf(p, stime + sizeof(stime) - p, ".%03d %c%02d%02d",
            (int) (milli % 1000),
            (::timezone <= 0) ? '+' : '-',
            tz / 3600, (tz % 3600) / 60
        );
        
        s << stime << " " << level(msg.level()) << ": " << msg.text() << "\n";
        
        std::unique_lock<io::sys::mutex> lock(mutex_);
        f_ << s.str() << std::flush;
    }
    
private:
    std::ofstream f_;
    io::sys::mutex mutex_;
    
    static std::string level(int lvl)
    {
        if (lvl == LogMessage::ERROR)
            return "error";
        else if (lvl == LogMessage::WARN)
            return "warn";
        else if (lvl == LogMessage::INFO || lvl == LogMessage::NOTICE)
            return "info";
        else
            return "debug(" + std::to_string(lvl) + ")";
    }
};


class LogToSyslog: public Logger {
public:
    LogToSyslog(int maxLevel, std::string ident):
        Logger(maxLevel), ident_(std::move(ident))
    { openlog(ident_.c_str(), LOG_NDELAY | LOG_PID, LOG_USER); }
    
    ~LogToSyslog() { closelog(); }
    
    void doPut(const LogMessage& msg) override
    {
        if (msg.enabled()) {
            syslog(priority(msg.level()), "%s", msg.text().c_str());
        }
    }
    
private:
    std::string ident_;
    
    static int priority(int lvl)
    {
        if (lvl == LogMessage::ERROR)
            return LOG_ERR;
        else if (lvl == LogMessage::WARN)
            return LOG_WARNING;
        else if (lvl == LogMessage::INFO || lvl == LogMessage::NOTICE)
            return LOG_INFO;
        else
            return LOG_DEBUG;
    }
};


#define ERROR()  ::LogMessage(::LogMessage::ERROR)
#define WARN()   ::LogMessage(::LogMessage::WARN)
#define NOTICE() ::LogMessage(::LogMessage::NOTICE)
#define INFO()   ::LogMessage(::LogMessage::INFO)
#define DEBUG(x) ::LogMessage(x)
