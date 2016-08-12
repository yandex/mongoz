/**
 * options.h -- mongoz command-line options
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

#include <bson/bson.h>
#include <thread>
#include <chrono>
#include <limits>

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#define FOREACH_CMDLINE_OPTION_BRK(option,brk) \
    \
         /* type, name, default value, help */ \
    \
    option( bool,                        auth,                   false, \
        "require authorization for clients" ) \
    \
    option( std::string,                 keyFile,                std::string(), \
        "file containing a shared secret between monogz and mongod" ) \
    \
    brk() \
    \
    option( std::chrono::milliseconds,   localThreshold,         10, \
        "estimated ping between two local backends" ) \
    \
    option( std::chrono::milliseconds,   maxReplLag,             std::chrono::milliseconds::max(), \
        "ignore replicas whose lag exceeds specified value" ) \
    \
    brk() \
    \
    option( std::chrono::milliseconds,   readTimeout,            std::chrono::milliseconds::max(), \
        "default timeout for queries" ) \
    \
    option( std::chrono::milliseconds,   writeTimeout,           std::chrono::milliseconds::max(), \
        "default timeout for inserts/updates/deletes" ) \
    \
    option( std::chrono::milliseconds,   readRetransmit,         std::chrono::milliseconds::max(), \
        "default retransmit interval for queries" ) \
    \
    option( std::chrono::milliseconds,   writeRetransmit,        std::chrono::milliseconds::max(), \
        "default retransmit interval for inserts/updates/deletes" ) \
    \
    brk() \
    \
    option( std::chrono::milliseconds,   pingTimeout,            500, \
        "timeout for backend pings" ) \
    \
    option( std::chrono::milliseconds,   pingInterval,           10000, \
        "ping interval for alive backends" ) \
    \
    option( std::chrono::milliseconds,   pingFailInterval,       2000, \
        "ping interval for dead backends" ) \
    \
    brk() \
    \
    option( std::chrono::milliseconds,   confTimeout,            1000, \
        "timeout for config servers" ) \
    \
    option( std::chrono::milliseconds,   confRetransmit,         20, \
        "retransmit interval for config servers" ) \
    \
    option( std::chrono::milliseconds,   confInterval,           10000, \
        "poll interval for config servers" ) \
    \
    brk() \
    \
    option( std::chrono::milliseconds,   monitorNoPrimary,       std::chrono::milliseconds::max(), \
        "maximal primary node election time before triggering an event" ) \
    \
    option( std::chrono::milliseconds,   monitorConfigAge,       std::chrono::milliseconds::max(), \
        "maximal shard config age before triggering an event" ) \
    \
    brk() \
    \
    option( bool,                        globalCursors,          false, \
        "make all cursor IDs global" ) \
    \
    option( size_t,                      connPoolSize,           std::thread::hardware_concurrency(), \
        "maintain N persistent connection per backend" ) \
    \
    option( size_t,                      threads,                std::thread::hardware_concurrency(), \
        "spawn N threads" ) \
    \
    option( bool,                        readOnly,               false, \
        "forbid all writes through this server" ) \




#define CMDLINE_NO_BRK()
#define FOREACH_CMDLINE_OPTION(option) FOREACH_CMDLINE_OPTION_BRK(option, CMDLINE_NO_BRK)
        
struct Options {
    
#define DEFINE_OPTION(type, name, dfltValue, _) \
        type name { dfltValue };
    
    FOREACH_CMDLINE_OPTION(DEFINE_OPTION)

#undef DEFINE_OPTION
};

const Options& options();

const bson::ObjectID& serverID();


#ifdef CPUPROFILE
#    define PROFILE_CPU_CMDLINE_OPTION(option) \
        option(std::string, profileCpu, "")
#else
#    define PROFILE_CPU_CMDLINE_OPTION(option)
#endif

#define FOREACH_DEBUG_OPTION(option) \
    option(bool,         enable,          false) \
    option(size_t,       batchSize,       static_cast<size_t>(-1)) \
    option(bool,         detailedTimings, false) \
    PROFILE_CPU_CMDLINE_OPTION(option) \

struct DebugOptions {
#define DEFINE_OPTION(type,name,dfltValue) \
        type name { dfltValue };
    FOREACH_DEBUG_OPTION(DEFINE_OPTION)
#undef DEFINE_OPTION
};

const DebugOptions& debugOptions();
