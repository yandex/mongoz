/**
 * main.cpp -- a main entry point
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

#include "session.h"
#include "log.h"
#include "config.h"
#include "options.h"
#include "auth.h"
#include "cache.h"
#include "utility.h"
#include <syncio/syncio.h>
#include <string>
#include <iostream>
#include <vector>
#include <list>
#include <thread>
#include <stdint.h>
#include <stdlib.h>
#include <signal.h>
#include <unistd.h>
#include <getopt.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>

#ifdef CPUPROFILE
#    include <google/profiler.h>
#endif

const bson::ObjectID& serverID() {
    static const bson::ObjectID id = bson::ObjectID::generate();
    return id;
}


Options g_options;
const Options& options() { return g_options; }

DebugOptions g_debugOptions;
const DebugOptions& debugOptions() { return g_debugOptions; }

void listener(io::fd fd)
{
    for (;;) {
        try {
            io::fd fd2 = fd.accept();
            DEBUG(1) << "Accepted a connection on fd " << fd2.get();
            if (fd)
                Session::handle(std::move(fd2));
        }
        catch (std::exception& e) {
            WARN() << "cannot accept a new connection: " << e.what();
            io::sleep(10_ms);
        }
    }
}


template<class T>
void parseCmdArg(const std::string& /*paramName*/, const std::string& s, T& dest) { dest = fromString<T>(s); }

void parseCmdArg(const std::string& /*paramName*/, const std::string& s, bool& dest)
{
    if (s == "yes" || s == "1" || s == "true" || s.empty())
        dest = true;
    else if (s == "no" || s == "0" || s == "false")
        dest = false;
    else
        throw std::runtime_error("cannot parse `" + s + "' as bool");
}


template<class DestRep, class DestPeriod, class SrcRep, class SrcPeriod>
void checkedAssign(
    const std::string& paramName,
    std::chrono::duration<DestRep, DestPeriod>& dest,
    const std::chrono::duration<SrcRep, SrcPeriod>& src
){
    typedef std::chrono::duration<DestRep, DestPeriod> Dest;
    if (src.count() * SrcPeriod::num * DestPeriod::den < DestPeriod::num * SrcPeriod::den)
        throw std::runtime_error("`" + paramName + "' value (" + toString(src)
            + ") less than parameter resolution (" + toString(Dest(1)) + ")");
    
    dest = std::chrono::duration_cast<Dest>(src);
}

template<class Rep, class Period>
void parseCmdArg(const std::string& paramName, const std::string& s, std::chrono::duration<Rep, Period>& dest)
{
    typedef std::chrono::duration<Rep, Period> DestType;
    
    if (s == "inf") {
        dest = DestType::max();
        return;
    }
        
    size_t i = 0;
    while (i != s.size() && isdigit(s[i]))
        ++i;
    size_t val = fromString<size_t>(s.substr(0, i));
    std::string dim = s.substr(i);
    if (dim == "min" || dim == "m")
        checkedAssign(paramName, dest, std::chrono::duration_cast<DestType>(std::chrono::minutes(val)));
    else if (dim == "s")
        checkedAssign(paramName, dest, std::chrono::duration_cast<DestType>(std::chrono::seconds(val)));
    else if (dim == "ms")
        checkedAssign(paramName, dest, std::chrono::duration_cast<DestType>(std::chrono::milliseconds(val)));
    else if (dim == "us")
        checkedAssign(paramName, dest, std::chrono::duration_cast<DestType>(std::chrono::microseconds(val)));
    else
        throw std::runtime_error("cannot parse `" + s + "' as time; use `<num>(min|s|ms|us)' format");
}


template<class T> const char* helpDesc();

template<> const char* helpDesc<size_t>() { return "<n>"; }
template<> const char* helpDesc<bool>() { return "<yes|no>"; }
template<> const char* helpDesc<std::string>() { return "<str>"; }
template<> const char* helpDesc<std::chrono::milliseconds>() { return "<duration>"; }

/// Translates string "likeThis" into "like-this".
std::string toCmdlineOpt(const std::string& optname)
{
    std::string ret;
    for (char c: optname) {
        if (isupper(c)) {
            ret.push_back('-');
            ret.push_back(tolower(c));
        } else {
            ret.push_back(c);
        }
    }
    return ret;
}

void usage() __attribute__((noreturn));
void usage()
{
    std::cerr << R"(Usage: mongoz -c <config> -l <listen_on> [<options>...]

  -c, --config-servers <host>:<port>[,...]  config servers to use (required)
  -l, --listen [<host>:]<port>              host/port to listen on (required)
  -C, --config-cache <file>                 cache server config in the specified file

  -v, --verbose                             increase specified logging level
  -L, --log </path/to/file>                 filename to write log to
  -S, --syslog <ident>                      send logs to syslogd(8) with this identifier
  -d, --daemonize <pidfile>                 fork to background and create this pidfile

)";
    
    std::vector< std::pair<std::string, std::string> > helptext;
    
#define OPTION_HELP(type,name,_,help)  \
    helptext.push_back(std::make_pair("      --" + toCmdlineOpt(#name) + " " + helpDesc<type>(), help));
#define OPTION_BRK() \
    helptext.push_back(std::make_pair("", ""));
    FOREACH_CMDLINE_OPTION_BRK(OPTION_HELP, OPTION_BRK);
#undef OPTION_HELP
#undef OPTION_BRK
    
    size_t leftwidth = 41;
    for (const auto& p: helptext)
        leftwidth = std::max(leftwidth, p.first.size());
    
    for (const auto& p: helptext) {
        std::cerr << p.first
                  << std::string(leftwidth + 3 - p.first.size(), ' ') << p.second << std::endl;
    }
        
    exit(1);
}

void ignoreErrors(int) {}

void daemonize(const std::string& pidfile)
{
    int devnull = open("/dev/null", O_RDWR);
    if (devnull == -1)
        throw std::runtime_error(std::string("cannot open /dev/null: ") + strerror(errno));

    std::ofstream f(pidfile.c_str());
    if (!f)
        throw std::runtime_error(std::string("cannot create pidfile: ") + strerror(errno));

    if (fork())
        _exit(1);
    setsid();
    if (fork())
        _exit(1);
    
    f << getpid() << std::endl;
    
    ignoreErrors(chdir("/"));
    ignoreErrors(dup2(devnull, 0));
    ignoreErrors(dup2(devnull, 1));
    ignoreErrors(dup2(devnull, 2));
    close(devnull);
    umask(022);
}

int main(int argc, char** argv)
{
    ::signal(SIGPIPE, SIG_IGN);
    
    std::list<std::thread> threads;
    
    io::engine engine;
    engine.spawn([&]{

        try {
            int loglevel = LogMessage::NOTICE;
            std::string logpath = "";
            std::string logident = "";
            std::string configServers;
            std::string configCache;
            std::vector<io::addr> listenOn;
            std::string pidfile;
            
            std::vector<struct option> opts {
                { "listen",             1, 0, 'l' },
                { "config-servers",     1, 0, 'c' },
                { "config-cache",       1, 0, 'C' },
                
                { "logfile",            1, 0, 'L' },
                { "syslog",             1, 0, 'S' },
                { "verbose",            0, 0, 'v' },
                { "daemonize",          1, 0, 'd' },
                
                { "debug-option",       1, 0, 'D' }
            };
            std::vector<std::string> optstrings;

#define DESCRIBE_OPTION(type, name, _2, _3) \
            { \
                optstrings.push_back(toCmdlineOpt(#name)); \
                const char* cname = optstrings.back().c_str(); \
                opts.push_back(option { cname, std::is_same<type, bool>::value ? 2 : 1, 0, 0 }); \
            };
            
            FOREACH_CMDLINE_OPTION(DESCRIBE_OPTION);
#undef DESCRIBE_OPTION
            
            opts.push_back(option { 0, 0, 0, 0 });
            
            std::string optstring;
            for (const struct option& opt: opts) {
                if (!isalnum(opt.val))
                    continue;
                optstring.push_back(opt.val);
                if (opt.has_arg)
                    optstring.push_back(':');
            }
            
            int opt;
            int longidx;
            while ((opt = getopt_long(argc, argv, optstring.c_str(), opts.data(), &longidx)) != -1) {
                if (opt == 'c') {
                    configServers = optarg;
                } else if (opt == 'C') {
                    configCache = optarg;
                } else if (opt == 'l') {
                    std::vector<io::addr> addrs = io::resolve(optarg, io::resolve_mode::PASSIVE);
                    listenOn.insert(listenOn.end(), addrs.begin(), addrs.end());
                } else if (opt == 'v') {
                    ++loglevel;
                } else if (opt == 'L') {
                    logpath = optarg;
                } else if (opt == 'S') {
                    logident = optarg;
                } else if (opt == 'd') {
                    pidfile = optarg;
                } else if (opt == 'D') {
                    std::string arg = optarg;
                    size_t eq = arg.find('=');
                    std::string k, v;
                    if (eq != std::string::npos) {
                        k = arg.substr(0, eq);
                        v = arg.substr(eq + 1);
                    } else {
                        k = arg;
                    }
                    if (false) {}
#define PARSE_OPTION(_1, optname, _2) \
                    else if (k == toCmdlineOpt(#optname)) \
                        parseCmdArg(toCmdlineOpt(#optname), v, g_debugOptions.optname);
                    FOREACH_DEBUG_OPTION(PARSE_OPTION)
#undef PARSE_OPTION
                    else
                        throw std::runtime_error("unknown debug option: " + k);
                } else if (opt == 0) {
                    if (false) {}
#define PARSE_OPTION(_1, optname, _2, _3) \
                    else if (std::string(opts[longidx].name) == toCmdlineOpt(#optname)) \
                        parseCmdArg(toCmdlineOpt(#optname), optarg ? optarg : std::string(), g_options.optname);
                    FOREACH_CMDLINE_OPTION(PARSE_OPTION)
#undef PARSE_OPTION
                    else
                        usage();
                } else {
                    usage();
                }
            }
            
            if (optind != argc || configServers.empty() || listenOn.empty())
                usage();
            
            if (!logident.empty())
                Logger::instance() = new LogToSyslog(loglevel, logident);
            else if (!logpath.empty())
                Logger::instance() = new LogToFile(loglevel, logpath);
            else
                Logger::instance() = new LogToNowhere;

            NOTICE() << "starting mongoz";
            
#ifdef CPUPROFILE
            if (!debugOptions().profileCpu.empty())
                ProfilerStart(debugOptions().profileCpu.c_str());
#endif

            if (options().globalCursors)
                CursorStoragePolicy::current().reset(new CursorStoragePolicy::GlobalCursors);
            else
                CursorStoragePolicy::current().reset(new CursorStoragePolicy::LocalCursors);
            
            g_cache.reset(new Cache(configCache));
            
            if (!options().keyFile.empty())
                auth::loadSecret(options().keyFile);
                                                            
            g_config.reset(new ConfigHolder(configServers));

            for (const io::addr& addr: listenOn)
                io::spawn(&listener, io::listen(addr)).detach();
            
            if (options().auth)
                io::spawn([]{ auth::CredentialsCache::instance().keepUpdating(); }).detach();
            
            if (!pidfile.empty())
                daemonize(pidfile);
                        
            for (size_t i = 1; i < options().threads; ++i)
                threads.emplace_back([&engine]{ engine.run(); });
        }
        catch (std::exception& e) {
            std::cerr << "mongoz: " << e.what() << std::endl;
            ERROR() << e.what() << "; mongoz stopped";
            exit(1);
        }
        
    }).detach();
    
    engine.run();
    
    for (std::thread& th: threads)
        th.join();
    return 0;
}
