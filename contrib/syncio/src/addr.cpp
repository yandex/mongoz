/**
 * addr.cpp -- implementation of address-to-name and name-to-address resolving
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

#include <syncio/addr.h>
#include <syncio/error.h>
#include <syncio/task.h>
#include <sys/un.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <stdexcept>
#include <sstream>

namespace io {

std::ostream& operator << (std::ostream& out, const addr& addr)
{
    if (addr.af() == AF_UNIX) {
        return out << addr.as<struct sockaddr_un>()->sun_path;
    } else if (addr.af() == AF_INET) {
        char buf[INET_ADDRSTRLEN + 1];
        const struct sockaddr_in* a = addr.as<struct sockaddr_in>();
        inet_ntop(addr.af(), &a->sin_addr, buf, sizeof(buf));
        return out << buf << ":" << ntohs(a->sin_port);
    } else if (addr.af() == AF_INET6) {
        char buf[INET6_ADDRSTRLEN + 1];
        const struct sockaddr_in6* a = addr.as<struct sockaddr_in6>();
        inet_ntop(addr.af(), &a->sin6_addr, buf, sizeof(buf));
        return out << "[" << buf << "]:" << htons(a->sin6_port);
    } else {
        return out << "???";
    }
}

name::name(const std::string& full)
{
    if (full.empty())
        throw io::error("address cannot be empty");
    
    if (full.front() == '[') {
        size_t rbracket = full.find(']');
        if (rbracket == std::string::npos || rbracket + 2 >= full.size() || full[rbracket+1] != ':')
            throw io::error("bad address string: " + full);
        host_ = full.substr(1, rbracket - 1);
        service_ = full.substr(rbracket + 2);
    } else {
        size_t colon = full.find(':');
        if (colon == std::string::npos) {
            throw io::error("bad address string: " + full);
        } else {
            host_ = full.substr(0, colon);
            service_ = full.substr(colon + 1);
        }
    }
}

std::string name::full() const
{
    std::ostringstream s;
    size_t colon = host_.find(':');
    if (colon != std::string::npos)
        s << '[';
    s << host_;
    if (colon != std::string::npos)
        s << ']';
    s << ':' << service_;
    return s.str();
}

namespace {

template<class Callable>
class AtScopeExit {
public:
    explicit AtScopeExit(Callable c): f_(c) {}    
    ~AtScopeExit() { f_(); }
private:
    Callable f_;
};

template<class Callable>
AtScopeExit<Callable> atScopeExit(Callable c) { return AtScopeExit<Callable>(c); }

std::vector<addr> doResolve(const name& name, resolve_mode mode)
{
    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = (mode == resolve_mode::PASSIVE ? AI_PASSIVE : 0)
        | AI_V4MAPPED | AI_ALL;
    
    struct addrinfo* ai;
    int ret = ::getaddrinfo(
        name.host().empty()    ? nullptr : name.host().c_str(),
        name.service().empty() ? nullptr : name.service().c_str(),
        &hints, &ai
    );
    if (ret)
        throw io::error("cannot resolve " + name.full() + ": " + gai_strerror(ret));
    
    auto freeAi = atScopeExit([ai]{ freeaddrinfo(ai); });
    
    std::vector<addr> addrs;
    for (struct addrinfo* p = ai; p; p = p->ai_next)
        addrs.emplace_back(p->ai_family, p->ai_socktype, p->ai_protocol, p->ai_addr, p->ai_addrlen);
    
    return addrs;
}

name doResolve(const addr& addr)
{
    char host[1024];
    char port[256];
    int ret = ::getnameinfo(addr.sockaddr(), addr.addrlen(),
        host, sizeof(host), port, sizeof(port), NI_IDN);
    
    if (ret) {
        std::ostringstream s;
        s << "cannot resolve " << addr << ": " << gai_strerror(ret);
        throw io::error(s.str());
    }
    
    return name(host, port);
}   

} // namespace


// getaddrinfo() uses alloca() heavily and thus requires enlagrened stack size

std::vector<addr> resolve(const name& name, resolve_mode mode)
{    
    return doSpawn(impl::OneTimeFunction< std::vector<addr> >([&name, mode]{
        return doResolve(name, mode);
    }), 65536).join();
}

std::vector<addr> resolve(const std::string& name, resolve_mode mode)
{
    if (name.find(':') == std::string::npos && mode == resolve_mode::PASSIVE) {
        return resolve(io::name({}, name), mode);
    } else {
        return resolve(io::name(name), mode);
    }
}

name resolve(const addr& addr)
{
    return doSpawn(impl::OneTimeFunction<io::name>([&addr]{
        return doResolve(addr);
    }), 65536).join();
}

} // namespace io
