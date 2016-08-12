/**
 * addr.h -- wrapper classes for network addresses and names
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

#include <vector>
#include <iostream>
#include <utility>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <netinet/in.h>

namespace io {

class addr {
public:
    addr(int af, int socktype, int proto, struct sockaddr* addr, int addrlen):
        af_(af), type_(socktype), proto_(proto), addrlen_(addrlen)
        { memcpy(&union_, addr, addrlen); }
    
    template<class T>
    const T* as() const { return reinterpret_cast<const T*>(&union_); }

    int af() const { return af_; }
    int socktype() const { return type_; }
    int proto() const { return proto_; }
    
    const struct sockaddr* sockaddr() const { return as<struct sockaddr>(); }
    int addrlen() const { return addrlen_; }

    union Union {
        struct sockaddr     generic_;
        struct sockaddr_un  unix_;
        struct sockaddr_in  inet_;
        struct sockaddr_in6 inet6_;
    };
  
private:
    int af_;
    int type_;
    int proto_;
    Union union_;
    int addrlen_;
};

class name {
public:
    name(std::string host, std::string service):
        host_(std::move(host)), service_(std::move(service)) {}
    
    /*implicit*/ name(const std::string& full);
    
    const std::string& host() const { return host_; }
    const std::string& service() const { return service_; }
    std::string full() const;
    
private:
    std::string host_;
    std::string service_;
};

std::ostream& operator << (std::ostream& out, const addr& addr);

enum class resolve_mode {
    ACTIVE, // Return address suitable for connect()
    PASSIVE // Return address suitable for listen() (i.e. handle names consisting solely of port numbers)
};
std::vector<addr> resolve(const name& name, resolve_mode mode = resolve_mode::ACTIVE);
std::vector<addr> resolve(const std::string& name, resolve_mode mode = resolve_mode::ACTIVE);
name resolve(const addr& addr);

inline std::vector<addr> resolve(const std::string& host, const std::string& service, resolve_mode mode = resolve_mode::ACTIVE)
    { return resolve(name(host, service), mode); }

} // namespace io
