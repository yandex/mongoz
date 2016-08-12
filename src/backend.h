/**
 * backend.h -- an interface to a specific MongoDB backend server.
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

#include "proto.h"
#include "lazy.h"
#include "version.h"
#include <syncio/syncio.h>
#include <bson/bson.h>
#include <chrono>
#include <algorithm>
#include <mutex>
#include <cassert>
#include <atomic>
#include <iostream>
#include <map>
#include <initializer_list>
#include <cassert>

class Endpoint;
class Backend;
class Shard;

/// An open connection to a backend server.
class Connection {
public:
    Connection() {}
    explicit Connection(Endpoint* endpt, bool isPrimary):
        impl_(new Impl(endpt, isPrimary)) {}

    Connection(Connection&&) = default;
    Connection& operator = (Connection&&) = default;
    ~Connection();

    bool exists() const { return !!impl_; }
    Endpoint& endpoint() const { assert(exists()); return *impl_->endpt; }
    Backend& backend() const;
    
    /// Initiates the connection if neccessary, associates it with version `v'
    /// of collection `ns' and sends (msg,len) over.
    ///
    /// We need that many parameters to have something to send in any case,
    /// so we can detect prior disconnects and reconnect to the backend if neccessary.
    void establish(const Namespace& ns, const ChunkVersion& v, const char* msg, size_t len);
    
    void establish(const Namespace& ns, const ChunkVersion& v, const QueryComposer& q)
        { std::vector<char> msg = q.data(); establish(ns, v, msg.data(), msg.size()); }
        
    io::stream& stream() { assert(exists()); return impl_->s; }
        
    /// Puts the connection back into a connection pool, so it can be reused later.
    /// Called only on happy path; any exceptions lead to connection being closed
    /// in destructor.
    void release();

private:
    struct Impl {
        Endpoint* endpt = 0;
        bool isPrimary = false;
        bool authenticated = false;
        io::stream s;
        std::map<std::string, ChunkVersion> versions;
        
        Impl(Endpoint* ept, bool primary):
            endpt(ept), isPrimary(primary) {}
    };
    std::unique_ptr<Impl> impl_;
    
    void authenticate();
    void trySetVersion(const Namespace& ns, const ChunkVersion& v);
    
    friend class Endpoint;
};


/// A specific network address of a backend server
/// (there can be many of these corresponding to one backend,
///  since the backend may have different interfaces or different
///  address families, say IPv4 and IPv6).
/// Equipped with its own poller, so constantly keeps 
/// alive() and roundtrip() up-to-date.
class Endpoint {
public:
    explicit Endpoint(Backend* backend, const io::addr& addr):
        backend_(backend), addr_(addr), roundtrip_(std::chrono::microseconds::max()), pinged_(false)
    {
        ping_ = io::spawn([this]{ keepPing(); });
    }
    
    Backend& backend() const { return *backend_; }
    const io::addr& addr() const { return addr_; }
    
    /// Endpoint status.
    std::chrono::microseconds roundtrip() const { return roundtrip_; }
    bool alive() const { return roundtrip() != std::chrono::microseconds::max(); }
    bool wasAlive() const { return prevRoundtrip_ != std::chrono::microseconds::max(); }
    
    
    /// Fetches a cached connection to the backend or creates a new one.
    Connection getAny() { return get(conns_); }
    
    /// Same as above, but returned connection presumably has shard versioning support
    Connection getPrimary() { return get(primaries_); }

    /// Puts the connection back into the connection pool.
    void release(Connection&& c);
    
    /// Called upon backend failure. Immediately puts the endpoint
    /// into dead state and initiates an asynchronous ping.
    void failed();
    
    /// Closes all cached connections to the backend.
    void flush();
    
    /// Performs a synchronous ping. Returns fresh endpoint status (alive()).
    bool pingNow();
    
private /*fields*/:
    Backend* backend_;
    io::addr addr_;
    std::chrono::microseconds roundtrip_;
    std::chrono::microseconds prevRoundtrip_;
    io::task<void> ping_;
    std::vector<Connection> conns_;
    std::vector<Connection> primaries_;
    io::sys::mutex mutex_;
    bool pinged_;

private /*methods*/:
    Connection pop(std::vector<Connection>& v);
    Connection get(std::vector<Connection>& v);
    
    /// A background routine constantly updating endpoint status.
    void keepPing();
        
    void setAlive(std::chrono::microseconds netRoundtrip,
        std::chrono::microseconds grossRoundtrip, bson::Object obj);
    void setDead(const std::string& reason);
};


/// A backend server, consisting of several endpoints
/// and a constantly updated status.
class Backend {
public:
    explicit Backend(Shard* shard, const std::string& addr):
        shard_(shard), addr_(addr),
        nearest_([this]{ return calcNearest(); })
    {
        for (const io::addr& a: io::resolve(addr))
            endpts_.emplace_back(new Endpoint(this, a));
    }

    Shard* shard() const { return shard_; }
    const std::string& addr() const { return addr_; }
    
    bson::Object status() const
    {
        io::shared_lock<io::sys::shared_mutex> lock(mutex_);
        return status_;
    }
    
    std::chrono::microseconds roundtrip() const { return nearest_.get().value()->roundtrip(); }
    bool alive() const { return !status().empty() && nearest_.get().value()->alive(); }
    
    Endpoint* endpoint() const { return nearest_.get().value(); }
    
    const std::vector< std::unique_ptr<Endpoint> >& endpoints() const { return endpts_; }
    
    void failed();
    
    void pingNow();
    
    class SoftwareVersion {
    public:
        SoftwareVersion() = default;
        
        /*implicit*/ SoftwareVersion(std::initializer_list<unsigned> v): v_(v) {}
        
        explicit SoftwareVersion(const bson::Object& buildInfo)
        {
            for (const bson::Element& elt: buildInfo["versionArray"].as<bson::Array>(bson::Array()))
                v_.push_back(elt.as<int>());
        }
        
        bool operator == (const SoftwareVersion& rhs) const { return v_ == rhs.v_; }
        bool operator != (const SoftwareVersion& rhs) const { return !(*this == rhs); }
        bool operator < (const SoftwareVersion& rhs) const
            { return std::lexicographical_compare(v_.begin(), v_.end(), rhs.v_.begin(), rhs.v_.end()); }
        bool operator > (const SoftwareVersion& rhs) const { return rhs < *this; }
        bool operator <= (const SoftwareVersion& rhs) const { return *this == rhs || *this < rhs; }
        bool operator >= (const SoftwareVersion& rhs) const { return *this == rhs || *this > rhs; }
    private:
        std::vector<unsigned> v_;
    };
    
    SoftwareVersion softwareVersion() const { return SoftwareVersion(status()["build_info"].as<bson::Object>(bson::Object())); }
    
private:
    typedef std::unique_ptr<Endpoint> EndptPtr;
    
    Shard* shard_;
    std::string addr_;
    bson::Object status_;
    std::vector<EndptPtr> endpts_;
    Lazy<Endpoint*> nearest_;
    mutable io::sys::shared_mutex mutex_;
    
    void endpointAlive(Endpoint* pt, bson::Object status);
    void endpointDead(Endpoint* pt);
    Endpoint* calcNearest() const;
    
    friend class Endpoint;
};


inline std::ostream& operator << (std::ostream& out, const Backend& b)
{
    return out << b.addr();
}

inline std::ostream& operator << (std::ostream& out, const Endpoint& pt)
{
    return out << pt.backend().addr() << " (" << pt.addr() << ")";
}

inline Backend& Connection::backend() const
{
    assert(exists());
    return endpoint().backend();
}

inline void Connection::release()
{
    assert(exists());
    endpoint().release(std::move(*this));
}
