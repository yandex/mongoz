/**
 * shard.h -- a single shard in a sharded environment
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

#include "operations.h"
#include "backend.h"
#include "monitor.h"
#include "proto.h"
#include <map>
#include <memory>

class Shard: public std::enable_shared_from_this<Shard> {
public:
    virtual ~Shard() {}
    
    struct PingQuery {
        std::string key;
        Namespace ns;
        bson::Object criteria;
    };
    
    const std::string& id() const { return id_; }
    const std::string& connectionString() const { return connstr_; }
    
    /// Returns a minimal version of backend software among alive replicas.
    Backend::SoftwareVersion softwareVersion() const;
    
    bool supportsWriteCommands() const { return softwareVersion() >= Backend::SoftwareVersion { 2,6,0 }; }
    
    /// Returns a connection suitable for read operation with specified `flags'
    /// (taken directly from OP_QUERY message) and `readPreference',
    /// referencing a backend different from `exclude' (if specified).
    /// May return NULL if no suitable backend found.
    virtual Connection readOp(
        uint32_t queryFlags,
        const bson::Object& readPreference,
        Backend* exclude = 0
    ) = 0;
    
    virtual std::unique_ptr<WriteOperation> write(Namespace ns, ChunkVersion v, std::vector<char> msg);
    virtual std::unique_ptr<WriteOperation> write(Namespace ns, ChunkVersion v, bson::Object cmd);

    /// Returns a connection suitable for write operations.
    /// Used in default implementation of write().
    virtual Connection primary() { return Connection(); }
    
    /// Called upon a failure while communicating with a backend.
    void failed(Backend* b)
    {
        onFailure(b);
        b->failed();
    }
    
    /// Called upon recevied `not a master' error from a node
    /// which was supposed to be a master (applies only to replica set)
    virtual void lostMaster() {}
        
    /// Called upon backend either successful or unsuccessful ping
    virtual void backendUpdated(Backend* /*backend*/) {}
    
    /// Specifies list of queries to be executed on each backend during ping.
    const std::vector<PingQuery>& pingQueries() const { return pingQueries_; }

    const std::vector< std::unique_ptr<Backend> >& backends() const { return backends_; }
    
    /// Returns a user-readable short description of backend health
    virtual std::string status(const Backend*) const { return ""; }
    
    /// Returns a lag between replica's data and most recent data (or max() if not applicable)
    virtual std::chrono::milliseconds replicationLag(const Backend*) const { return std::chrono::milliseconds::max(); }
    
    /// Returns health status of the shard
    virtual monitoring::Status status() const { return {}; }
    
    bson::Object debugInspect() const;
    
    /// Parses `connstr' and constructs a shard of neccessary type.
    static std::unique_ptr<Shard> make(const std::string& id, const std::string& connstr);
    
protected:
    Shard(std::vector<PingQuery> pingQueries, const std::vector<std::string>& backends):
        pingQueries_(std::move(pingQueries))
    {
        for (const std::string& addr: backends)
            backends_.emplace_back(new Backend(this, addr));
    }
        
    std::vector< std::unique_ptr<Backend> >::iterator begin() { return backends_.begin(); }
    std::vector< std::unique_ptr<Backend> >::iterator end()   { return backends_.end();   }
    std::vector< std::unique_ptr<Backend> >::const_iterator begin() const { return backends_.begin(); }
    std::vector< std::unique_ptr<Backend> >::const_iterator end()   const { return backends_.end();   }
    
private /*methods*/:
    virtual void onFailure(Backend*) {}
    
private /*fields*/:
    std::string id_;
    std::string connstr_;
    std::vector<PingQuery> pingQueries_;
    std::vector< std::unique_ptr<Backend> > backends_;
};



class ShardPool {
public:
    static ShardPool& instance() { static ShardPool p; return p; }
    
    // Used for debugging and testing
    std::shared_ptr<Shard> find(const std::string& id)
    {
        io::shared_lock<io::sys::shared_mutex> lock(mutex_);
        for (const auto& kv: shards_)
            if (kv.second->id() == id)
                return kv.second;
        return {};
    }

    std::shared_ptr<Shard> get(const std::string& id, const std::string& connstr)
    {
        {
            io::shared_lock<io::sys::shared_mutex> lock(mutex_);
            auto i = shards_.find(connstr);
            if (i != shards_.end())
                return i->second;
        }
        
        std::shared_ptr<Shard> newshard(Shard::make(id, connstr).release());
        std::unique_lock<io::sys::shared_mutex> lock(mutex_);
        auto i = shards_.insert(std::make_pair(connstr, newshard)).first;
        return i->second;
    }
        
private:
    std::map<std::string, std::shared_ptr<Shard> > shards_;
    io::sys::shared_mutex mutex_;
    
    ShardPool() {}
    ShardPool(const ShardPool&) = delete;
    ShardPool& operator = (const ShardPool&) = delete;
};
