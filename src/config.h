/**
 * config.h -- a sharding config state
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

#include "sorted_vector.h"
#include "log.h"
#include "error.h"
#include "backend.h"
#include "version.h"
#include "clock.h"
#include <bson/bson.h>
#include <mutex>
#include <vector>
#include <string>
#include <memory>
#include <stdexcept>

class Shard;

class Config {
public:
        
    class Chunk {
    public:
        explicit Chunk(const bson::Object& obj);
        
        /// Constructs a primary shard containing all possible key values.
        explicit Chunk(Namespace ns, std::string shard);
        
        void link(Config& conf);
        
        const Namespace& ns() const { return ns_; }
        const ChunkVersion& version() const { return version_; }
        std::shared_ptr<Shard> shard() const { return shard_; }
        
        const bson::Object& lowerBound() const { return min_; }
        const bson::Object& upperBound() const { return max_; }
        
        bool contains(const bson::Object& key) const { return (min_.empty() || key >= min_) && (max_.empty() || key < max_); }
        
        void setVersion(ChunkVersion v) { version_ = std::move(v); }
        
    private:
        Namespace ns_;
        ChunkVersion version_;
        bson::Object min_;
        bson::Object max_;
        std::string shardID_;
        std::shared_ptr<Shard> shard_;
    };


    class Collection {
    public:
        typedef std::vector<Chunk>::const_iterator const_iterator;
        
        explicit Collection(const bson::Object& obj);
        void link(Config& conf);

        const Namespace& ns() const { return ns_; }
        bool isDropped() const { return isDropped_; }
        const bson::Object& shardingKey() const { return shardingKey_; }
        
        const_iterator begin() const { return range_.first; }
        const_iterator end() const { return range_.second; }
        
    private:
        const Config* conf_;
        Namespace ns_;
        bool isDropped_;
        bson::Object shardingKey_;
        std::pair<const_iterator, const_iterator> range_;
    };

    
    class Database {
    public:
        explicit Database(const bson::Object& obj);
        void link(Config& conf);
        
        const std::string& name() const { return name_; }
        bool isPartitioned() const { return isPartitioned_; }
        
        std::shared_ptr<Shard> primaryShard() const { return primary_; }
        
    private:
        const Config* conf_;
        std::string name_;
        bool isPartitioned_;
        std::string primaryID_;
        std::shared_ptr<Shard> primary_;
    };    
    
    
    Config(std::shared_ptr<Shard> configShard, const bson::Object& obj);
    ~Config();
    
    const ::bson::Object& bson() const { return bson_; }
    
    std::shared_ptr<Shard> shard(const std::string& name) const;
    const Database* database(const std::string& name) const { return databases_.findPtr(name); }
    const Collection* collection(const Namespace& ns) const { return collections_.findPtr(ns.ns()); }
    
    const SortedVector<Chunk, std::pair<std::string, bson::Object> >& chunks() const { return chunks_; }
    
    const SortedVector<Database, std::string>& databases() const { return databases_; }
    std::vector< std::shared_ptr<Shard> > shards() const;
    
    struct VersionedShard {
        std::shared_ptr<Shard> shard;
        ChunkVersion version;
    };

    /// Returns a list of shards containing collection `ns', along with
    /// the data version on each shard.
    std::vector<VersionedShard> shards(const Namespace& ns) const;

    /// Returns a list of shards containing a part of collection `ns' matching `criteria'.
    /// If uncertain, returns shards(ns).
    std::vector<VersionedShard> find(const Namespace& ns, const bson::Object& criteria) const;
    
    SteadyClock::time_point createdAt() const { return createdAt_; }

private:
    bson::Object bson_;
    std::shared_ptr<Shard> configShard_;
    SortedVector<std::pair< std::string, std::shared_ptr<Shard> >, std::string> shards_;
    SortedVector<Chunk, std::pair<std::string, bson::Object> > chunks_;
    SortedVector<Collection, std::string> collections_;
    SortedVector<Database, std::string> databases_;
    SteadyClock::time_point createdAt_;
    
    Config(const Config&) = delete;
    Config& operator = (const Config&) = delete;
};


class ConfigHolder {
public:
    explicit ConfigHolder(const std::string& connstr);
    
    const std::string& connectionString() const { return connstr_; }
    
    bool exists()
    {
        std::unique_lock<io::sys::mutex> lock(mutex_);
        return !!config_.get();
    }
    
    std::shared_ptr<Config> get()
    {
        std::unique_lock<io::sys::mutex> lock(mutex_);
        if (!config_.get())
            throw errors::NoShardConfig("no shard config available yet");
        return config_;
    }
    
    void update();
    
    std::shared_ptr<Shard> shard() const { return configShard_; }

private /*methods*/:
    bson::Object fetchConfig();
    void keepUpdating();

private /*fields*/:
    std::string connstr_;
    std::shared_ptr<Shard> configShard_;
    std::string cache_;
    std::shared_ptr<Config> config_;
    io::sys::mutex mutex_;
    io::task<void> updater_;
};

extern std::unique_ptr<ConfigHolder> g_config;
