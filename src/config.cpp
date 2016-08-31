/**
 * config.cpp -- a sharding config state
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

#include "config.h"
#include "proto.h"
#include "shard.h"
#include "options.h"
#include "cache.h"
#include <set>
#include <syncio/syncio.h>
#include <bson/bson11.h>
#include <algorithm>
#include <cassert>
#include <openssl/md5.h>


namespace {

void hashType(MD5_CTX* ctx, const bson::Element& elt)
{
    int type = -1;
    if (!elt.exists())
        type = 0;
    else if (elt.is<bson::MinKey>() || elt.is<bson::MaxKey>())
        type = elt.type();
    else if (elt.is<bson::Null>())
        type = 5;
    else if (elt.canBe<double>())
        type = 10;
    else if (elt.is<std::string>())
        type = 15;
    else if (elt.is<bson::Object>())
        type = 20;
    else if (elt.is<bson::Array>())
        type = 25;
    else if (elt.is< std::vector<char> >())
        type = 30;
    else if (elt.is<bson::ObjectID>())
        type = 35;
    else if (elt.is<bool>())
        type = 40;
    else if (elt.is<bson::Time>())
        type = 45;
    else if (elt.is<bson::Timestamp>())
        type = 47;
    
    MD5_Update(ctx, &type, sizeof(type));
}

void hash(MD5_CTX* ctx, const bson::Element& elt);

template<class Range>
void hashRange(MD5_CTX* ctx, const Range& range)
{
    for (bson::Element elt: range) {
        hashType(ctx, elt);
        MD5_Update(ctx, elt.name(), strlen(elt.name()) + 1);
        hash(ctx, elt);
    }
    int32_t type = 0;
    MD5_Update(ctx, &type, sizeof(type));
}

void hash(MD5_CTX* ctx, const bson::Element& elt)
{
    if (elt.is<bson::Object>()) {
        hashRange(ctx, elt.as<bson::Object>());
    } else if (elt.is<bson::Array>()) {
        hashRange(ctx, elt.as<bson::Array>());
    } else if (elt.is<double>()) {
        int64_t i;
        double v = elt.as<double>();
        if (std::isnan(v))
            i = 0;
        else if (v < (double) std::numeric_limits<int64_t>::min())
            i = std::numeric_limits<int64_t>::min();
        else if (v > (double) std::numeric_limits<int64_t>::max())
            i = std::numeric_limits<int64_t>::max();
        else
            i = static_cast<int64_t>(v);
        MD5_Update(ctx, &i, sizeof(i));
    } else if (elt.canBe<int64_t>()) {
        int64_t i = elt.as<int64_t>();
        MD5_Update(ctx, &i, sizeof(i));
    } else {
        MD5_Update(ctx, elt.valueData(), elt.valueSize());
    }
}

size_t hash(const bson::Element& elt)
{
    MD5_CTX ctx;
    MD5_Init(&ctx);
    
    int seed = 0;
    MD5_Update(&ctx, &seed, sizeof(seed));
    
    hashType(&ctx, elt);
    hash(&ctx, elt);
    
    union {
        unsigned char md5[MD5_DIGEST_LENGTH];
        size_t out;
    };
    MD5_Final(md5, &ctx);
    return out;
}

} // namespace


Config::Chunk::Chunk(const bson::Object& obj):
    ns_(obj["ns"].as<std::string>()),
    version_(
        obj["lastmodEpoch"].as<bson::ObjectID>(),
        obj["lastmod"].as<bson::Timestamp>()
    ),
    min_(obj["min"].as<bson::Object>()),
    max_(obj["max"].as<bson::Object>()),
    shardID_(obj["shard"].as<std::string>())
{
    if (!strcmp(min_.front().name(), "$minkey"))
        min_ = bson::Object();
    if (!strcmp(max_.front().name(), "$maxkey"))
        max_ = bson::Object();
}

Config::Chunk::Chunk(Namespace ns, std::string shard):
    ns_(std::move(ns)),
    shardID_(std::move(shard))
{}

void Config::Chunk::link(Config& conf) { shard_ = conf.shard(shardID_); }



Config::Database::Database(const bson::Object& obj):
    name_(obj["_id"].as<std::string>()),
    isPartitioned_(obj["partitioned"].as<bool>()),
    primaryID_(obj["primary"].as<std::string>())
{}

void Config::Database::link(Config& conf) { primary_ = conf.shard(primaryID_); }



Config::Collection::Collection(const bson::Object& obj):
    ns_(obj["_id"].as<std::string>()),
    isDropped_(obj["dropped"].as<bool>()),
    shardingKey_(obj["key"].as<bson::Object>())
{}

void Config::Collection::link(Config& conf)
{
    range_ = std::equal_range(conf.chunks().begin(), conf.chunks().end(), ns_.ns(),
        SortedVector<Chunk, std::string>::Cmp([](const Chunk& ch) { return ch.ns().ns(); }));
    
    for (auto j = range_.first, i = j++, ie = range_.second; i != ie && j != ie; ++i, ++j)
        if (i->upperBound() != j->lowerBound())
            throw std::runtime_error("gap in partition of collection " + ns_.ns());
}

std::vector<Config::VersionedShard> Config::find(const Namespace& ns, const bson::Object& criteria) const
{
    const Collection* coll = collection(ns);
    if (!coll) {
        DEBUG(2) << "collection " << ns << " not sharded";
        return shards(ns);
    }
    
    std::string hashedField = (coll->shardingKey().size() == 1
        && coll->shardingKey().begin()->is<std::string>()
        && coll->shardingKey().begin()->as<std::string>() == "hashed"
    ) ? coll->shardingKey().begin()->name() : std::string();
    
    std::string vectorName;
    bson::Array vectorValues;
    bson::ObjectBuilder headb, tailb;
    for (const bson::Element& kel: coll->shardingKey()) {
        bson::Element el = criteria[kel.name()];
        if (!el.exists()) {
            return shards(ns);
        } else if (!el.is<bson::Object>() || *el.as<bson::Object>().front().name() != '$') {
            if (vectorName.empty())
                headb[kel.name()] = el;
            else
                tailb[kel.name()] = el;
        } else if (el.as<bson::Object>().front().name() == std::string("$in")) {
            if (vectorName.empty()) {
                vectorName = kel.name();
                vectorValues =  el.as<bson::Object>().front().as<bson::Array>();
            } else {
                return shards(ns);
            }
        } else {
            return shards(ns);
        }
    }
    
    bson::Object head = headb.obj(), tail = tailb.obj();
    auto doFind = [this, &ns, &hashedField](const bson::Object& key) -> VersionedShard {
        bson::Object k = hashedField.empty() ? key : bson::object(hashedField, static_cast<ssize_t>(hash(key[hashedField.c_str()])));
        auto i = chunks_.upper_bound(std::make_pair(ns.ns(), k));
        ASSERT(i != chunks_.begin());
        --i;
        DEBUG(2) << "found chunk " << i->lowerBound() << "..." << i->upperBound() << " for " << k;
        ASSERT(i->contains(k));
        return VersionedShard { i->shard(), i->version() };
    };
    
    if (vectorName.empty())
        return { doFind(head) };
    
    auto vsLess = [](const Config::VersionedShard& a, const Config::VersionedShard& b) { return a.shard.get() < b.shard.get(); };
    std::set<Config::VersionedShard, decltype(vsLess)> ret(vsLess);
    for (const bson::Element& v: vectorValues) {
        bson::ObjectBuilder kb;
        for (const bson::Element& el: head)
            kb[el.name()] = el;
        kb[vectorName] = v;
        for (const bson::Element& el: tail)
            kb[el.name()] = el;
        ret.insert(doFind(kb.obj()));
    }
    return { ret.begin(), ret.end() };
}

std::vector<Config::VersionedShard> Config::shards(const Namespace& ns) const
{
    std::map<std::shared_ptr<Shard>, ChunkVersion> map;
    
    if (ns.db() == "config")
        return {{ configShard_, ChunkVersion() }};

    const Collection* c = collection(ns);

    if (c) {
        for (const Config::Chunk& chunk: *c) {
            auto prev = map.find(chunk.shard());
            if (prev == map.end()) {
                map.insert(std::make_pair(chunk.shard(), chunk.version()));
            } else {
                assert(prev->second == chunk.version());
            }
        }
    } else {
        const Database* db = database(ns.db());
        if (db)
            map.insert(std::make_pair(db->primaryShard(), ChunkVersion{}));
    }
    
    std::vector<VersionedShard> ret;
    for (auto&& s: map)
        ret.push_back({ s.first, s.second });
    return ret;
}

std::vector< std::shared_ptr<Shard> > Config::shards() const
{
    std::vector< std::shared_ptr<Shard> > ret;
    ret.push_back(configShard_);
    for (const auto& s: shards_)
        ret.push_back(s.second);
    return ret;
}


namespace {

class ShardConf {
public:
    explicit ShardConf(const bson::Object& obj):
        id_(obj["_id"].as<std::string>()),
        connstr_(obj["host"].as<std::string>())
    {}
    
    const std::string& id() const { return id_; }
    const std::string& connstr() const { return connstr_; }

private:
    std::string id_;
    std::string connstr_;
};


template<class T, class Key>
void populate(SortedVector<T, Key>& dest, const bson::Element& arr)
{
    for (const bson::Element& el: arr.as<bson::Array>())
        dest.vector().emplace_back(el.as<bson::Object>());
    dest.finish();
}

template<class T, class Key>
void link(Config* conf, SortedVector<T, Key>& dest)
{
    for (auto& t: dest.vector())
        t.link(*conf);
}

}

Config::Config(std::shared_ptr<Shard> configShard, const bson::Object& obj):
    bson_(obj),
    configShard_(std::move(configShard)),
    shards_([](const std::pair<std::string, std::shared_ptr<Shard> >& s) { return s.first; }),
    chunks_([](const Chunk& c) { return std::make_pair(c.ns().ns(), c.lowerBound()); }),
    collections_([](const Collection& c) { return c.ns().ns(); }),
    databases_([](const Database& db) { return db.name(); }),
    createdAt_(SteadyClock::now())
{
    SortedVector<ShardConf, std::string> shards([](const ShardConf& s) { return s.id(); });
    
    populate(shards, obj["shards"]);
    populate(chunks_, obj["chunks"]);
    populate(collections_, obj["collections"]);
    populate(databases_, obj["databases"]);
    
    for (const ShardConf& shard: shards)
        shards_.vector().push_back(std::make_pair(
            shard.id(), ShardPool::instance().get(shard.id(), shard.connstr())
        ));

    link(this, chunks_);
    link(this, collections_);
    link(this, databases_);
    
    std::map<std::pair<std::string, Shard*>, ChunkVersion> versions;
    for (const Chunk& ch: chunks_) {
        auto k = std::make_pair(ch.ns().ns(), ch.shard().get());
        auto i = versions.find(k);
        if (i == versions.end()) {
            versions.insert(std::make_pair(k, ch.version()));
        } else if (i->second.epoch() != ch.version().epoch()) {
            throw errors::ShardConfigBroken(
                "chunks epochs differ for collection " + ch.ns().ns() + " and shard " + ch.shard()->connectionString());
        } else if (i->second.stamp() < ch.version().stamp()) {
            i->second = ch.version();
        }
    }
    
    for (Chunk& ch: chunks_)
        ch.setVersion(versions.find(std::make_pair(ch.ns().ns(), ch.shard().get()))->second);
}

Config::~Config() {}

std::shared_ptr<Shard> Config::shard(const std::string& name) const
{
    return name == "config" ? configShard_ : shards_.findRef(name).second;
}


namespace {

template<class... Conditions>
bson::Array readTable(io::stream& s, const Namespace& ns, Conditions&&... conditions)
{
    DEBUG(1) << "Fetching table " << ns;
    bson::ArrayBuilder ret;
 
    s << QueryComposer(ns, bson::object(
        "query", bson::object(std::forward<Conditions>(conditions)...),
        "$orderby", bson::object("_id", 1)
    )) << std::flush;
    while (uint64_t cursorID = readReply(s, 0, [&ret](const bson::Object& obj) { ret << obj; })) {
        MsgBuilder b;
        b << (uint32_t) 0 << (uint32_t) 0 << Opcode::GET_MORE
          << (uint32_t) 0 << ns.ns() << (int32_t) 0 << cursorID;
        s.write(b.data(), b.size());
        s << std::flush;
    }
    return ret.array();
}

bson::Object readconf(io::stream& stream)
{
    bson::ObjectBuilder ret;
    ret["shards"]      = readTable(stream, Namespace("config.shards"));
    ret["databases"]   = readTable(stream, Namespace("config.databases"));
    ret["collections"] = readTable(stream, Namespace("config.collections"), "dropped", false);
    ret["chunks"]      = readTable(stream, Namespace("config.chunks"));
    DEBUG(1) << "Fetching config complete";
    return ret.obj();
}

} // namespace


bson::Object ConfigHolder::fetchConfig()
{
    std::vector<size_t> serverIdxs;
    for (size_t i = 0; i != servers_.size(); ++i)
        serverIdxs.push_back(i);
    
    auto pickServer = [this, &serverIdxs]() -> Backend* {
        auto i = std::min_element(serverIdxs.begin(), serverIdxs.end(),
            [this](size_t a, size_t b) { return servers_[a]->roundtrip() < servers_[b]->roundtrip(); });
        assert(i != serverIdxs.end());
        
        Backend* ret = servers_[*i].get();
        *i = serverIdxs[serverIdxs.size()-1];
        serverIdxs.pop_back();
        return ret;
    };
    
    io::task<bson::Object> task1, task2;
    
    auto runFetch = [pickServer]() {
        Backend* server = pickServer();
        DEBUG(2) << "Using config server " << server->addr();
        io::task<bson::Object> task = io::spawn([server]() -> bson::Object {
            Endpoint* endpt = server->endpoint();
            Connection c = endpt->getAny();
            c.establish(Namespace(), ChunkVersion(), QueryComposer(Namespace("local", "$cmd"), bson::object("ping", 1)));
            readReply(c.stream(), 0, [](const bson::Object&){});
            bson::Object ret = readconf(c.stream());
            c.release();
            return ret;
        });
        task.rename("fetch config from " + server->addr());
        return std::move(task);
    };
    
    io::timeout retransmit = options().confRetransmit;
    io::timeout timeout = options().confTimeout;
    
    task1 = runFetch();
    io::wait(task1, std::min(retransmit, timeout));
        
    if (!task1.succeeded() && !serverIdxs.empty() && retransmit.finite()) {
        DEBUG(1) << "Retransmitting config request to another server";
        task2 = runFetch();
        io::wait_any(task1, task2, timeout);
        if (task1.failed() && !task2.completed())
            io::wait(task2, timeout);
    } else {
        io::wait(task1, timeout);
    }
    
    bson::Object cfg;
    if (!task1.empty() && task1.completed())
        cfg = task1.get();
    else if (!task2.empty() && task2.completed())
        cfg = task2.get();
    else
        throw errors::BackendInternalError("cannot communicate with config servers");
    
    DEBUG(3) << "Read config: " << cfg;
    return cfg;
}

ConfigHolder::ConfigHolder(const std::string& connstr):
    connstr_(connstr)
{
    if (connstr.empty())
        throw std::runtime_error("connection string for config servers cannot be empty");
    
    std::vector<std::string> servers;
    size_t pos = 0;
    for (size_t comma; (comma = connstr.find(',', pos)) != std::string::npos; pos = comma + 1)
        servers.push_back(connstr.substr(pos, comma - pos));
    servers.push_back(connstr.substr(pos));
    
    for (const std::string& server: servers)
        servers_.emplace_back(new Backend(0, server));

    configShard_ = ShardPool::instance().get("config", connstr_);
    
    bson::Object cache = g_cache->get("shard_config");
    if (!cache.empty()) {
        INFO() << "Using shard config cache";
        config_ = std::make_shared<Config>(configShard_, cache);
        if (!config_.get()) {
            INFO() << "Cannot use shard config cache";
        }
    }
    
    updater_ = io::spawn([this]{ keepUpdating(); });
}

void ConfigHolder::update()
{
    DEBUG(1) << "Fetching shard config";
    bson::Object confBson = fetchConfig();

    std::shared_ptr<Config> current;
    {
        std::unique_lock<io::sys::mutex> lock(mutex_);
        current = config_;
    }

    if (!current || current->bson() != confBson) {
        {
            std::shared_ptr<Config> conf = std::make_shared<Config>(shard(), confBson);
            DEBUG(1) << "Applying shard config";
            std::unique_lock<io::sys::mutex> lock(mutex_);
            config_ = conf;
            NOTICE() << "Shard config changed";
        }
        g_cache->put("shard_config", confBson);
    } else {
        DEBUG(1) << "Shard config unchanged";
    }
}

void ConfigHolder::keepUpdating()
{
    for (;;) {
        try {
            update();
        }
        catch (std::exception& e) {
            WARN() << "Cannot update config: " << e.what();
        }
        io::sleep(options().confInterval);
    }
}

std::unique_ptr<ConfigHolder> g_config;
