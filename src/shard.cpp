/**
 * shard.cpp -- various type of shards
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

#include "shard.h"
#include "proto.h"
#include "lazy.h"
#include "options.h"
#include "write.h"
#include "clock.h"
#include "log.h"
#include <bson/bson11.h>
#include <cstdlib>
#include <map>

std::unique_ptr<WriteOperation> Shard::write(Namespace ns, ChunkVersion v, std::vector<char> msg)
{
    return std::unique_ptr<WriteOperation>(new WriteToBackend24(
        Config::VersionedShard { shared_from_this(), std::move(v) }, std::move(ns), std::move(msg)));
}

std::unique_ptr<WriteOperation> Shard::write(Namespace ns, ChunkVersion v, bson::Object cmd)
{
    return std::unique_ptr<WriteOperation>(new WriteToBackend26(
        Config::VersionedShard { shared_from_this(), std::move(v) }, std::move(ns), std::move(cmd)));
}

Backend::SoftwareVersion Shard::softwareVersion() const
{
    typedef Backend::SoftwareVersion Version;
    
    static const Version NULL_VERSION;
    Version ret = NULL_VERSION;
    
    for (const std::unique_ptr<Backend>& b: backends_) {
        Version v = b->softwareVersion();
        if (v != NULL_VERSION && (ret == NULL_VERSION || ret < v))
            ret = v;
    }
    
    return ret;
}

bson::Object Shard::debugInspect() const
{
    bson::ArrayBuilder backends;
    for (const auto& backend: *this) {
        bson::ObjectBuilder b;
        b["addr"] = backend->addr();
        b["alive"] = backend->alive();
        if (backend->alive()) {
            b["roundtripMs"] = (int32_t) std::chrono::duration_cast<std::chrono::milliseconds>(backend->roundtrip()).count();
            b["status"] = backend->status();
        }
        backends << b.obj();
    }
    return bson::object("ok", 1, "id", id(), "connstr", connectionString(), "backends", backends.array());
}

namespace shard_types {

class Null: public Shard {
public:
    Null(): Shard({}, {}) {}
    Connection readOp(uint32_t, const bson::Object&, Backend*) override { return {}; }
};

class Single: public Shard {
public:
    explicit Single(const std::string& addr): Shard({}, { addr }) {}
    
    Connection readOp(uint32_t, const bson::Object&, Backend* exclude) override
    {
        if (backend()->alive() && backend() != exclude)
            return backend()->endpoint()->getPrimary();
        else
            return {};
    }
    
    Connection primary() override
    {
        if (backend()->alive())
            return backend()->endpoint()->getPrimary();
        else
            return {};
    }
    
    monitoring::Status status() const override
    {
        if (backend()->alive())
            return monitoring::Status::ok();
        else
            return monitoring::Status::critical(backend()->addr() + " is dead");
    }
    
private:
    Backend* backend() const { return begin()->get(); }
};


class Multiple: public Shard {
protected:
    Multiple(std::vector<PingQuery> pingQueries, const std::vector<std::string>& backends):
        Shard(std::move(pingQueries), backends),
        byRoundtrip_([this]{ return calcByRoundtrip(); })
    {}
    
    Lazy< std::vector<Backend*> >::Reference byRoundtrip() { return byRoundtrip_.get(); }
    
    void backendUpdated(Backend*) override { byRoundtrip_.clear(); }
    void onFailure(Backend*) override { byRoundtrip_.clear(); }
    
    template<class Pred>
    Backend* selectLocal(Pred pred)
    {
        std::vector<Backend*> candidates;
        auto byrt = byRoundtrip();
        for (Backend* b: byrt.value()) {
            if (pred(b))
                candidates.push_back(b);
        }

        if (candidates.empty())
            return 0;

        auto threshold = candidates.front()->roundtrip() + options().localThreshold;
        auto it = candidates.begin();
        for (; it != candidates.end() && (*it)->roundtrip() < threshold; ++it) {}
        if (it == candidates.begin())
            it = candidates.end();
        
        unsigned i = rand() * std::distance(candidates.begin(), it) / ((unsigned long long) RAND_MAX + 1);
        return candidates[i];
    }

private:
    Lazy< std::vector<Backend*> > byRoundtrip_;

    std::vector<Backend*> calcByRoundtrip()
    {
        std::vector<std::pair<std::chrono::microseconds, Backend*> > backends;
        
        // Need to save roundtrip times for all backends since they can change
        // during std::sort()
        for (const auto& b: *this)
            backends.push_back(std::make_pair(b->roundtrip(), b.get()));
        
        std::sort(backends.begin(), backends.end());
        
        std::vector<Backend*> ret;
        for (const auto& pair: backends)
            ret.push_back(pair.second);
        return ret;
    }

};


static const std::vector<Shard::PingQuery> RS_PING_QUERIES = {
    { "status", Namespace("admin", "$cmd"), bson::object("replSetGetStatus", 1) },
    { "conf", Namespace("local", "system.replset"), bson::object() }
};

class ReplicaSet: public Multiple {
private:
    typedef decltype(Options::maxReplLag) Optime;
    struct BackendInfo {
        bson::Object tags;
        Optime optime = Optime::max();
    };
    typedef std::map<const Backend*, BackendInfo> BackendInfoMap;

public:
    ReplicaSet(std::string name, const std::vector<std::string>& addrs):
        Multiple(RS_PING_QUERIES, addrs), name_(std::move(name)),
        primary_([this]{ return calcPrimary(); }),
        lostPrimarySince_(SteadyClock::now())
    {}    
    
    Connection readOp(uint32_t queryFlags, const bson::Object& readPref, Backend* exclude) override
    {
        waitForPings();
        
        std::string mode = "primary";
        bson::Array tags;
        
        if (!readPref.empty()) {
            mode = readPref["mode"].as<std::string>();
            tags = readPref["tags"].as<bson::Array>(bson::Array());
        } else if (queryFlags & messages::Query::SLAVE_OK) {
            mode = "nearest";
        }
        
        if (mode == "primary" || mode == "primaryPreferred") {
            Backend* p = primary_.get().value();
            if (p) {
                DEBUG(2) << "current primary for shard " << id() << ": " << p->addr();
            } else {
                DEBUG(2) << "shard " << id() << " has no primary";
            }
            
            if (mode == "primary" || (p != exclude && p && tagsMatch(p, tags))) {
                
                // On `primary' read preference previous primary node might have been
                // re-elected as primary node again. Since after primary failure
                // we've issued an emergency ping and now we are sure that `p' is
                // the new primary node, we ignore `exclude' and return `p'.
                
                if (p) {
                    DEBUG(2) << "Selecting " << p->addr() << " for operation";
                    return p->endpoint()->getPrimary();
                } else {
                    DEBUG(2) << "No backend suitable for operation";
                    return Connection();
                }
            }
        }
        
        // We are not distinguishing `secondary', `secondaryPreferred' and `nearest'.
        std::map<const Backend*, BackendInfo> info;
        if (options().maxReplLag != decltype(options().maxReplLag)::max() || !tags.empty())
            info = backendInfo();
        
        Optime optimeThreshold = options().maxReplLag != decltype(options().maxReplLag)::max()
            ? this->maxOptime(info) - options().maxReplLag
            : Optime(0);
        
        Backend* b = selectLocal([this, &tags, exclude, &info, optimeThreshold](Backend* b){
            auto i = info.find(b);
            return isHealthy(b) && b != exclude && (info.empty() || (i != info.end()
                    && tagsMatch(i->second.tags, tags)
                    && i->second.optime >= optimeThreshold
                ));
        });
        if (b) {
            DEBUG(2) << "Selecting " << b->addr() << " for operation";
            return b->endpoint()->getAny();
        } else {
            DEBUG(2) << "No backend suitable for operation";
            return Connection();
        }
    }
    
    Connection primary() override
    {
        waitForPings();
        Backend* p = primary_.get().value();
        return p ? p->endpoint()->getPrimary() : Connection();
    }
    
    void backendUpdated(Backend* b) override
    {
        Multiple::backendUpdated(b);
        
        bool isPrimary = this->isPrimary(b);
        if (isPrimary) {
            primary_.assign(b);
            lostPrimarySince_ = decltype(lostPrimarySince_)::max();
        } else if (b == primary_.get().value()) {
            lostMaster();
        }
        
        if (b->alive()) {
            bson::Object status = b->status();
            bson::Object self = find(status["status"]["members"], "self", bson::Any());
            std::string id = self["name"].as<std::string>("");
            bson::Time optime = self["optimeDate"].as<bson::Time>(bson::Time(0));
            bson::Object tags = find(status["conf"]["members"], "host", id)["tags"].as<bson::Object>(bson::Object());
        
            auto lock = uniqueLock();
            auto& info = backendInfo_[b];
            info.tags = std::move(tags);
            info.optime = std::chrono::duration_cast<Optime>(std::chrono::milliseconds(optime.milliseconds()));
        }
    }
    
    void onFailure(Backend* b) override
    {
        bool lostMaster = false;
        {
            auto primary = primary_.cached();
            if (primary.exists() && b == primary.value())
                lostMaster = true;
        }
        if (lostMaster)
            this->lostMaster();
        Multiple::onFailure(b);
    }
    
    void lostMaster() override
    {
        DEBUG(2) << "Shard " << id() << " lost its primary node; will re-ping";
        primary_.clear();
        if (lostPrimarySince_ == decltype(lostPrimarySince_)::max())
            lostPrimarySince_ = SteadyClock::now();
        pingNow();
    }
    
    std::string status(const Backend* b) const override
    {
        waitForPings();
        if (!isHealthy(b))
            return "DEAD";
        for (const bson::Element& elt: b->status()["status"]["members"].as<bson::Array>()) {
            if (elt["self"].exists())
                return elt["stateStr"].as<std::string>();
        }
        return "UNKNOWN";
    }
    
    std::chrono::milliseconds replicationLag(const Backend* b) const override
    {
        waitForPings();
        if (!isHealthy(b))
            return Optime::max();
        
        auto info = backendInfo();
        Optime maxOptime = this->maxOptime(info);
        auto i = info.find(b);
        if (i != info.end())
            return std::chrono::duration_cast<std::chrono::milliseconds>(maxOptime - i->second.optime);
        else
            return std::chrono::milliseconds::max();
    }
    
    monitoring::Status status() const override
    {
        waitForPings();
        std::vector<std::string> msgs;
        bool hasAliveMember = false;
        bool hasPrimary = false;
        
        monitoring::Status ret;
        
        auto info = backendInfo();
        Optime optimeThreshold = options().maxReplLag != decltype(options().maxReplLag)::max()
            ? this->maxOptime(info) - options().maxReplLag
            : Optime(0);

        for (const std::unique_ptr<Backend>& backend: *this) {
            auto i = info.find(backend.get());
            if (!backend->alive() || !isHealthy(backend.get())) {
                ret.merge(monitoring::Status::warning(backend->addr() + " is dead"));
            } else if (!backend->permanentErrmsg().empty()) {
                ret.merge(monitoring::Status::critical(
                    backend->addr() + " is permanently half-alive: " + backend->permanentErrmsg()
                ));
            } else if (i == info.end() || i->second.optime < optimeThreshold) {
                ret.merge(monitoring::Status::warning(backend->addr() + "'s replication lag exceeds threshold"));
            } else {
                hasAliveMember = true;
                if (isPrimary(backend.get()))
                    hasPrimary = true;
            }
        }
        
        if (!hasPrimary) {
            if (options().monitorNoPrimary != decltype(Options::monitorNoPrimary)::max()
                && SteadyClock::now() >= lostPrimarySince_ + options().monitorNoPrimary)
            {
                ret.merge(monitoring::Status::critical(
                    "replica set " + id() + " has no primary member for "
                    + std::to_string( std::chrono::duration_cast<std::chrono::minutes>(SteadyClock::now() - lostPrimarySince_).count() )
                    + " min"
                ));
            } else {
                ret.merge(monitoring::Status::warning("replica set " + id() + " has no primary member"));
            }
        }
        
        if (!hasAliveMember)
            ret.merge(monitoring::Status(monitoring::Status::CRITICAL));

        return ret;
    }
    
private /*methods*/:
    
    std::unique_lock<io::sys::shared_mutex> uniqueLock() { return std::unique_lock<io::sys::shared_mutex>(mutex_); }
    io::shared_lock<io::sys::shared_mutex> sharedLock() const { return io::shared_lock<io::sys::shared_mutex>(mutex_); }
    
    template<class T>
    bson::Object find(const bson::Element& objs, const char* key, const T& value)
    {
        if (!objs.exists() || !objs.is<bson::Array>())
            return bson::Object();
        
        for (bson::Element elt: objs.as<bson::Array>()) {
            bson::Object obj = elt.as<bson::Object>();
            bson::Element k = obj[key];
            if (k.exists() && k.canBe<T>() && k.as<T>() == value)
                return obj;
        }
        return bson::Object();
    }
    
    Backend* calcPrimary()
    {
        auto i = std::find_if(begin(), end(), [](const std::unique_ptr<Backend>& b) {
            return isPrimary(b.get());
        });
        
        if (i != end()) {
            lostPrimarySince_ = decltype(lostPrimarySince_)::max();
            return i->get();
        } else {
            return 0;
        }
    }
    
    bool tagsMatch(const Backend* b, const bson::Array& criteria)
    {
        return criteria.empty() || tagsMatch(backendInfo(b).tags, criteria);
    }
    
    bool tagsMatch(const bson::Object& tags, const bson::Array& criteria)
    {
        return criteria.empty() || std::any_of(criteria.begin(), criteria.end(), [&tags](const bson::Element& tagEl) {
            bson::Object tag = tagEl.as<bson::Object>();
            return std::all_of(tag.begin(), tag.end(),
                [&tags](const bson::Element& el) { return el == tags[el.name()]; });
        });
    }

    static bool isPrimary(const Backend* b) { return b->alive() && b->status()["status"]["myState"].as<int>(0) == 1; }

    static bool isHealthy(const Backend* b)
    {
        if (!b->alive())
            return false;
        int status = b->status()["status"]["myState"].as<int>(0);
        return status == 1 || status == 2;
    }
    
    BackendInfo backendInfo(const Backend* b) const
    {
        auto lock = sharedLock();
        auto i = backendInfo_.find(b);
        return i != backendInfo_.end() ? i->second : BackendInfo();
    }
    
    std::map<const Backend*, BackendInfo> backendInfo() const
    {
        auto lock = sharedLock();
        return backendInfo_;
    }
    
    Optime maxOptime(const BackendInfoMap& info) const
    {
        auto i = std::max_element(
            info.begin(), info.end(),
            [](const BackendInfoMap::value_type& a, const BackendInfoMap::value_type& b) {
                return a.second.optime < b.second.optime;
            }
        );
        return (i != info.end()) ? i->second.optime : Optime::max();
    }
    
    void pingNow()
    {
        if (ping_.completed())
            ping_ = io::spawn([this]{ 
                io::for_each(begin(), end(), [](const std::unique_ptr<Backend>& b) { b->pingNow(); });
            });
    }
    
    void waitForPings() const
    {
        if (!ping_.completed()) {
            DEBUG(2) << "emergency ping still in progress; waiting";
        }
        io::wait(ping_);
    }
    
private /*fields*/:
    std::string name_;
    mutable io::sys::shared_mutex mutex_;
    Lazy<Backend*> primary_;
    std::map<const Backend*, BackendInfo> backendInfo_;
    SteadyClock::time_point lostPrimarySince_;
    mutable io::task<void> ping_;
};


class Sync: public Multiple {
public:
    explicit Sync(const std::vector<std::string>& addrs):
        Multiple({}, addrs)
    {}
    
    Connection readOp(const uint32_t /*queryFlags*/, const bson::Object& /*readPreference*/, Backend* exclude)
    {
        Backend* b = selectLocal([exclude](const Backend* b) { return b != exclude; });
        return b ? b->endpoint()->getAny() : Connection();
    }
};

} // namespace shard_types



std::unique_ptr<Shard> Shard::make(const std::string& id, const std::string& connstr)
{
    std::string replset;
    std::vector<std::string> members;
    std::unique_ptr<Shard> ret;
    
    if (connstr.empty())
        return std::unique_ptr<Shard>(new shard_types::Null);
    
    size_t cur = 0;
    size_t slash = connstr.find('/');
    if (slash != std::string::npos) {
        replset = connstr.substr(0, slash);
        cur = slash + 1;
    }
    
    for (size_t comma = 0; (comma = connstr.find(',', cur)) != std::string::npos; cur = comma + 1)
        members.push_back(connstr.substr(cur, comma - cur));
    members.push_back(connstr.substr(cur));
    
    if (replset.empty() && members.size() == 1)
        ret = std::unique_ptr<Shard>(new shard_types::Single(members.front()));
    else if (!replset.empty())
        ret = std::unique_ptr<Shard>(new shard_types::ReplicaSet(replset, members));
    else if (replset.empty() && members.size() > 1)
        ret = std::unique_ptr<Shard>(new shard_types::Sync(members));
    else
        throw std::runtime_error("bad connection string: " + connstr);
    
    ret->id_ = id;
    ret->connstr_ = connstr;
    
    return ret;
}
