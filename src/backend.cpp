/**
 * backend.cpp -- an interface to a specific MongoDB backend server.
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

#include "backend.h"
#include "shard.h"
#include "proto.h"
#include "clock.h"
#include "log.h"
#include "config.h"
#include "options.h"
#include "auth.h"
#include "utility.h"
#include <bson/bson11.h>
#include <cassert>

Connection::~Connection()
{
    if (exists())
        DEBUG(3) << "Closing connection to " << endpoint().addr();
}

void Connection::establish(const Namespace& ns, const ChunkVersion& v, const char* msg, size_t msglen)
{
    assert(exists());

    if (stream().rdbuf()) {
        authenticate();
        if (impl_->isPrimary && !ns.empty() && v.stamp() != bson::Timestamp())
            trySetVersion(ns, v);
        
        stream().write(msg, msglen).flush();
        if (stream())
            return;
        
        endpoint().flush();
    }
    
    impl_->versions.clear();
    stream() = io::stream(io::connect(endpoint().addr()));
    authenticate();
    if (impl_->isPrimary && !ns.empty() && v.stamp() != bson::Timestamp())
        trySetVersion(ns, v);
    stream().write(msg, msglen).flush();
    
    if (!stream()) {
        throw io::error(
            "cannot communicate with " + backend().addr()
            + " (" + toString(endpoint().addr()) + ")"
        );
    }
}

void Connection::stepDown(std::chrono::seconds duration)
{
    stream() << QueryComposer(Namespace("admin", "$cmd"), bson::object(
        "replSetStepDown", duration.count(),
        "force", true
    )).batchSize(1) << std::flush;
    readReply(stream(), 0);
}

void Connection::authenticate()
{
    auto cmdOk = [](const bson::Object& ret) -> bool {
        bson::Element ok = ret["ok"];
        return ok.exists() && ok.canBe<int>() && ok.as<int>() == 1;
    };
    
    if (impl_->authenticated || auth::sharedSecret().empty())
        return;
    
    DEBUG(1) << "Authenticating in " << endpoint().addr();
    
    stream() << QueryComposer(Namespace("local", "$cmd"), bson::object("getnonce", 1)).batchSize(1) << std::flush;
    if (!stream())
        return; // establish() will take care of error handling
    
    bson::Object ret = readReply(stream(), 0);
    DEBUG(3) << "Received nonce: " << ret;
    if (!cmdOk(ret))
        throw errors::BackendInternalError(backend().addr(), ret["err"].as<std::string>("unknown error"));
    
    std::string nonce = ret["nonce"].as<std::string>();
    
    stream() << QueryComposer(Namespace("local", "$cmd"), bson::object(
        "authenticate", 1,
        "user", "__system",
        "nonce", nonce,
        "key", auth::makeAuthKey(nonce, "__system", auth::sharedSecret())
    )).batchSize(1) << std::flush;
    ret = readReply(stream(), 0);
    DEBUG(3) << "Received reply: " << ret;
    
    if (!cmdOk(ret))
        throw errors::BackendInternalError(backend().addr(), ret["errmsg"].as<std::string>("unknown error"));
    
    impl_->authenticated = true;
}

void Connection::trySetVersion(const Namespace& ns, const ChunkVersion& v)
{
    assert(exists());
    ChunkVersion& current = impl_->versions[ns.ns()];
    if (current == v)
        return;
    
    DEBUG(1) << "Updating shard version for " << ns << " on " << endpoint().addr()
             << " to " << v;
    
    for (unsigned attempt = 0; attempt != 2; ++attempt) {
    
        static const uint32_t reqID = 0x56544553; // "SETV"
        bson::ObjectBuilder b;
        b["setShardVersion"] = ns.ns();
        b["configdb"] = g_config->connectionString();
        b["version"] = v.stamp();
        b["versionEpoch"] = v.epoch();
        if (backend().softwareVersion() < Backend::SoftwareVersion { 3, 0 })
            b["serverID"] = serverID();
        b["shard"] = backend().shard()->id();
        b["shardHost"] = backend().shard()->connectionString();
        b["authoritative"] = true;
        
        stream() << QueryComposer(Namespace("admin", "$cmd"), b.obj()).msgID(reqID).batchSize(1) << std::flush;
        if (!stream())
            return;

        bson::Object ret = readReply(stream(), reqID);
        if (ret["ok"].as<int>()) {
            current = v;
            return;
        }
        std::string errmsg = ret["errmsg"].as<std::string>();
        if (errmsg == "not master") {
            throw errors::NotMaster();
        } else if (errmsg.find(":: 8002 all servers down") != std::string::npos) {
            DEBUG(1) << "mongod went crazy, will retry";
            continue;
        } else if (errmsg.find("sharding metadata manager failed to initialize") != std::string::npos) {
            ERROR() << backend().addr() << " permanently incapable of operating as master";
            backend().permanentlyFailed(errmsg);
            stepDown(3600_s);
            throw errors::PermanentFailure(backend().addr(), errmsg);
        } else if (errmsg.find("None of the hosts for replica set") != std::string::npos) {
            throw errors::ConnectivityError(backend().addr(), errmsg);
        } else {
            throw errors::ShardConfigStale(backend().addr(), errmsg);
        }
    }
}


Connection Endpoint::pop(std::vector<Connection>& v)
{
    std::unique_lock<io::sys::mutex> lock(mutex_);
    if (v.empty())
        return {};
    Connection ret = std::move(v.back());
    v.pop_back();
    return ret;
}

Connection Endpoint::get(std::vector<Connection>& v)
{
    Connection ret = pop(v);
    if (ret.exists()) {
        DEBUG(1) << "Using existing connection for " << addr_;
        return ret;
    } else {
        DEBUG(1) << "Creating new connection for " << addr_;
        return Connection(this, &v == &primaries_);
    }
}

void Endpoint::flush()
{
    DEBUG(1) << "Flushing all connections for " << addr_;
    std::unique_lock<io::sys::mutex> lock(mutex_);
    conns_.clear();
    primaries_.clear();
}

void Endpoint::release(Connection&& conn)
{
    if (!conn.exists())
        return;

    std::vector<Connection>* v = (conn.impl_->isPrimary ? &primaries_ : &conns_);
    std::unique_lock<io::sys::mutex> lock(mutex_);
    if (v->size() < options().connPoolSize) {
        DEBUG(3) << "Stashing connection to " << conn.endpoint().addr();
        v->push_back(std::move(conn));
    } else {
        DEBUG(3) << "Not stashing connection to " << conn.endpoint().addr() << ": connection pool full";
    }
}

void Endpoint::setAlive(
    std::chrono::microseconds netRoundtrip,
    std::chrono::microseconds grossRoundtrip, bson::Object obj
){
    LogMessage(!alive() ? LogMessage::NOTICE : 1)
        << backend_->addr() << " at " << addr_ << " alive (roundtrip = "
        << std::chrono::duration_cast<std::chrono::milliseconds>(netRoundtrip).count() << " ms net, "
        << std::chrono::duration_cast<std::chrono::milliseconds>(grossRoundtrip).count() << " ms gross)";
    
    pinged_ = true;
    prevRoundtrip_ = roundtrip_;
    roundtrip_ = netRoundtrip;
    backend_->endpointAlive(this, std::move(obj));
}

void Endpoint::setDead(const std::string& reason)
{
    LogMessage(!pinged_ || alive() ? LogMessage::NOTICE : 3) << backend_->addr() << " at " << addr_ << " dead: " << reason;
    
    pinged_ = true;
    prevRoundtrip_ = roundtrip_;
    roundtrip_ = std::chrono::microseconds::max();
    backend_->endpointDead(this);
    flush();
}

void Endpoint::failed()
{
    DEBUG(1) << "Initiating ping of " << backend_->addr() << " at " << addr_ << " due to backend failure";
    prevRoundtrip_ = roundtrip_;
    roundtrip_ = std::chrono::microseconds::max();
    flush();
    io::spawn([this] { pingNow(); }).detach();
}

bool Endpoint::pingNow()
{
    static const uint32_t REQ_ID = 0x474E4950; // "PING"    
    std::vector<Shard::PingQuery> queries = (backend_->shard_ ? backend_->shard_->pingQueries() : std::vector<Shard::PingQuery>{});
    queries.emplace_back(Shard::PingQuery { "build_info", Namespace("local", "$cmd"), bson::object("buildinfo", 1) });
    queries.emplace_back(Shard::PingQuery { "server_status", Namespace("admin", "$cmd"), bson::object("serverStatus", 1) });
    
    DEBUG(1) << "Pinging " << backend_->addr() << " on " << addr_;
    io::task<void> t = io::spawn([this, &queries]() {
        SteadyClock::time_point started = SteadyClock::now();
        bson::ObjectBuilder status;

        Connection c = getAny();
        c.establish(Namespace(), ChunkVersion(),
            QueryComposer(Namespace("local", "$cmd"), bson::object("ping", 1)).msgID(REQ_ID).batchSize(1).slaveOK());
        readReply(c.stream(), REQ_ID, [this](const bson::Object& obj) {
            if (obj["ok"].as<int>(0) != 1)
                throw std::runtime_error("negative reply to ping command");
        });
        
        SteadyClock::time_point firstResp = SteadyClock::now();

        uint32_t reqID = REQ_ID;
        for (const Shard::PingQuery& q: queries) {
            c.stream() << QueryComposer(q.ns, q.criteria).msgID(++reqID).batchSize(1).slaveOK() << std::flush;
            readReply(c.stream(), reqID, [&status, &q, this](const bson::Object& obj) {
                status[q.key] = obj;
            });
        }

        setAlive(firstResp - started, SteadyClock::now() - started, status.obj());
        c.release();
    });
    io::wait(t, options().pingTimeout);
    
    if (t.succeeded())
        return true;
    
    if (!t.completed()) {
        setDead("timeout");
        return false;
    }
    
    try {
        t.get();
    }
    catch (std::exception& e) {
        setDead(e.what());
    }
    return false;
}

void Endpoint::keepPing()
{
    for (;;) {
        if (pingNow()) {
            io::sleep(options().pingInterval);
        } else {
            io::sleep(options().pingFailInterval);
        }
    }
}

void Backend::endpointAlive(Endpoint* pt, bson::Object status)
{
    auto pid = [](const bson::Object& status) -> unsigned {
        auto elt = status["pid"];
        return elt.exists() && elt.canBe<unsigned>() ? elt.as<unsigned>() : 0;
    };

    {
        std::unique_lock<io::sys::shared_mutex> lock(mutex_);
        if (pid(status_) != pid(status))
            permanentErrmsg_.clear();
        status_ = std::move(status);
    }

    if (pt->roundtrip() < nearest_.get().value()->roundtrip())
        nearest_.assign(pt);
    
    if (shard_)
        shard_->backendUpdated(this);

    pinged_ = true;
}

void Backend::endpointDead(Endpoint* pt)
{
    if (pt->wasAlive()) {
        if (std::none_of(endpts_.begin(), endpts_.end(), [](const EndptPtr& pt) { return pt->alive(); })) {
            std::unique_lock<io::sys::shared_mutex> lock(mutex_);
            status_ = bson::Object();
        }
        nearest_.clear();

        if (shard_)
            shard_->backendUpdated(this);
    }
    
    pinged_ = true;
}

Endpoint* Backend::calcNearest() const
{
    auto i = std::min_element(endpts_.begin(), endpts_.end(),
        [](const EndptPtr& a, const EndptPtr& b) { return a->roundtrip() < b->roundtrip(); });
    assert(i != endpts_.end());
    return i->get();
}


void Backend::failed()
{
    DEBUG(1) << addr_ << " failed";
    {
        std::unique_lock<io::sys::shared_mutex> lock(mutex_);
        status_ = bson::Object();
    }
    for (const auto& endpt: endpts_)
        endpt->failed();
}

void Backend::permanentlyFailed(const std::string& errmsg)
{
    std::unique_lock<io::sys::shared_mutex> lock(mutex_);
    permanentErrmsg_ = errmsg;
}

void Backend::pingNow()
{
    io::for_each(endpts_.begin(), endpts_.end(), [](const EndptPtr& pt) { pt->pingNow(); });
}
