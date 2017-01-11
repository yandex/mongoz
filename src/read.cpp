/**
 * read.cpp -- query implementation
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

#include "read.h"
#include "shard.h"
#include "backend.h"
#include "error.h"
#include "parallel.h"
#include "utility.h"
#include "clock.h"
#include "options.h"
#include "config.h"
#include <syncio/syncio.h>
#include <cstdlib>

BackendDatasource::BackendDatasource(std::shared_ptr<Shard> shard, ChunkVersion version, messages::Query msg):
    shard_(std::move(shard)), version_(std::move(version)), msg_(std::move(msg))
{
    conn_ = shard_->readOp(msg_.flags, msg_.readPreference());
    if (!conn_.exists())
        throw errors::NoSuitableBackend("no backend suitable for operation on shard " + shard_->id());
    
    reqID_ = rand();
    
    DEBUG(1) << "Requesting initial portition of data";
    talk([this](uint32_t reqID) { return makeQuery(reqID); });
}

BackendDatasource::~BackendDatasource() {}

void BackendDatasource::doClose()
{
    if (!conn_.exists())
        return;

    if (cursorID_ != 0) {
        io::task<void> t = io::spawn([this]{
            MsgBuilder b;
            b << (uint32_t) 0 << (uint32_t) 0 << Opcode::KILL_CURSORS
              << (uint32_t) 0 << (uint32_t) 1 << cursorID_;
            
            if (!(conn_.stream().write(b.data(), b.size()).flush()))
                throw io::error("cannot send OP_KILL_CURSORS");
        });
        io::wait(t, 20_ms);
        if (!t.succeeded())
            conn_ = Connection();
    }
    
    if (conn_.exists())
        conn_.release();
}

std::vector<char> BackendDatasource::makeQuery(uint32_t reqID)
{
    QueryComposer q(msg_.ns, msg_.query);
    q.msgID(reqID);
    q.skip(pos());
    q.batchSize(msg_.nToReturn == 1 ? 1 : 0);
    q.fieldSelector(msg_.fieldSelector);
    
    if (
        msg_.readPreference()["mode"].as<std::string>("primary") != "primary"
        || (msg_.readPreference().empty() && (msg_.flags & messages::Query::SLAVE_OK))
    ) {
        q.slaveOK();
    }
    
    return q.data();
}

void BackendDatasource::requestMore()
{
    DEBUG(1) << "Need to request more data";
    auto makeRequestMore = [this](uint32_t reqID) -> std::vector<char> {
        MsgBuilder b;
        b << reqID << (uint32_t) 0 << Opcode::GET_MORE
          << (uint32_t) 0 << msg_.ns.ns() << (int32_t) 0 << cursorID_;
        return b.finish();
    };
    
    talk(makeRequestMore);
}

namespace {
    struct Reply {
        std::vector<bson::Object> objects;
        uint64_t cursorID;
        
        Reply() {}
        Reply(Reply&&) = default;
        Reply(const Reply&) = delete; // don't need 'em anyway
    };

} // namespace

Namespace BackendDatasource::ns() const
{
    if (msg_.ns.collection() == "$cmd" && !msg_.query.empty())
        return Namespace(msg_.ns.db(), msg_.query.begin()->as<std::string>());
    else
        return msg_.ns;
}

void BackendDatasource::talk(std::function<std::vector<char>(uint32_t)> msgMaker)
{
    bson::Object readPref = msg_.readPreference();
    
    auto readReply = [this](io::stream& s, uint32_t reqID) {
        Reply r;
        r.cursorID = ::readReply(s, reqID, [this, &r](bson::Object obj) { r.objects.push_back(std::move(obj)); });
        DEBUG(1) << "Returned " << r.objects.size() << " objects and cursor " << r.cursorID;
        return r;
    };

    io::task<Reply> t1, t2;
    Backend *b1 = 0, *b2 = 0;
    
    auto handleErrors = [this, &t1, &t2, &b1, &b2](io::task<Reply>& t) -> Reply {
        Backend* b = &t == &t1 ? b1 : b2;
        try {
            return t.get();
        }
        catch (errors::NotMaster&) {
            shard_->lostMaster();
            throw;
        }
        catch (errors::BackendClientError&) {
            throw;
        }
        catch (std::exception&) {
            shard_->failed(b);
            throw;
        }
        
    };

    auto useReply = [this, handleErrors](io::task<Reply>& t) {
        Reply r = handleErrors(t);
        objects_ = std::move(r.objects);
        current_ = objects_.begin();
        cursorID_ = r.cursorID;
    };

    SteadyClock::time_point startedAt = SteadyClock::now();
    
    io::timeout retransmit(std::chrono::milliseconds(readPref["retransmitMs"].as<unsigned>(options().readRetransmit.count())));
    io::timeout timeout(std::chrono::milliseconds(readPref["timeoutMs"].as<unsigned>(options().readTimeout.count())));
    
    uint32_t reqID = makeReqID();
    
    TaskPool<Reply> pool;
    Backend* b = b1 = &conn_.backend();
    t1 = io::spawn([this, reqID, msgMaker, readReply]{
        
        Connection c = std::move(conn_);
        std::vector<char> msg = msgMaker(reqID);

        DEBUG(1) << "Starting communicating with endpoint " << c.endpoint();
        c.establish(ns(), version_, msg.data(), msg.size());
        DEBUG(1) << "Sent query to " << c.endpoint();

        Reply reply = readReply(c.stream(), reqID);
        conn_ = std::move(c);
        return reply;
        
    });
    pool.add(t1);
    
    io::task<Reply>* t = pool.wait(std::min(retransmit, timeout));
    
    if (t) {
        try {
            useReply(*t);
            DEBUG(1) << "Query took " << std::chrono::duration_cast<std::chrono::milliseconds>(SteadyClock::now() - startedAt).count() << " ms";
            return;
        }
        catch (errors::NotMaster&) {}
        catch (errors::BackendClientError&) { throw; }
        catch (std::exception&) {}
        *t = io::task<Reply>();
    }
    
    Connection c2;
    if (retransmit.finite() && (c2 = shard_->readOp(msg_.flags, msg_.readPreference(), b)).exists()) {
        b2 = &c2.backend();
        DEBUG(1) << "Retransmitting query to " << c2.endpoint();
        t2 = io::spawn([this, reqID, readReply](Connection c2){
            std::vector<char> q = makeQuery(reqID);

            DEBUG(1) << "(retransmit) Starting communicating with endpoint " << c2.endpoint();
            c2.establish(ns(), version_, q.data(), q.size());
            DEBUG(1) << "(retransmit) Sent query to " << c2.endpoint();

            Reply reply = readReply(c2.stream(), reqID);
            conn_ = std::move(c2);
            return reply;
        }, std::move(c2));
        pool.add(t2);
    }
    
    t = pool.wait(timeout);
    
    if (t) {
        useReply(*t);
        DEBUG(1) << "Query took " << std::chrono::duration_cast<std::chrono::milliseconds>(SteadyClock::now() - startedAt).count() << " ms";
        return;
    }
        
    if (t1.failed())
        handleErrors(t1);
    if (t2.failed())
        handleErrors(t2);
    throw io::error("timeout while talking to shard " + shard_->connectionString(), ETIMEDOUT);
}



namespace {
class CompareBsons {
public:
    explicit CompareBsons(const bson::Object& orderBy): orderBy_(&orderBy) {}
    
    bool operator()(const bson::Object& a, const bson::Object& b) const
    {
        for (bson::Element el: *orderBy_) {
            bson::Element ea = a[el.name()];
            bson::Element eb = b[el.name()];
            
            int direction = el.as<int>();
            if (ea < eb)
                return direction < 0;
            else if (ea > eb)
                return direction > 0;
        }
        return false;
    }
    
    bool operator()(const DataSource* a, const DataSource* b) const
    {
        if (a->atEnd() && b->atEnd())
            return false;
        else if (b->atEnd())
            return false;
        else if (a->atEnd())
            return true;
        else
            return (*this)(a->get(), b->get());
    }
    
    template<class DS1, class DS2>
    bool operator()(const std::unique_ptr<DS1>& a, const std::unique_ptr<DS2>& b) const
    {
        return (*this)(a.get(), b.get());
    }
    
private:
    const bson::Object* orderBy_;
};
}


MergeDatasource::MergeDatasource(messages::Query query, std::vector<Config::VersionedShard> shards):
    msg_(std::move(query)),
    orderBy_(msg_.properties["$orderby"].as<bson::Object>(bson::Object()))
{
    std::vector< io::task< std::unique_ptr<BackendDatasource> > > tasks;
    for (Config::VersionedShard& vs: shards) {
        tasks.push_back(io::spawn([this](Config::VersionedShard& vs) {
            return std::unique_ptr<BackendDatasource>(new BackendDatasource(vs.shard, std::move(vs.version), msg_));
        }, vs));
    }

    for (auto& t: tasks) {
        std::unique_ptr<BackendDatasource> ds;
        if (protect([&ds, &t]{ ds = t.join(); }) && !ds->atEnd())
            datasources_.push_back(std::move(ds));
    }
        
    std::make_heap(datasources_.begin(), datasources_.end(), CompareBsons(orderBy_));
}

MergeDatasource::~MergeDatasource() {}


void MergeDatasource::doAdvance()
{
    CompareBsons cmp(orderBy_);
    std::pop_heap(datasources_.begin(), datasources_.end(), cmp);
    
    if (!protect([this]{ datasources_.back()->advance(); })) {
        datasources_.pop_back();
    } else if (datasources_.back()->atEnd()) {
        datasources_.back()->close();
        datasources_.pop_back();
    } else {
        std::push_heap(datasources_.begin(), datasources_.end(), cmp);
    }
}

void MergeDatasource::doClose()
{
    for (auto& ds: datasources_)
        ds->close();
}


namespace {

bson::Object runCommand(Config::VersionedShard vs, const messages::Query& q)
{    
    BackendDatasource ds(std::move(vs.shard), std::move(vs.version), q);
    ASSERT(!ds.atEnd());
    bson::Object ret = ds.get();
    ds.close();
    
    return ret;
}

template<class Ret>
inline Ret readOp(
    const Namespace& ns, const bson::Object& criteria,
    std::function<Ret()> null,
    std::function<Ret(Config::VersionedShard)> single,
    std::function<Ret(std::vector<Config::VersionedShard>)> multi
){
    std::exception_ptr ex;
    for (size_t attempt = 0; attempt != 3; ++attempt) {
        try {            
            std::shared_ptr<Config> config = g_config->get();
            std::vector<Config::VersionedShard> shards = config->find(ns, criteria);
            
            if (shards.empty()) {
                DEBUG(2) << "query has no shards to run on";
                return null();
            } else if (shards.size() == 1) {
                DEBUG(2) << "query goes to a single shard";
                return single(std::move(shards.front()));
            } else {
                DEBUG(2) << "query goes to " << shards.size() << " shards";
                return multi(std::move(shards));
            }            
        }
        catch (errors::ShardConfigStale& e) {
            ex = std::current_exception();
            INFO() << e.what() << "; updating config";
            g_config->update();
        }
        catch (errors::NotMaster& e) {
            ex = std::current_exception();
            INFO() << e.what() << "; re-executing query";
        }
    }
    
    std::rethrow_exception(ex); // Give up
}

template<class Aggr>
bson::Object aggregation(const messages::Query& q, const auth::Privileges& privileges, Aggr aggr)
{
    privileges.require(q.ns.db(), auth::Privilege::READ);
    Namespace ns(q.ns.db(), q.query.begin()->as<std::string>());

    return readOp<bson::Object>(
        ns, q.query["query"].as<bson::Object>(bson::Object()),
        [&aggr]() {
            bson::ObjectBuilder b;
            aggr(std::vector<bson::Object>(), b);
            b["ok"] = 1;
            return b.obj();
        },
        [&q](Config::VersionedShard vs) { return runCommand(std::move(vs), q); },
        [&q, &aggr](std::vector<Config::VersionedShard> shards) {
            
            std::vector<bson::Object> rets;
            io::transform(shards.begin(), shards.end(), std::back_inserter(rets),
                [&q](const Config::VersionedShard& vs) { return runCommand(vs, q); });

            for (const bson::Object& ret: rets)
                if (ret["ok"].as<int>(0) != 1)
                    throw errors::BackendClientError(ret["errmsg"].as<std::string>("unknown error"));

            bson::ObjectBuilder b;
            aggr(rets, b);
            b["ok"] = 1;
            return b.obj();            
        }
    );
}

} // namespace

namespace operations {

std::unique_ptr<DataSource> query(const messages::Query& query, const auth::Privileges& privileges)
{   
    static const uint32_t ALLOWED_FLAGS =
          messages::Query::SLAVE_OK
        | messages::Query::EXHAUST
        | messages::Query::NO_TIMEOUT
        | messages::Query::PARTIAL;

    if (query.flags & ~ALLOWED_FLAGS)
        throw errors::BadRequest("specified flags are not supported");
    if (query.properties["$explain"].exists())
        throw errors::BadRequest("$explain is not supported");
    
    if (query.ns.collection() == "system.users")
        privileges.require(query.ns.db(), auth::Privilege::USER_ADMIN);
    else
        privileges.require(query.ns.db(), auth::Privilege::READ);

    return readOp< std::unique_ptr<DataSource> >(
        query.ns, query.criteria,
        []() { return std::unique_ptr<DataSource>(new NullDatasource); },
        [&query](Config::VersionedShard vs) {
            return std::unique_ptr<DataSource>(
                new BackendDatasource(std::move(vs.shard), std::move(vs.version), query));
        },
        [&query](std::vector<Config::VersionedShard> shards) {
            return std::unique_ptr<DataSource>(new MergeDatasource(query, std::move(shards)));
        }
    );
}

bson::Object count(const messages::Query& q, const auth::Privileges& privileges)
{
    return aggregation(q, privileges, [](const std::vector<bson::Object>& objs, bson::ObjectBuilder& b) {
        b["n"] = std::accumulate(objs.begin(), objs.end(), (int64_t) 0,
            [](int64_t a, const bson::Object& x) { return a + x["n"].as<int64_t>(); });
    });
}

bson::Object distinct(const messages::Query& q, const auth::Privileges& privileges)
{
    return aggregation(q, privileges, [](const std::vector<bson::Object>& objs, bson::ObjectBuilder& b) {

        std::vector<bson::Element> values;
        for (const bson::Object& obj: objs)
            for (const bson::Element& elt: obj["values"].as<bson::Array>())
                values.push_back(elt);

        std::sort(values.begin(), values.end(),
            [](const bson::Element& a, const bson::Element& b) { return a.stripName() < b.stripName(); });
        values.erase(
            std::unique(
                values.begin(), values.end(),
                [](const bson::Element& a, const bson::Element& b) { return a.stripName() == b.stripName(); }
            ), values.end()
        );

        bson::ArrayBuilder ret;
        for (const bson::Element& elt: values)
            ret << elt;
        b["values"] = ret.array();
    });
}

} // namespace operations
