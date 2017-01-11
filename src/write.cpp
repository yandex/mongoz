/**
 * write.cpp -- insert, update and remove operations
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

#include "write.h"
#include "proto.h"
#include "config.h"
#include "utility.h"
#include "options.h"
#include <syncio/syncio.h>
#include <bson/bson11.h>
#include <unordered_map>

/// Compares two write concerns, using the following rules:
///  - `getLastError' does not matter anything;
///  -  missing `w' equals to `w: 1';
///  -  other keys must match each other.
bool WriteOperation::areWriteConcernsEqual(const bson::Object& w1, const bson::Object& w2)
{
    auto eltname = [](const bson::Element& elt) {
        std::string s = elt.name();
        std::transform(s.begin(), s.end(), s.begin(), &tolower);
        return s;
    };
    
    std::unordered_map<std::string, bson::Element> seen;
    bson::Element c1, c2;
    
    for (const bson::Element& elt: w1) {
        std::string name = eltname(elt);
        if (name == "getlasterror") {
            // do nothing
        } else if (name == "w") {
            c1 = elt;
        } else if (name == "wtimeout") {
            return false;
        } else {
            seen.insert(std::make_pair(name, elt));
        }
    }
    
    for (const bson::Element& elt: w2) {
        std::string name = eltname(elt);
        if (name == "getlasterror") {
            // do nothing
        } else if (name == "w") {
            c2 = elt;
        } else if (name == "wtimeout") {
            return false;
        } else {        
            auto i = seen.find(name);
            if (i == seen.end() || i->second != elt)
                return false;
            seen.erase(i);
        }
    }
    
    return seen.empty() && (
        (c1.is<std::string>() && c2.is<std::string>() && c1.as<std::string>() == c2.as<std::string>())
        || ((!c1.exists() || c1.canBe<int>()) && ((!c2.exists() || c2.canBe<int>())) && c1.as<int>(1) == c2.as<int>(1))
    );
}

const bson::Object& WriteOperation::setAcknowledge(const bson::Object& writeConcern, const bson::Object& status)
{
    lastWriteConcern_ = writeConcern;
    lastStatus_ = status;
    return lastStatus_;
}

namespace {

bson::Object validateAck(bson::Object ack)
{
    if (!ack["ok"].exists() || !ack["n"].exists())
        throw errors::BackendInternalError("bad write operation status");
    return ack;
}

bson::Object defaultAckMerger(const std::vector<bson::Object>& rets)
{
    bson::Element err;
    bson::Element code;
    size_t n = 0;
    bool hasUpdatedExisting = false;
    bool updatedExisting = false;
    bson::Element upserted;
    bool wtimeout = false;
    int32_t waited = 0;
    int32_t wtime = 0;
    
    if (rets.empty())
        return bson::Object();
    if (rets.size() == 1)
        return rets.front();
    
    for (const bson::Object& ret: rets) {
        for (const bson::Element& elt: ret) {
            if (!strcmp(elt.name(), "err") && (!err.exists() || err.is<bson::Null>()))
                err = elt;
            if (!strcmp(elt.name(), "code") && !code.exists())
                code = elt;
            if (!strcmp(elt.name(), "n"))
                n += elt.as<size_t>();
            if (!strcmp(elt.name(), "updatedExisting")) {
                hasUpdatedExisting = true;
                updatedExisting = updatedExisting || elt.as<bool>();
            }
            if (!strcmp(elt.name(), "upserted") && !upserted.exists())
                upserted = elt;
            if (!strcmp(elt.name(), "wtimeout"))
                wtimeout = wtimeout || elt.as<bool>();
            if (!strcmp(elt.name(), "waited"))
                waited = std::max(waited, elt.as<int32_t>());
            if (!strcmp(elt.name(), "wtime"))
                wtime = std::max(wtime, elt.as<int32_t>());
        }
    }
    
    bson::ObjectBuilder b;
    
    b["ok"] = (err.exists() && !err.is<bson::Null>()) ? 0 : 1;
    if (err.exists())
        b["err"] = err;
    if (code.exists())
        b["code"] = code;
    
    if (n > std::numeric_limits<int32_t>::max())
        b["n"] = (int64_t) n;
    else
        b["n"] = (int32_t) n;
    
    if (hasUpdatedExisting)
        b["updatedExisting"] = updatedExisting;
    if (upserted.exists())
        b["upserted"] = upserted;
    if (wtimeout)
        b["wtimeout"] = true;
    if (waited)
        b["waited"] = waited;
    if (wtime)
        b["wtime"] = wtime;
    
    return b.obj();
}

} // namespace

void WriteToBackend::perform()
{
    io::timeout timeout;
    if (options().writeTimeout != decltype(options().writeTimeout)::max())
        timeout = options().writeTimeout;
    
    for (size_t attempt = 0; !timeout.expired(); ++attempt) {
        
        c_ = vs_.shard->primary();
        if (!c_.exists()) {
            io::sleep(500_ms);
            continue;
        }
        
        io::task<bson::Object> t = io::spawn([this]{ return doPerform(c_); });
        io::wait(t, std::min(options().writeRetransmit, options().writeTimeout));
        
        if (t.completed()) {
            bson::Object result = t.join();
            if (
                !result["err"].exists() || result["err"].is<bson::Null>()
                || result["code"].as<int>(0) != 10058 /* not master */
            )
                return;
        
            vs_.shard->lostMaster();
            if (attempt != 0)
                io::sleep(500_ms);
        } else {
            WARN() << "timeout while writing to " << c_.backend().addr();
            t.cancel();
            io::wait(t);
            vs_.shard->failed(&c_.backend());
        }
    }
    
    throw errors::NoSuitableBackend("cannot communicate with primary for shard " + vs_.shard->connectionString());
}

void WriteToBackend::finish()
{
    if (c_.exists())
        c_.release();
}



WriteToBackend24::WriteToBackend24(Config::VersionedShard vs, Namespace ns, std::vector<char> msg):
    WriteToBackend(std::move(vs), std::move(ns)), msg_(std::move(msg))
{}

bson::Object WriteToBackend24::doPerform(Connection& c)
{
    DEBUG(1) << "Issuing 2.4 write command to " << c.backend().addr();
    c.establish(ns(), version(), msg_.data(), msg_.size());
    return acknowledge(bson::object("getLastError", 1));
}

bson::Object WriteToBackend24::doAcknowledge(Connection& c, const bson::Object& writeConcern)
{
    static const uint32_t REQ_ID = 0x0A4B4341; // "ACK\n"
    
    c.stream() << QueryComposer(Namespace(ns().db(), "$cmd"), writeConcern).msgID(REQ_ID).batchSize(1) << std::flush;
    return validateAck(readReply(c.stream(), REQ_ID));
}


WriteToBackend26::WriteToBackend26(Config::VersionedShard vs, Namespace ns, bson::Object cmd):
    WriteToBackend(std::move(vs), std::move(ns)), cmd_(std::move(cmd))
{}

bson::Object WriteToBackend26::doPerform(Connection& c)
{
    DEBUG(1) << "Issuing 2.6 write command to " << c.backend().addr();
    c.establish(ns(), version(), QueryComposer(Namespace(ns().db(), "$cmd"), cmd_));
    return setAcknowledge(cmd_["writeConcern"].as<bson::Object>(bson::object("w", 1)), validateAck(readReply(c.stream(), 0)));
}

bson::Object WriteToBackend26::doAcknowledge(Connection&, const bson::Object&)
{
    throw errors::BadRequest("cannot issue getLastError after 2.6 write command");
}


WriteFindAndModify::WriteFindAndModify(Config::VersionedShard vs, Namespace ns, bson::Object cmd):
    WriteToBackend(std::move(vs), std::move(ns)), cmd_(std::move(cmd))
{}

bson::Object WriteFindAndModify::doPerform(Connection& c)
{
    DEBUG(1) << "Issuing findAndModify command to " << c.backend().addr();
    c.establish(ns(), version(), QueryComposer(Namespace(ns().db(), "$cmd"), cmd_));
    return setAcknowledge(bson::Object(), readReply(c.stream(), 0));
}

bson::Object WriteFindAndModify::doAcknowledge(Connection&, const bson::Object&)
{
    throw errors::BadRequest("cannot issue getLastError after findAndModify");
}


MultiWrite::MultiWrite(bson::Object writeConcern):
    writeConcern_(std::move(writeConcern)),
    merge_(&defaultAckMerger)
{
    if (writeConcern_.empty())
        writeConcern_ = bson::object("getLastError", 1);
}

bson::Object MultiWrite::doAcknowledge(const bson::Object& writeConcern)
{
    std::vector<bson::Object> rets;
    io::transform(
        commenced_.begin(), commenced_.end(), std::back_inserter(rets),
        [&writeConcern](WriteOperation* op) { return op->acknowledge(writeConcern); }
    );
    return mergeAcks(rets);
}

void MultiWrite::finish()
{
    for (WriteOperation* op: commenced_)
        op->finish();
}

bool MultiWrite::isAcknowledgable() const
{
    return std::all_of(ops_.begin(), ops_.end(),
        [](const std::unique_ptr<WriteOperation>& op) { return op->isAcknowledgable(); });        
}


void ParallelWrite::perform()
{
    for (const std::unique_ptr<WriteOperation>& op: *this)
        commence(op.get());
    
    std::vector<bson::Object> acks;
    io::transform(
        begin(), end(), std::back_inserter(acks),
        [](const std::unique_ptr<WriteOperation>& op) -> bson::Object {
            op->perform();
            return op->lastStatus();
        }
    );

    setAcknowledge(writeConcern(), mergeAcks(acks));
}


void SequentialWrite::perform()
{
    std::vector<bson::Object> acks;
    
    for (const std::unique_ptr<WriteOperation>& op: *this) {
        commence(op.get());
        op->perform();
        bson::Object ack = op->lastStatus();
        acks.push_back(ack);
        
        bson::Element err = ack["err"];
        if (err.exists() && !err.is<bson::Null>())
            break;
        if (stop_ && stop_(ack))
            break;
    }
    
    setAcknowledge(writeConcern(), mergeAcks(acks));
}


namespace {

template<class Op>
std::unique_ptr<WriteOperation> upcast(std::unique_ptr<Op>& op)
{
    return std::unique_ptr<WriteOperation>(op.release());
}

template<class Op> struct WriteOpTraits {};

template<class Op, class Self> struct WriteOpTraitsBase {

    /// Constructs a write operation which dispatches all its sub-operations (`subops')
    /// onto a single shard (`vs').
    static std::unique_ptr<WriteOperation> makeLocal(
        Config::VersionedShard vs, const Namespace& ns,
        const std::vector<typename Op::Subop> subops,
        const bson::Object& writeConcern
    ){
        if (vs.shard->supportsWriteCommands() && !writeConcern.empty()) {
            return make26(std::move(vs), ns, subops.data(), subops.size(), writeConcern);
        } else if (subops.size() == 1) {
            return Self::make24(std::move(vs), ns, subops.front());
        } else {
            
            auto ws = make_unique<ParallelWrite>(writeConcern);
            for (const typename Op::Subop& subop: subops)
                ws->add(Self::make24(vs, ns, subop));
            return upcast(ws);
        }
    }
    
    /// Constructs a write operation which runs a single `subop' on all `shards'
    /// sequentially, with optional limit.
    static std::unique_ptr<WriteOperation> makeGlobal(
        std::vector<Config::VersionedShard> shards, const Namespace& ns,
        const typename Op::Subop& subop, const bson::Object& writeConcern
    ){
        if (Self::limit(subop) != 0 && Self::limit(subop) != 1)
            throw errors::NotImplemented("Limit greater than one is not implemented");

        auto makeSingle = [&ns, &subop, &writeConcern](Config::VersionedShard vs) -> std::unique_ptr<WriteOperation> {
            if (vs.shard->supportsWriteCommands() && !writeConcern.empty())
                return make26(std::move(vs), ns, &subop, 1, writeConcern);
            else
                return Self::make24(std::move(vs), ns, subop);
        };
        
        if (shards.size() == 1)
            return makeSingle(std::move(shards.front()));
            
        auto ws = make_unique<SequentialWrite>(writeConcern);
        if (Self::limit(subop) != 0)
            ws->stopAtFirstThat([](const bson::Object& ack) { return ack["n"].as<size_t>(0) != 0; });
        
        auto i = shards.begin(), ie = shards.end();
        while (i != ie) {
            auto next = i;
            ++next;
            ws->add(makeSingle(*i));
            i = next;
        }
        return upcast(ws);
    }
    
private:
    static std::unique_ptr<WriteOperation> make26(
        Config::VersionedShard vs, const Namespace& ns,
        const typename Op::Subop* subops, size_t count,
        const bson::Object& writeConcern
    ){
        bson::ArrayBuilder subs;
        for (; count--; subops++)
            subs << Self::pack26(*subops);
        
        bson::ObjectBuilder b;
        b[Self::cmdName()] = ns.collection();
        b[Self::subopsKey()] = subs.array();
        b["ordered"] = false;
        b["writeConcern"] = writeConcern;
        return vs.shard->write(ns, vs.version, b.obj());
    }

    static std::unique_ptr<WriteOperation> make24(
        Config::VersionedShard vs, const Namespace& ns,
        const typename Op::Subop& subop
    ){
        MsgBuilder b;
        Self::pack24(b, ns, subop);
        return vs.shard->write(ns, vs.version, b.finish());
    }    

};

template<> struct WriteOpTraits<messages::Insert> {
public:
    static const bson::Object& selector(const bson::Object& doc) { return doc; }
    static bool isParallelizable(const bson::Object&) { return false; }
    
    static void null(const bson::Object&)
    {
        throw errors::BadRequest("insert operation requires sharding key");
    }
    
    static std::unique_ptr<WriteOperation> makeLocal(
        Config::VersionedShard vs, const Namespace& ns,
        const std::vector<bson::Object>& docs, const bson::Object& writeConcern
    ){
        if (vs.shard->supportsWriteCommands() && !writeConcern.empty()) {
            
            bson::ArrayBuilder docsArray;
            for (const bson::Object& doc: docs)
                docsArray << doc;
            
            bson::ObjectBuilder b;
            b["insert"] = ns.collection();
            b["documents"] = docsArray.array();
            b["ordered"] = false;
            b["writeConcern"] = writeConcern;
            return vs.shard->write(ns, vs.version, b.obj());
            
        } else {
            
            MsgBuilder b;
            b << (uint32_t) 0 << (uint32_t) 0 << Opcode::INSERT
              << (uint32_t) 0 << ns.ns();
            for (const bson::Object& doc: docs)
                b << doc;
            return vs.shard->write(ns, vs.version, b.finish());
        }
    }
    
    static std::unique_ptr<WriteOperation> makeGlobal(
        std::vector<Config::VersionedShard>, const Namespace&,
        const bson::Object&, const bson::Object&
    ){
        throw errors::BadRequest("insert operation requires sharding key");
    }
};

template<> struct WriteOpTraits<messages::Update>:
    public WriteOpTraitsBase< messages::Update, WriteOpTraits<messages::Update> > {
public:
    static const bson::Object& selector(const messages::Update::Subop& subop) { return subop.selector; }
    static unsigned limit(const messages::Update::Subop& subop) { return subop.multi ? 0 : 1; }
    
    static bool isParallelizable(const messages::Update::Subop& subop)
    {
        if (subop.upsert)
            throw errors::BadRequest("upsert requires sharding key");
        return subop.multi;
    }

    static void null(const messages::Update::Subop& subop)
    {
        if (subop.upsert)
            throw errors::BadRequest("upsert requires sharding key");
    }
    
    static const char* cmdName() { return "update"; }
    static const char* subopsKey() { return "updates"; }
    
    static bson::Object pack26(const messages::Update::Subop& u)
    {
        return bson::object(
            "q", u.selector,
            "u", u.update,
            "upsert", u.upsert,
            "multi", u.multi
        );
    }

    static void pack24(MsgBuilder& b, const Namespace& ns, const messages::Update::Subop& u)
    {
        b << (uint32_t) 0 << (uint32_t) 0 << Opcode::UPDATE
          << (uint32_t) 0 << ns.ns() << (
              (u.upsert ? messages::Update::Compat::UPSERT : (uint32_t) 0)
              | (u.multi ? messages::Update::Compat::MULTI_UPDATE : (uint32_t) 0))
          << u.selector << u.update;
    }
};


template<>
struct WriteOpTraits<messages::Delete>:
    public WriteOpTraitsBase< messages::Delete, WriteOpTraits<messages::Delete> >
{
    static const bson::Object& selector(const messages::Delete::Subop& subop) { return subop.selector; }
    static unsigned limit(const messages::Delete::Subop& subop) { return subop.limit; }
    static bool isParallelizable(const messages::Delete::Subop& subop) { return !subop.limit; }
    static void null(const messages::Delete::Subop&) {}

    static const char* cmdName() { return "delete"; }
    static const char* subopsKey() { return "deletes"; }

    static bson::Object pack26(const messages::Delete::Subop& d)
    { return bson::object("q", d.selector, "limit", (int) d.limit); }
    
    static void pack24(MsgBuilder& b, const Namespace& ns, const messages::Delete::Subop& d)
    {        
        b << (uint32_t) 0 << (uint32_t) 0 << Opcode::DELETE
          << (uint32_t) 0 << ns.ns()
          << (d.limit == 1 ? messages::Delete::Compat::SINGLE : (uint32_t) 0)
          << d.selector;
    }
};




template<class Msg, class Iter>
std::unique_ptr<WriteOperation> parseSubop(const Config& conf, const Msg& msg, Iter begin, Iter end)
{
    typedef WriteOpTraits<Msg> Traits;
    
    std::map< std::shared_ptr<Shard>, std::pair<ChunkVersion, std::vector<typename Iter::value_type> > > parts;
    std::vector< std::pair< typename Iter::value_type, std::vector<Config::VersionedShard> > > sequential;

    for (; begin != end; ++begin) {
        auto sub = *begin;
        std::vector<Config::VersionedShard> shards = conf.find(msg.ns, Traits::selector(sub));
        
        auto addToShard = [&sub, &parts](const Config::VersionedShard& vs) {
            auto i = parts.find(vs.shard);
            if (i == parts.end()) {
                i = parts.insert(std::make_pair(
                        vs.shard,
                        std::make_pair(vs.version, std::vector<typename Iter::value_type>())
                )).first;
            } else {
                ASSERT(vs.version == i->second.first);
            }
            
            i->second.second.push_back(sub);
        };
        
        if (shards.empty()) {
            Traits::null(sub);
        } else if (shards.size() == 1) {
            addToShard(shards.front());
        } else if (Traits::isParallelizable(sub)) {
            for (auto&& s: shards)
                addToShard(s);
        } else {
            sequential.push_back(std::make_pair(sub, std::move(shards)));
        }
    }
    
    if (parts.empty() && sequential.empty()) {
        return make_unique<NullWrite>(bson::object("ok", 1, "n", 0));
    } else if (parts.size() == 1 && sequential.empty()) {
        auto&& p = parts.begin();
        return Traits::makeLocal(
            Config::VersionedShard { p->first, p->second.first },
            msg.ns, std::move(p->second.second), msg.writeConcern
        );
    } else if (sequential.size() == 1 && parts.empty()) {
        auto&& seq = sequential.front();
        return Traits::makeGlobal(std::move(seq.second), msg.ns, std::move(seq.first), msg.writeConcern);
    } else {
        
        auto ws = make_unique<ParallelWrite>(msg.writeConcern);
        for (auto&& p: parts) {
            ws->add(Traits::makeLocal(
                Config::VersionedShard { p.first, p.second.first },
                msg.ns, p.second.second, msg.writeConcern
            ));
        }
        for (auto&& p: sequential)
            ws->add(Traits::makeGlobal(std::move(p.second), msg.ns, std::move(p.first), msg.writeConcern));
        
        return upcast(ws);
    }
}

template<class Msg>
std::unique_ptr<WriteOperation> parseWriteOp(const Config& conf, const Msg& msg)
{
    if (msg.subops.empty())
        throw errors::BadRequest("no operations given");
        
    if (msg.ordered && msg.subops.size() > 1) {
        auto ws = make_unique<SequentialWrite>(msg.writeConcern);
        auto i = msg.subops.begin(), ie = msg.subops.end();
        while (i != ie) {
            auto next = i;
            ++next;
            ws->add(parseSubop(conf, msg, i, next));
            i = next;
        }
        return upcast(ws);
    } else {
        return parseSubop(conf, msg, msg.subops.begin(), msg.subops.end());
    }
}

struct FindAndModify {
    Namespace ns;
    const bson::Object* obj;
};

std::unique_ptr<WriteOperation> parseWriteOp(const Config& conf, const FindAndModify& cmd)
{
    const bson::Object& obj = *cmd.obj;
    std::vector<Config::VersionedShard> shards = conf.find(cmd.ns, obj["query"].as<bson::Object>());
    if (shards.empty() && !obj["upsert"].as<bool>(false))
        return make_unique<NullWrite>(bson::object("value", bson::Null(), "ok", 1));
    if (shards.size() == 1)
        return make_unique<WriteFindAndModify>(shards.front(), cmd.ns, obj);
    
    if (obj["upsert"].as<bool>(false))
        throw errors::BadRequest("findAndModify() with upsert flag requires sharding key");
    
    auto ret = make_unique<SequentialWrite>(bson::object());
    ret->stopAtFirstThat([](const bson::Object& obj) { return !obj["value"].is<bson::Null>(); });
    ret->mergeAcksWith([](const std::vector<bson::Object>& acks) {
        ASSERT(acks.empty() || std::all_of(acks.begin(), acks.end() - 1,
            [](const bson::Object& ack) { return ack["value"].is<bson::Null>(); }));
        return !acks.empty() ? acks.back() : bson::object("value", bson::Null(), "ok", 1);
    });
    
    for (const auto& shard: shards)
        ret->add(make_unique<WriteFindAndModify>(shard, cmd.ns, obj));

    return upcast(ret);
}


void checkPrivileges(const Namespace& ns, const auth::Privileges& privileges)
{
    if (ns.collection() == "system.users")
        privileges.require(ns.db(), auth::Privilege::USER_ADMIN);
    if (ns.db() == "config")
        privileges.require(ns.db(), auth::Privilege::CLUSTER_ADMIN);
    if (ns.collection().substr(0, 7) == "system.")
        privileges.require(ns.db(), auth::Privilege::DB_ADMIN);
    
    privileges.require(ns.db(), auth::Privilege::WRITE);
}

template<class Msg>
std::unique_ptr<WriteOperation> performWriteOp(const Msg& msg, const auth::Privileges& privileges)
{
    // TODO: partial operation restarts!
    
    if (options().readOnly)
        throw errors::BadRequest("writes through this server is forbidden");
    
    checkPrivileges(msg.ns, privileges);
    
    std::exception_ptr ex;
    for (int attempt = 0; attempt != 3; ++attempt) {
        try {
            DEBUG(2) << "Making up the write operation";
            std::shared_ptr<Config> config = g_config->get();
            
            const Config::Database* db = config->database(msg.ns.db());
            if (!config->collection(msg.ns) && (!db || db->isPartitioned())) {
                if (attempt == 0)
                    throw errors::ShardConfigStale("collection " + msg.ns.ns() + " does not exist");
                else
                    throw errors::NotImplemented("collection " + msg.ns.ns() + " does not exist");
            }
            
            std::unique_ptr<WriteOperation> ret = parseWriteOp(*config, msg);
            
            DEBUG(1) << "Performing the write operation";
            io::task<void> t = io::spawn([&ret]{ ret->perform(); });
            io::wait(t, options().writeTimeout);
            if (t.completed()) {
                t.join();
                DEBUG(1) << "Write operation done";
                return ret;
            } else {
                DEBUG(1) << "Write operation timed out";
                throw errors::BackendInternalError("timeout");
            }
        }
        catch (errors::ShardConfigStale& e) {
            ex = std::current_exception();
            INFO() << e.what() << "; updating shard config";
            g_config->update();
        }
    }
    std::rethrow_exception(ex);
}

} // namespace

namespace operations {

std::unique_ptr<WriteOperation> insert(const messages::Insert& ins, const auth::Privileges& privileges)
{
    return performWriteOp(ins, privileges);
}

std::unique_ptr<WriteOperation> update(const messages::Update& upd, const auth::Privileges& privileges)
{
    return performWriteOp(upd, privileges);
}

std::unique_ptr<WriteOperation> remove(const messages::Delete& del, const auth::Privileges& privileges)   
{
    return performWriteOp(del, privileges);
}

std::unique_ptr<WriteOperation> findAndModify(const std::string& db, const bson::Object& cmd, const auth::Privileges& privileges)
{
    return performWriteOp(FindAndModify { Namespace(db, cmd.front().as<std::string>()), &cmd }, privileges);
}

} // namespace operations
