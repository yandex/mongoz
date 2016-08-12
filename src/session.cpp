/**
 * session.cpp -- a mongodb client connected to us
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

#include "session.h"
#include "error.h"
#include "proto.h"
#include "log.h"
#include "config.h"
#include "version.h"
#include "shard.h"
#include "utility.h"
#include "clock.h"
#include "http.h"
#include <bson/bson11.h>
#include <algorithm>
#include <cctype>
#include <cxxabi.h>

#ifdef CPUPROFILE
#    include <google/profiler.h>
#endif

Session::~Session()
{
    setWriteOp(std::unique_ptr<WriteOperation>());
}

void Session::performHttp()
{
    std::string query;
    if (!(stream_ >> query))
        return;

    std::ostringstream resp;
    std::unordered_map<std::string, std::string> headers;
    http::dispatch(query, headers, resp);

    std::string body = resp.str();
    headers["Content-Length"] = std::to_string(body.size());

    std::string status = "200";
    auto i = headers.find("Status");
    if (i != headers.end()) {
        status = i->second;
        headers.erase(i);
    }
    
    stream_ << "HTTP/1.0 " << status << " \r\n";
    for (const auto& kv: headers)
        stream_ << kv.first << ": " << kv.second << "\r\n";
    stream_ << "\r\n" << body << "\r\n" << std::flush;
}

bool Session::readMsg(Message& msg)
{
    uint32_t len;
    if (!stream_.read(reinterpret_cast<char*>(&len), sizeof(len)))
        return false;
    
    if (len > 16*1024*1024) {
        if (len == 0x20544547u) { // "GET "
            performHttp();
            return false;
        } else {
            WARN() << "message length too big";
        }
        return false;
    } 
    
    std::vector<char> buf(len - sizeof(len), 0);
    if (!stream_.read(buf.data(), buf.size()))
        return false;
    
    msg = Message(std::move(buf));
    return true;
}

template<class Op, class... Args>
std::unique_ptr<WriteOperation> protect(Op op, Args&&... args)
{
    std::unique_ptr<WriteOperation> ret;
    
    for (size_t attempt = 0; attempt != 8; ++attempt) {
        try {
            return op(std::forward<Args>(args)...);
        }
        catch (errors::ShardConfigStale& e) {
            ret.reset(new FailedOperation(e.what()));
            g_config->update();
        }
        catch (std::exception& e) {
            return std::unique_ptr<WriteOperation>(new FailedOperation(e.what()));
        }
    }
    return ret;
}

void Session::setWriteOp(std::unique_ptr<WriteOperation> op)
{
    if (lastWriteOp_.get()) {
        lastWriteOp_->finish();
        lastWriteOp_.reset();
    }
    
    if (!op.get())
        return;
    
    if (op->isAcknowledgable()) {
        lastWriteOp_ = std::move(op);
    } else {
        op->finish();
    }
}

template<class TimePoint>
class TimeRange {
public:
    TimeRange(TimePoint from, TimePoint till): from_(from), till_(till) {}
    friend std::ostream& operator << (std::ostream& out, const TimeRange& r)
    {
        out << std::chrono::duration_cast<std::chrono::milliseconds>(r.till_ - r.from_).count() << " ms";
        if (debugOptions().detailedTimings) {
            out << " (";
            TimeRange<TimePoint>::putTimePoint(out, r.from_);
            out << " ... ";
            TimeRange<TimePoint>::putTimePoint(out, r.till_);
            out << ")";
        }
        return out;
    }
    
private:
    TimePoint from_;
    TimePoint till_;
    
    static void putTimePoint(std::ostream& out, const TimePoint& tp)
    {
        WallClock::time_point wtp = WallClock::now() + (tp - TimePoint::clock::now());
        size_t milli = std::chrono::duration_cast<std::chrono::milliseconds>(wtp.time_since_epoch()).count();
        time_t t = milli/1000;
        char buf[64];
        struct tm tm;
        strftime(buf, sizeof(buf), "%H:%M:%S", localtime_r(&t, &tm));
        char* p = buf + strlen(buf);
        snprintf(p, buf + sizeof(buf) - p, ".%03d", (int) (milli % 1000));
        out << buf;
    }
};

void Session::run()
{
    std::string client = toString(stream_.fd()->getpeername());

    for (;;) {
        Message msg;
        if (!readMsg(msg))
            return;
        
        SteadyClock::time_point started = SteadyClock::now();
        auto timeSpent = [&started]() {
            return TimeRange<SteadyClock::time_point>(started, SteadyClock::now());
        };
        
        if (msg.opcode() == Opcode::UPDATE) {
        
            messages::Update upd(msg);
            setWriteOp(protect(&operations::update, upd, privileges_));
            INFO() << client << " (#" << msg.reqID() << ") " << upd << " => " << lastWriteOp_->lastStatus() << ", " << timeSpent();
            
        } else if (msg.opcode() == Opcode::INSERT) {
            
            messages::Insert ins(msg);
            setWriteOp(protect(&operations::insert, ins, privileges_));
            INFO() << client << " (#" << msg.reqID() << ") " << ins << " => " << lastWriteOp_->lastStatus() << ", " << timeSpent();
            
        } else if (msg.opcode() == Opcode::DELETE) {
            
            messages::Delete del(msg);
            setWriteOp(protect(&operations::remove, del, privileges_));
            INFO() << client << " (#" << msg.reqID() << ") " << del << " => " << lastWriteOp_->lastStatus() << ", " << timeSpent();
                        
        } else if (msg.opcode() == Opcode::QUERY) {

            messages::Query q(msg);
            std::unique_ptr<DataSource> datasource;
            
            static const std::string CMD_SUFFIX = ".$cmd";
            bool isCmd = (q.ns.collection() == "$cmd");
            
            std::string errmsg;
            const std::type_info* errtype = 0;
            auto saveErr = [isCmd, &errmsg, &errtype, &datasource](const std::exception& e, const std::string& msg) {
                errmsg = e.what();
                errtype = &typeid(e);
                datasource = isCmd ? FixedDataSource::cmdError(8, msg) : FixedDataSource::queryError(msg);
            };            
            
            try {
                if (isCmd) {
                    if (q.query.empty())
                        throw errors::BadRequest("query object empty");
                    datasource.reset(new FixedDataSource(command(q)));
                } else {
                    datasource = operations::query(q, privileges_);
                }
                for (int32_t i = q.nToSkip; i; --i)
                    datasource->advance();
            }
            catch (errors::Error& e)  { saveErr(e, e.what()); }
            catch (io::error& e)      { saveErr(e, e.what()); }
            catch (std::exception& e) { saveErr(e, "Internal error; see mongoz syslog"); }
            
            DataSource* ds = feed(msg.reqID(), datasource.get(), q.nToReturn);
            if (ds != datasource.get())
                datasource.reset(ds);
            
            LogMessage logmsg(errmsg.empty() ? LogMessage::INFO : LogMessage::WARN);
            logmsg << client << " (#" << msg.reqID() << ") ";
            if (isCmd)
                logmsg << q.ns.db() << ".runCommand(" << q.query << ")";
            else
                logmsg << q;
            if (errmsg.empty())
                logmsg << " => " << *datasource;
            else
                logmsg << " => " << errmsg << " (" << demangledType(*errtype) << ")";
            logmsg << ", " << timeSpent();
            
            if (!datasource->isClosed())
                cursors_->insert(std::move(datasource));
            
        } else if (msg.opcode() == Opcode::GET_MORE) {
            
            messages::GetMore more(msg);
            DataSource* oldds = cursors_->find(more.cursorID);
            DataSource* newds = feed(msg.reqID(), oldds, more.nToReturn);
            std::unique_ptr<DataSource> ds;
            if (newds && newds != oldds)
                ds.reset(newds);
            
            if (newds) {
                INFO() << client << " (#" << msg.reqID() << ") " << more << " => " << *newds << ", " << timeSpent();
            } else {
                INFO() << client << " (#" << msg.reqID() << ") " << more << " => cursor not found, " << timeSpent();
            }

            if (newds) {
                if (newds->isClosed()) {
                    cursors_->erase(newds);
                } else if (newds != oldds) {
                    cursors_->insert(std::move(ds));
                } else {
                    // cursor has not been consumed yet; do nothing
                }
            }

        } else if (msg.opcode() == Opcode::KILL_CURSORS) {
            
            messages::KillCursors kill(msg);
            INFO() << client << " (#" << msg.reqID() << ") " << kill;
            for (uint64_t cursorID: kill.cursorIDs) {
                DataSource* ds = cursors_->find(cursorID);
                if (ds) {
                    ds->close();
                    cursors_->erase(ds);
                }
            }

        }
    }
}

/// Sends a portition of `datasource' back to client.
/// Returns a datasource (either given or another one) used.
DataSource* Session::feed(uint32_t reqID, DataSource* datasource, int32_t count)
{
    static const size_t INF = static_cast<size_t>(-1);
    static const size_t MAX_SIZE = 16*1024*1024;

    std::unique_ptr<DataSource> err;
    
    struct ReplyHeader {
        uint32_t size;
        uint32_t requestID;
        uint32_t responseTo;
        Opcode   opcode;
        uint32_t flags;
        uint64_t cursorID;
        uint32_t startingFrom;
        uint32_t numberReturned;
    } __attribute__((packed));
    
    std::vector<char> reply(sizeof(ReplyHeader), 0);
    auto hdr = [&reply]() -> ReplyHeader& { return *reinterpret_cast<ReplyHeader*>(reply.data()); };
    
    hdr().responseTo = reqID;
    hdr().opcode = Opcode::REPLY;
    hdr().startingFrom = datasource ? datasource->pos() : 0;
    hdr().flags = (datasource ? datasource->flags() : messages::Reply::CURSOR_NOT_FOUND);
    
    bool autoClose = (count == 1 || count < 0);
    size_t cnt = abs(count);
    if (cnt == 0)
        cnt = INF;
    cnt = std::min(cnt, debugOptions().batchSize);
    
    size_t retcnt = 0;
    while (datasource && !datasource->atEnd() && reply.size() + datasource->get().rawSize() < MAX_SIZE && cnt != 0) {
        bson::Object obj;
        try {
            obj = datasource->get();
            datasource->advance();
        }
        catch (std::exception& e) {
            DEBUG(1) << e.what() << " (" << demangledType(typeid(e)) << ") "
                     << "while fetching data on cursor " << datasource->id();
            
            // If we haven't sent anything so far, we can return a { $err: ... } document
            // along with QUERY_FAILURE flag right now. Otherwise, we have to stop right here
            // and make a FixedDataSource and stash it so future GET_MORE with this cursor
            // will return this error.
            err = FixedDataSource::queryError(e.what());
            err->setId(datasource->id());
            datasource = err.get();
            if (retcnt != 0) {
                break;
            } else {
                hdr().flags |= datasource->flags();
                continue;
            }
        }

        reply.insert(reply.end(), obj.rawData(), obj.rawData() + obj.rawSize());
        ++retcnt;
        if (cnt != INF)
            --cnt;
    }
    
    hdr().numberReturned = retcnt;
    DEBUG(1) << "returning " << retcnt << " items in the batch";
    
    if (!autoClose && datasource && !datasource->atEnd()) {
        hdr().cursorID = datasource->id();
    } else {
        if (datasource)
            datasource->close();
    }

    hdr().size = reply.size();
    stream_.write(reply.data(), reply.size());
    stream_ << std::flush;
    
    err.release();
    return datasource;
}

namespace {

template<class... Args>
bson::Object success(Args&&... args) { return bson::object("ok", 1, std::forward<Args>(args)...); }

bson::Object failure(int errcode, const std::string& msg) { return bson::object("ok", 0, "errmsg", msg, "code", errcode); }

} // namespace

bson::Object Session::command(const messages::Query& q)
{
    std::string dbname = q.ns.db();
    std::string cmd = q.query.begin()->name();
    std::transform(cmd.begin(), cmd.end(), cmd.begin(), &tolower);
    bson::Object obj = q.query;
    
    if (cmd == "ping") {
        g_config->get(); // ensure shard config exists
        return success();
        
    } else if (cmd == "getlasterror") {
        return lastWriteOp_.get()
            ? lastWriteOp_->acknowledge(obj)
            : bson::object("err", bson::Null(), "ok", 1, "n", 0);
        
    } else if (cmd == "getlog") {
        return success("log", bson::array());
        
    } else if (cmd == "replsetgetstatus") {
        return bson::object("ok", 0, "errmsg", "replSetGetStatus is not supported through mongoz", "info", "mongoz");
        
    } else if (cmd == "ismaster") {
        return success(
            "ismaster", true,
            "maxBsonObjectSize", 16*1024*1024,
            "maxMessageSizeBytes", 16*1024*1024,
            "localTime", bson::Time(std::chrono::duration_cast<std::chrono::seconds>(WallClock::now().time_since_epoch()).count()),
            "maxWireVersion", 2,
            "minWireVersion", 0
        );
        
    } else if (cmd == "getnonce") {
        
        nonce_ = auth::mknonce();
        return success("nonce", nonce_);
        
    } else if (cmd == "authenticate") {
        
        if (nonce_.empty() || nonce_ != obj["nonce"].as<std::string>())
            return failure(18, "nonce mismatch");
        nonce_.clear();
        
        if (!privileges_.auth(dbname, obj))
            return failure(18, "invalid username/password");
        
        return success();
        
    } else if (cmd == "listdatabases") {
        
        auto config = g_config->get();
        bson::ArrayBuilder b;
        for (const Config::Database& db: config->databases()) {
            b << bson::object("name", db.name(), "sizeOnDisk", 1, "empty", false);
        }
        return success("databases", b.array());
        
    } else if (cmd == "insert") {
        
        messages::Insert ins(dbname, obj);
        std::unique_ptr<WriteOperation> op = protect(&operations::insert, ins, privileges_);
        op->finish();
        return op->lastStatus();

    } else if (cmd == "update") {
        
        messages::Update upd(dbname, obj);
        std::unique_ptr<WriteOperation> op = protect(&operations::update, upd, privileges_);
        op->finish();
        return op->lastStatus();

    } else if (cmd == "delete") {
        
        messages::Delete del(dbname, obj);
        std::unique_ptr<WriteOperation> op = protect(&operations::remove, del, privileges_);
        op->finish();
        return op->lastStatus();
        
    } else if (cmd == "count") {
        return operations::count(q, privileges_);
        
    } else if (cmd == "distinct") {
        return operations::distinct(q, privileges_);
        
    } else if (cmd == "findandmodify") {
        std::unique_ptr<WriteOperation> op = protect(&operations::findAndModify, dbname, obj, privileges_);
        op->finish();
        return op->lastStatus();
        
    } else if (cmd == "setloglevel") {
        privileges_.require("admin", auth::Privilege::DB_ADMIN);
        if (Logger::instance())
            Logger::instance()->setMaxLevel(obj.front().as<int>());
        return success();

    } else if (debugOptions().enable && cmd == "getconnectionid") {
        return success("conn", reinterpret_cast<int64_t>(this));
        
    } else if (debugOptions().enable && cmd == "getusedbackends") {
        Session* s = this;
        if (obj["connection"].exists())
            s = reinterpret_cast<Session*>(obj["connection"].as<int64_t>());
        DataSource* ds = s->cursors_->find(obj["cursor"].as<int64_t>());
        if (!ds)
            return failure(20, "cursor not found");
        
        bson::ArrayBuilder b;
        for (const Connection* c: ds->usedConnections())
            b << bson::object(
                "shard", c->backend().shard()->id(),
                "backend", c->backend().addr(),
                "endpoint", toString(c->endpoint().addr())
            );
        return success("backends", b.array());
        
    } else if (debugOptions().enable && cmd == "inspectshard") {
        auto&& shard = ShardPool::instance().find(obj["shard"].as<std::string>());
        return shard ? shard->debugInspect() : failure(20, "shard not found");

#ifdef CPUPROFILE
    } else if (!debugOptions().profileCpu.empty() && cmd == "dumpprofile") {
        ProfilerFlush();
        return success();

#endif

    }

    return bson::object("ok", 0, "err", "unknown command", "bad cmd", obj);
}
