/**
 * proto.h -- a MongoDB wire protocol
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

#include "error.h"
#include "log.h"
#include <vector>
#include <stdexcept>
#include <stdint.h>
#include <syncio/syncio.h>
#include <bson/bson11.h>


enum class Opcode: int32_t {
    REPLY         = 1,
    UPDATE        = 2001,
    INSERT        = 2002,
    QUERY         = 2004,
    GET_MORE      = 2005,
    DELETE        = 2006,
    KILL_CURSORS  = 2007
};

class Namespace {
public:
    Namespace() {}
    
    Namespace(std::string db, std::string coll):
        db_(std::move(db)), coll_(std::move(coll)), ns_(db_ + "." + coll_) {}
    
    explicit Namespace(std::string ns): ns_(std::move(ns))
    {
        auto dot = ns_.find('.');
        REQUIRE(dot != std::string::npos);
        db_ = ns_.substr(0, dot);
        coll_ = ns_.substr(dot + 1);
    }
    
    const std::string& db() const { return db_; }
    const std::string& collection() const { return coll_; }
    const std::string& ns() const { return ns_; }
    
    bool empty() const { return ns_.empty(); }
    
    friend std::ostream& operator << (std::ostream& out, const Namespace& ns) { return out << ns.ns(); }

private:
    std::string db_, coll_, ns_;
};


class Message {
public:
    Message() {}
    Message(std::vector<char> data): data_(std::move(data)), pos_(sizeof(Header)) {}
    
    bool empty() const { return data_.empty(); }
    
    const char* data() const { return data_.data(); }
    size_t size() const { return data_.size(); }
    
    int32_t reqID() const { return header().reqID; }
    int32_t responseTo() const { return header().responseTo; }
    Opcode opcode() const { return header().opcode; }

    Message& fetch(int32_t& x)  { return fetchPOD(x); }
    Message& fetch(uint32_t& x) { return fetchPOD(x); }
    Message& fetch(uint64_t& x) { return fetchPOD(x); }
    
    Message& fetch(std::string& dest)
    {
        dest.clear();
        const char* ptr = pos();
        while (ptr != end() && *ptr)
            dest.push_back(*ptr++);

        advance(ptr - pos() + 1);
        
        return *this;
    }
    
    Message& fetch(Namespace& dest)
    {
        std::string s;
        fetch(s);
        dest = Namespace(s);
        return *this;
    }
    
    Message& fetch(bson::Object& obj)
    {
        need(sizeof(uint32_t));
        uint32_t len = *reinterpret_cast<const uint32_t*>(pos());
        need(len);
        obj = bson::Object::construct(pos(), len);
        advance(len);
        return *this;
    }
    
    bool atEnd() const { return pos() == end(); }
    bool good() const { return pos_ != static_cast<size_t>(-1); }
    explicit operator bool() const { return good(); }

    template<class T>
    Message& operator >> (T& t)
    {
        if (atEnd()) {
            pos_ = static_cast<size_t>(-1);
            return *this;
        } else {
            return fetch(t);
        }
    }    
        
private:
    std::vector<char> data_;
    size_t pos_;
    
    struct Header {
        int32_t reqID;
        int32_t responseTo;
        Opcode opcode;
    } __attribute__((packed));
    
    void need(size_t len) const
    {
        if (pos_ + len > data_.size())
            throw std::runtime_error("message truncated");
    }

    const char* pos() const { return data_.data() + pos_; }
    const char* end() const { return data_.data() + data_.size(); }
    void advance(size_t x) { need(x); pos_ += x; }
    
    const Header& header() const { return *reinterpret_cast<const Header*>(data_.data()); }
        
    template<class T>
    Message& fetchPOD(T& t)
    {
        need(sizeof(T));
        t = *reinterpret_cast<const T*>(pos());
        advance(sizeof(T));
        return *this;
    }

};

namespace messages {

template<class SubopType>
struct Base {
    typedef SubopType Subop;
    Namespace ns;
    std::vector<Subop> subops;
    bool ordered;
    bson::Object writeConcern;
    
protected:
    Base(): ordered(false) {}
    
    template<class F>
    Base(const std::string& db, const bson::Object& cmd, const char* title, const char* subopsKey, F parseSubop)
    {
        static const bson::Object DEFAULT_WRITE_CONCERN = bson::object("w", 1);

        ns = Namespace(db, cmd[title].as<std::string>());
        for (bson::Element elt: cmd[subopsKey].as<bson::Array>()) {
            subops.emplace_back();
            parseSubop(elt.as<bson::Object>(), subops.back());
        }
        ordered = cmd["ordered"].as<bool>(true);
        writeConcern = patchWriteConcern(cmd["writeConcern"].as<bson::Object>(DEFAULT_WRITE_CONCERN));
    }
    
    template<class F>
    class PrintTo {
    public:
        PrintTo(const Base<SubopType>& op, F printSubop):
            op_(&op), printSubop_(printSubop) {}
        
        friend std::ostream& operator << (std::ostream& out, const PrintTo<F>& p)
        {
            bool first = true;
            for (const Subop& sub: p.op_->subops) {
                if (first)
                    first = false;
                else
                    out << ", ";
                p.printSubop_(sub);
            }
            if (p.op_->subops.size() > 1)
                out << (p.op_->ordered ? " [ordered]" : " [parallel]");
            if (!p.op_->writeConcern.empty())
                out << " [writeConcern = " << p.op_->writeConcern << "]";
            return out;
        }
        
    private:
        const Base<SubopType>* op_;
        F printSubop_;
    };
    
    template<class F>
    PrintTo<F> printTo(F printSubop) const { return PrintTo<F>(*this, printSubop); }
    
private:
    bson::Object patchWriteConcern(const bson::Object& concern)
    {
        if (concern["w"].exists())
            return concern;
        
        bson::ObjectBuilder b;
        b["w"] = 1;
        for (const bson::Element& elt: concern)
            b[elt.name()] = elt;
        return b.obj();
    }
};

struct UpdateSub {
    bson::Object selector;
    bson::Object update;
    bool upsert;
    bool multi;
};

struct Update: public Base<UpdateSub> {
    struct Compat {
        static const uint32_t UPSERT        = 1;
        static const uint32_t MULTI_UPDATE  = 2;
    };
        
    explicit Update(Message& msg)
    {        
        int32_t zero;
        int32_t flags;
        subops.resize(1);
        auto& u = subops.front();
        if (!(msg >> zero >> ns >> flags >> u.selector >> u.update))
            throw std::runtime_error("bad update message");
        u.upsert = (flags & Compat::UPSERT);
        u.multi =  (flags & Compat::MULTI_UPDATE);
    }
    
    Update(const std::string& db, const bson::Object& cmd): Base(
        db, cmd, "update", "updates",
        [](const bson::Object& obj, Subop& u) {
            u.selector = obj["q"].as<bson::Object>();
            u.update   = obj["u"].as<bson::Object>();
            u.upsert   = obj["upsert"].as<bool>(false);
            u.multi    = obj["multi"].as<bool>(false);
        }
    ) {}
    
    friend std::ostream& operator << (std::ostream& out, const Update& upd)
    {
        return out << "UPDATE " << upd.ns << " " << upd.printTo([&out](const UpdateSub& u) {
            out << "SET " << u.update << " WHERE " << u.selector
                << (u.upsert ? " [upsert]" : "")
                << (u.multi  ? " [multi]" : "");
        });
    }
};


struct Insert: public Base<bson::Object> {
    struct Compat {
        static const uint32_t CONTINUE_ON_ERROR = 1;
    };
        
    explicit Insert(Message& msg)
    {
        uint32_t flags;
        if (!(msg >> flags >> ns))
            throw std::runtime_error("bad insert message");
        
        ordered = (flags & Compat::CONTINUE_ON_ERROR);
        
        bson::Object obj;
        while (msg >> obj)
            subops.push_back(obj);
    }
    
    Insert(const std::string& db, const bson::Object& cmd): Base(
        db, cmd, "insert", "documents",
        [](const bson::Object& obj, bson::Object& dest) { dest = obj; }
    ) {}
    
    friend std::ostream& operator << (std::ostream& out, const Insert& ins)
    {
        return out << "INSERT INTO " << ins.ns << " VALUES "
                   << ins.printTo([&out](const bson::Object& obj) { out << obj; });
    }
};

struct DeleteSub {
    bson::Object selector;
    unsigned limit;
};

struct Delete: public Base<DeleteSub> {
    struct Compat {
        static const uint32_t SINGLE = 1;
    };
    
    explicit Delete(Message& msg)
    {
        uint32_t zero;
        uint32_t flags;
        
        subops.push_back( DeleteSub {{}, 0} );
        Subop& d = subops.back();
        if (!(msg >> zero >> ns >> flags >> d.selector))
            throw std::runtime_error("bad delete message");
        
        d.limit = (flags & Compat::SINGLE ? 1 : 0);
    }
    
    Delete(const std::string& db, const bson::Object& cmd): Base(
        db, cmd, "delete", "deletes",
        [](const bson::Object& obj, DeleteSub& sub) {
            sub.selector = obj["q"].as<bson::Object>();
            sub.limit = obj["limit"].as<uint32_t>(0);
        }
    ) {}
    
    friend std::ostream& operator << (std::ostream& out, const Delete& del)
    {
        return out << "DELETE FROM " << del.ns << " " << del.printTo([&out](const DeleteSub& d) {
            out << " WHERE " << d.selector;
            if (d.limit)
                out << " LIMIT " << d.limit;
        });
    }
};

struct Query {
    uint32_t flags;
    Namespace ns;
    uint32_t nToSkip;
    int32_t nToReturn;
    bson::Object query;
    bson::Object fieldSelector;
    
    bson::Object criteria;
    bson::Object properties;
    
    static const uint32_t TAILABLE    = 0x02;
    static const uint32_t SLAVE_OK    = 0x04;
    static const uint32_t NO_TIMEOUT  = 0x10;
    static const uint32_t AWAIT_DATA  = 0x20;
    static const uint32_t EXHAUST     = 0x40;
    static const uint32_t PARTIAL     = 0x80;
    
    explicit Query(Message& msg)
    {
        if (!(msg >> flags >> ns >> nToSkip >> nToReturn >> query))
            throw std::runtime_error("bad query message");
        msg >> fieldSelector;
        assign(query);
    }
    
    Query(std::string coll, bson::Object q):
        flags(0), ns(coll), nToSkip(0), nToReturn(0), query(std::move(q))
    {
        assign(q);
    }
    
    bson::Object readPreference() const { return properties["$readPreference"].as<bson::Object>(bson::Object()); }
    
    
    friend std::ostream& operator << (std::ostream& out, const Query& q)
    {
        out << "SELECT " << q.fieldSelector
            << " FROM " << q.ns
            << " WHERE " << q.criteria
            << " LIMIT(" << q.nToSkip << ", " << q.nToReturn << ")";
        
        for (const bson::Element& prop: q.properties) {
            if (strcmp(prop.name(), "query") && strcmp(prop.name(), "$query"))
                out << " [" << prop.name() << "=" << prop << "]";
        }

        return out << (q.flags & TAILABLE   ? " [tailable]"   : "")
                   << (q.flags & SLAVE_OK   ? " [slave OK]"   : "")
                   << (q.flags & NO_TIMEOUT ? " [no timeout]" : "")
                   << (q.flags & AWAIT_DATA ? " [await data]" : "")
                   << (q.flags & EXHAUST    ? " [exhaust]"    : "")
                   << (q.flags & PARTIAL    ? " [partial]"    : "");
    }
    
private:
    void assign(bson::Object q)
    {
        if (!q.empty() && (
               !strcmp(q.front().name(), "query")
            || !strcmp(q.front().name(), "$query")
        )) {
            criteria = q.front().as<bson::Object>();
            properties = std::move(q);
        } else {
            criteria = std::move(q);
        }
    }
};

struct GetMore {
    Namespace ns;
    uint32_t nToReturn;
    uint64_t cursorID;
    
    explicit GetMore(Message& msg)
    {
        uint32_t zero;
        if (!(msg >> zero >> ns >> nToReturn >> cursorID))
            throw std::runtime_error("bad get_more message");
    }
    
    friend std::ostream& operator << (std::ostream& out, const GetMore& more)
    {
        return out << "GET MORE " << more.nToReturn << " items from "
                   << more.ns << " on cursor " << more.cursorID;
    }
};

struct KillCursors {
    std::vector<uint64_t> cursorIDs;
    
    explicit KillCursors(Message& msg)
    {
        uint32_t zero, count;
        if (!(msg >> zero >> count))
            throw std::runtime_error("bad kill_cursors message");
        while (count--) {
            uint32_t cursorID;
            if (!(msg >> cursorID))
                throw std::runtime_error("bad kill_cursors message");
            cursorIDs.push_back(cursorID);
        }
    }
    
    friend std::ostream& operator << (std::ostream& out, const KillCursors& kill)
    {
        out << "KILL CURSORS ";
        for (uint64_t cursor: kill.cursorIDs)
            out << (cursor == kill.cursorIDs.front() ? "" : ", ") << cursor;
        return out;
    }
};

struct Reply {
    uint32_t flags;
    uint64_t cursorID;
    uint32_t startingFrom;
    std::vector<bson::Object> documents;
    
    static const uint32_t CURSOR_NOT_FOUND   = 0x01;
    static const uint32_t QUERY_FAILURE      = 0x02;
    static const uint32_t SHARD_CONFIG_STALE = 0x04;
    static const uint32_t AWAIT_CAPABLE      = 0x08;
    
    explicit Reply(Message& msg)
    {
        uint32_t count;
        if (!(msg >> flags >> cursorID >> startingFrom >> count))
            throw std::runtime_error("bad reply message");
        while (count--) {
            bson::Object obj;
            if (!(msg >> obj))
                throw std::runtime_error("bad reply message");
            documents.push_back(obj);
        }
    }
    
    friend std::ostream& operator << (std::ostream& out, const Reply& reply)
    {
        out << "REPLY: cursor = " << reply.cursorID << ", limit = (" << reply.startingFrom
            << ", " << reply.documents.size() << ")"
            << (reply.flags & CURSOR_NOT_FOUND   ? " [cursor not found]"   : "")
            << (reply.flags & QUERY_FAILURE      ? " [query failure]"      : "")
            << (reply.flags & SHARD_CONFIG_STALE ? " [shard config stale]" : "")
            << (reply.flags & AWAIT_CAPABLE      ? " [await capable]"      : "");
        for (const bson::Object& obj: reply.documents)
            out << "\n    " << obj;
        return out;
    }
};

} // namespace messages


class MsgBuilder {
public:
    MsgBuilder(): buf_(sizeof(uint32_t), 0) {}
    
    void push(uint8_t x) { pushPod(x); }
    void push(int32_t x) { pushPod(x); }
    void push(uint32_t x) { pushPod(x); }
    void push(uint64_t x) { pushPod(x); }
    void push(Opcode x) { pushPod(x); }
    void push(const std::string& x) { buf_.insert(buf_.end(), x.c_str(), x.c_str() + x.size() + 1); }
    void push(const bson::Object& x) { buf_.insert(buf_.end(), x.rawData(), x.rawData() + x.rawSize()); }
    
    template<class T>
    MsgBuilder& operator << (const T& x) { push(x); return *this; }
    
    const std::vector<char>& finish()
    {
        *reinterpret_cast<uint32_t*>(buf_.data()) = buf_.size();
        return buf_;
    }
    
    const char* data() { return finish().data(); }
    size_t size() const { return buf_.size(); }
    
private:
    std::vector<char> buf_;
    
    template<class T>
    void pushPod(const T& t)
    {
        const char* p = reinterpret_cast<const char*>(&t);
        buf_.insert(buf_.end(), p, p + sizeof(T));
    }
};



class QueryComposer {
public:
    QueryComposer(Namespace ns, bson::Object query): ns_(std::move(ns)), query_(std::move(query)) {}
    
    QueryComposer& msgID(uint32_t x) { msgid_ = x; return *this; }
    QueryComposer& flags(uint32_t x) { flags_ = x; return *this; }
    QueryComposer& skip(int32_t x) { skip_ = x; return *this; }
    QueryComposer& batchSize(int32_t x) { batchSize_ = x; return *this; }
    QueryComposer& fieldSelector(bson::Object x) { fieldSelector_ = std::move(x); return *this; }
    
    QueryComposer& slaveOK() { flags_ |= 0x04; return *this; }
    QueryComposer& exhaust() { flags_ |= 0x40; return *this; }
    
    std::vector<char> data() const { return msg().finish(); }
    
    friend std::ostream& operator << (std::ostream& s, const QueryComposer& q)
    {
        MsgBuilder b = q.msg();        
        s.write(b.data(), b.size());
        return s;
    }
    
private:
    uint32_t msgid_ = 0;
    uint32_t flags_ = 0;
    Namespace ns_;
    int32_t skip_ = 0;
    int32_t batchSize_ = 0;
    bson::Object query_;
    bson::Object fieldSelector_;
    
    MsgBuilder msg() const
    {
        uint32_t batchSize = (batchSize_ == 0 && ns_.collection() == "$cmd") ? 1 : batchSize_;
        
        MsgBuilder b;
        b << msgid_ << (uint32_t) 0 << Opcode::QUERY
          << flags_ << ns_.ns() << skip_ << batchSize
          << query_;
        if (!fieldSelector_.empty()) {
            b << fieldSelector_;
        }
        return b;
    }
};


template<class OnDoc>
uint64_t readReply(io::stream& s, uint32_t msgid, OnDoc onDoc)
{
    struct ReplyHdr {
        uint32_t msgid;
        uint32_t responseTo;
        Opcode   opcode;
        uint32_t flags;
        uint64_t cursorID;
        uint32_t startingFrom;
        uint32_t numberReturned;
    } __attribute__((packed));
        
    ReplyHdr hdr;
    uint32_t msglen;
    
    auto bail = [&s](const std::string& msg) {
        DEBUG(1) << "error communicating with backend " << s.fd()->getpeername() << msg;
        throw errors::BackendInternalError(msg);
    };
    
    if (!s.read(reinterpret_cast<char*>(&msglen), sizeof(msglen)))
        bail("");
    if (msglen < sizeof(msglen) + sizeof(hdr))
        bail(": response too short");
    if (msglen > 16*1024*1024)
        bail(": response too long");
    
    if (!s.read(reinterpret_cast<char*>(&hdr), sizeof(hdr)))
        bail("");
    if (hdr.responseTo != msgid)
        bail(": msg_id mismatch");

    if (hdr.flags & 0x01)
        throw errors::CursorNotFound("CURSOR_NOT_FOUND received from backend");
    if (hdr.flags & 0x04)
        throw errors::ShardConfigStale("SHARD_CONFIG_STALE received from backend");
    
    for (size_t i = hdr.numberReturned; i; --i) {
        uint32_t len;
        s.read(reinterpret_cast<char*>(&len), sizeof(len));

        bson::impl::Storage buf;
        buf.push(&len, sizeof(len));
        buf.resize(len);
        s.read(buf.data() + sizeof(uint32_t), buf.size() - sizeof(uint32_t));
        
        bson::Object obj(buf);
        if (hdr.flags & 0x02) {
            if (obj["code"].as<int>(0) == 13435)
                throw errors::NotMaster();
            else
                throw errors::QueryFailure(obj["$err"].as<std::string>());
        }
        
        onDoc(std::move(obj));
    }
    
    return hdr.cursorID;
}

inline bson::Object readReply(io::stream& s, uint32_t msgid)
{
    bson::Object ret;
    readReply(s, msgid, [&ret](bson::Object obj) { ret = std::move(obj); });
    return ret;
}
