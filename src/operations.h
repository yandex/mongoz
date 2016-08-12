/**
 * operations.h -- declarations of CRUD operations
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
#include "auth.h"
#include <bson/bson.h>
#include <bson/bson11.h>
#include <atomic>

class Connection;

class DataSource {
public:
    DataSource(): id_(generateID()), pos_(0), closed_(false) {}
    
    /// NB: no I/O here please; just ::close() all neccessary
    ///     fds and free memory. Utilize `close' for more gentle cleanup.
    virtual ~DataSource() {}
    
    
    DataSource(const DataSource&) = delete;
    DataSource& operator = (const DataSource&) = delete;
    
    uint64_t id() const { return id_; }
    void setId(uint64_t id) { id_ = id; }
    
    virtual bool atEnd() const = 0;
    virtual bson::Object get() const = 0;
    
    virtual uint32_t flags() const { return 0; }
    
    void advance()
    {
        if (!atEnd()) {
            ++pos_;
            doAdvance();
        }
    }
    
    size_t pos() const { return pos_; }

    /// Releases all resources held by datasource in a gentle way.
    /// (issues OP_KILL_CURSORS to backends, returns connections
    /// to their pools, etc...). Called only on happy path, so may
    /// perform I/O operations and throw exceptions.
    void close() { closed_ = true; doClose(); }
    bool isClosed() const { return closed_; }
    
    /// Used for debugging and testing.
    std::vector<const Connection*> usedConnections() const
    {
        std::vector<const Connection*> ret;
        reportConnections(ret);
        return ret;
    }

private /*methods*/:
    virtual void doAdvance() = 0;
    virtual void doClose() {}
    
    static uint64_t generateID() {
        static std::atomic<uint64_t> g_id { 0 };
        return ++g_id;
    }
    
    virtual void reportConnections(std::vector<const Connection*>&) const {}
    
    virtual void dump(std::ostream& out) const
    {
        out << "at pos " << pos_;
        if (atEnd())
            out << ", EOF";
        else
            out << ", cursor " << id();
    }
    
    friend std::ostream& operator << (std::ostream& out, const DataSource& ds) { ds.dump(out); return out; }
    
private /*fields*/:
    uint64_t id_;
    size_t pos_;
    bool closed_;
};


class FixedDataSource: public DataSource {
public:
    explicit FixedDataSource(const bson::Object& obj, uint32_t flags = 0): obj_(obj), consumed_(false), flags_(flags) {}
    bool atEnd() const override { return consumed_; }
    bson::Object get() const override { return obj_; }
    void doAdvance() override { consumed_ = true; }
    uint32_t flags() const override { return flags_; }
    
    void dump(std::ostream& out) const override { out << obj_; }
    
    static std::unique_ptr<DataSource> queryError(const std::string& msg)
    {
        return std::unique_ptr<DataSource>(new FixedDataSource(bson::object("$err", msg), messages::Reply::QUERY_FAILURE));
    }
    
    static std::unique_ptr<DataSource> cmdError(int code, const std::string& msg)
    {
        return std::unique_ptr<DataSource>(new FixedDataSource(bson::object("ok", 0, "code", code, "errmsg", msg)));
    }

private:
    bson::Object obj_;
    bool consumed_;
    uint32_t flags_;
};



class WriteOperation {
public:
    virtual ~WriteOperation() {}
    virtual void perform() = 0;
    
    virtual bool isAcknowledgable() const = 0;
    
    bson::Object acknowledge(const bson::Object& writeConcern)
    {
        if (writeConcern["wtimeout"].exists() || lastStatus_.empty()
            || !areWriteConcernsEqual(writeConcern, lastWriteConcern_))
        {
            setAcknowledge(writeConcern, doAcknowledge(writeConcern));
        }
        return lastStatus_;
    }
    
    virtual bson::Object lastStatus() const { return lastStatus_; }
    
    virtual void finish() = 0;
    
protected:
    const bson::Object& setAcknowledge(const bson::Object& writeConcern, const bson::Object& status);
    
private /*methods*/:
    virtual bson::Object doAcknowledge(const bson::Object& writeConcern) = 0;
    
    static bool areWriteConcernsEqual(const bson::Object& w1, const bson::Object& w2);
    
private /*fields*/:
    bson::Object lastWriteConcern_;
    bson::Object lastStatus_;
};


class FailedOperation: public WriteOperation {
public:
    explicit FailedOperation(bson::Object err): err_(err) {}

    explicit FailedOperation(const std::string& errmsg):
        err_(bson::object("errmsg", errmsg, "err", errmsg, "ok", 0)) {}
    
    bool isAcknowledgable() const override { return true; }
    void perform() override {}
    bson::Object doAcknowledge(const bson::Object&) override { return err_; }
    bson::Object lastStatus() const override { return err_; }
    void finish() {}
    
private:
    bson::Object err_;
};



namespace operations {

std::unique_ptr<DataSource> query(const messages::Query&, const auth::Privileges&);

inline std::unique_ptr<DataSource> query(std::string coll, bson::Object q) {
    return query(messages::Query(std::move(coll), std::move(q)), auth::Privileges::root());
}

std::unique_ptr<WriteOperation> update(const messages::Update&, const auth::Privileges&);
std::unique_ptr<WriteOperation> insert(const messages::Insert&, const auth::Privileges&);
std::unique_ptr<WriteOperation> remove(const messages::Delete&, const auth::Privileges&);
std::unique_ptr<WriteOperation> findAndModify(const std::string& db, const bson::Object& cmd, const auth::Privileges&);

bson::Object count(const messages::Query&, const auth::Privileges&);
bson::Object distinct(const messages::Query&, const auth::Privileges&);

} // namespace operations
