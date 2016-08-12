/**
 * write.h -- insert, update and remove operations
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
#include "shard.h"
#include "backend.h"
#include "version.h"
#include "config.h"
#include <bson/bson.h>

class WriteToBackend: public WriteOperation {
public:
    WriteToBackend(Config::VersionedShard vs, Namespace ns): vs_(std::move(vs)), ns_(std::move(ns)) {}
    
    void perform() override;
    void finish() override;
    bson::Object doAcknowledge(const bson::Object& writeConcern) override { return doAcknowledge(c_, writeConcern); }
    
protected:
    const Namespace& ns() const { return ns_; }
    const ChunkVersion& version() const { return vs_.version; }
    
private /*methods*/:
    virtual bson::Object doPerform(Connection& c) = 0;
    virtual bson::Object doAcknowledge(Connection& c, const bson::Object& writeConcern) = 0;
    
private /*fields*/:
    Config::VersionedShard vs_;
    Namespace ns_;
    Connection c_;
};

class WriteToBackend24: public WriteToBackend {
public:
    WriteToBackend24(Config::VersionedShard vs, Namespace ns, std::vector<char> msg);
    
    bool isAcknowledgable() const override { return true; }
    bson::Object doPerform(Connection& c) override;
    bson::Object doAcknowledge(Connection& c, const bson::Object& writeConcern) override;
    
private:
    std::vector<char> msg_;
};

class WriteToBackend26: public WriteToBackend {
public:
    WriteToBackend26(Config::VersionedShard vs, Namespace ns, bson::Object cmd);

    bool isAcknowledgable() const override { return false; }
    bson::Object doPerform(Connection& c) override;
    bson::Object doAcknowledge(Connection& c, const bson::Object& writeConcern) override;
    
private:
    bson::Object cmd_;
};

class WriteFindAndModify: public WriteToBackend {
public:
    WriteFindAndModify(Config::VersionedShard vs, Namespace ns, bson::Object cmd);

    bool isAcknowledgable() const override { return false; }
    bson::Object doPerform(Connection& c) override;
    bson::Object doAcknowledge(Connection& c, const bson::Object& writeConcern) override;
private:
    bson::Object cmd_;
};



class MultiWrite: public WriteOperation {
public:
    explicit MultiWrite(bson::Object writeConcern);
    void mergeAcksWith(std::function<bson::Object(const std::vector<bson::Object>&)> merge) { merge_ = merge; }
    void add(std::unique_ptr<WriteOperation> op) { ops_.push_back(std::move(op)); }

    bson::Object doAcknowledge(const bson::Object& writeConcern) override;
    void finish() override;
    bool isAcknowledgable() const override;
    
protected:
    typedef std::vector< std::unique_ptr<WriteOperation> >::iterator iterator;
    iterator begin() { return ops_.begin(); }
    iterator end() { return ops_.end(); }
    
    void commence(WriteOperation* op) { commenced_.push_back(op); }
    const bson::Object writeConcern() const { return writeConcern_; }
    bson::Object mergeAcks(const std::vector<bson::Object>& acks) const { return merge_(acks); }
    
private:
    std::vector< std::unique_ptr<WriteOperation> > ops_;
    std::vector<WriteOperation*> commenced_;
    bson::Object writeConcern_;    
    std::function<bson::Object(const std::vector<bson::Object>&)> merge_;
};


class ParallelWrite: public MultiWrite {
public:
    explicit ParallelWrite(bson::Object writeConcern): MultiWrite(std::move(writeConcern)) {}
    void perform() override;
};


class SequentialWrite: public MultiWrite {
public:
    explicit SequentialWrite(bson::Object writeConcern): MultiWrite(std::move(writeConcern)) {}
    void stopAtFirstThat(std::function<bool(const bson::Object&)> stop) { stop_ = stop; }
    void perform() override;
private:
    std::function<bool(const bson::Object&)> stop_;
};


class NullWrite: public WriteOperation {
public:
    NullWrite(bson::Object ack): ack_(ack) {}
    void perform() override {}
    void finish() override {}
    bool isAcknowledgable() const override { return true; }
    bson::Object doAcknowledge(const bson::Object& /*writeConcern*/) { return ack_; }
    bson::Object lastStatus() const override { return ack_; }
    
private:
    bson::Object ack_;
};
