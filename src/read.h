/**
 * read.h -- query implementation
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

#include "operations.h"
#include "proto.h"
#include "backend.h"
#include "version.h"
#include "config.h"
#include "log.h"
#include "error.h"
#include <syncio/error.h>

class Backend;
class Shard;
class Endpoint;
class Config;

class NullDatasource: public DataSource {
public:
    bool atEnd() const override { return true; }
    bson::Object get() const override { ASSERT(!"NullDatasource::get(): should never reach here"); }
    void doAdvance() override { ASSERT(!"NullDatasource::doAdvance(): should never reach here"); }
};

class BackendDatasource: public DataSource {
public:
    BackendDatasource(std::shared_ptr<Shard> shard, ChunkVersion version, messages::Query query);
    ~BackendDatasource();
    
    bool atEnd() const override
    {
        return cursorID_ == 0 && current_ == objects_.end();
    }
    
    bson::Object get() const override
    {
        return *current_;
    }
    
    void doAdvance() override
    {
        if (++current_ == objects_.end() && cursorID_ != 0)
            requestMore();
    }
    
    void doClose() override;
    
    void reportConnections(std::vector<const Connection*>& dest) const override { dest.push_back(&conn_); }
    
private /*methods*/:
    std::shared_ptr<Config> config_;
    std::vector<char> makeQuery(uint32_t reqID);
    
    void requestMore();
    void talk(std::function<std::vector<char>(uint32_t)> msgMaker);
    
    uint32_t makeReqID() { return reqID_++; }
    Namespace ns() const;
    
private /*fields*/:
    std::shared_ptr<Shard> shard_;
    ChunkVersion version_;
    Connection conn_;
    messages::Query msg_;
    uint64_t cursorID_;
    uint32_t reqID_;
    std::vector<bson::Object> objects_;
    std::vector<bson::Object>::const_iterator current_;
};



class MergeDatasource: public DataSource {
public:
    MergeDatasource(messages::Query query, std::vector<Config::VersionedShard> shards);
    ~MergeDatasource();
    
    bool atEnd() const override { return datasources_.empty() || datasources_.front()->atEnd(); }
    bson::Object get() const override { return datasources_.front()->get(); }
    void doAdvance() override;
    void doClose() override;

    void reportConnections(std::vector<const Connection*>& dest) const override
    {
        for (const auto& ds: datasources_)
            ds->reportConnections(dest);
    }

private:
    std::shared_ptr<Config> config_;
    messages::Query msg_;
    bson::Object orderBy_;
    std::vector< std::unique_ptr<BackendDatasource> > datasources_;
    
    template<class F>
    bool protect(F f)
    {
        try {
            f();
            return true;
        }
        catch (io::error& e) {
            if (!(msg_.flags & messages::Query::PARTIAL))
                throw;
        }
        catch (errors::BackendInternalError& e) {
            if (!(msg_.flags & messages::Query::PARTIAL))
                throw;
        }
        return false;
    }
};
