/**
 * session.h -- a mongodb client connected to us
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
#include "operations.h"
#include "cursor_storage.h"
#include <syncio/syncio.h>
#include <vector>
#include <memory>
#include <map>

class Session {
public:    
    static void handle(io::fd fd)
    {
        io::spawn(&Session::doHandle, std::move(fd)).rename("serve fd " + std::to_string(fd.get())).detach();
    }

private /*methods*/:

    static void doHandle(io::fd fd) { Session(std::move(fd)).run(); }
    
    explicit Session(io::fd fd):
        stream_(std::move(fd)),
        cursors_(CursorStoragePolicy::current()->obtain())
    {}

    ~Session();
    void run();

    bool readMsg(Message& msg);
    void performHttp();
    DataSource* feed(uint32_t reqID, DataSource* datasource, int32_t count);
    
    void reply(uint32_t msgid, const messages::Query& query, std::unique_ptr<DataSource> datasource);
    
    bson::Object command(const messages::Query& q);
    
    void setWriteOp(std::unique_ptr<WriteOperation> op);

private /*fields*/:
    io::stream stream_;
   
    CursorMap::Ptr cursors_;
    std::unique_ptr<WriteOperation> lastWriteOp_;
    
    std::string nonce_;
    auth::Privileges privileges_;
};
