/**
 * cursor_storage.h -- a facility which resolves cursor IDs
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
#include "error.h"
#include <unordered_map>
#include <memory>
#include <mutex>
#include <stdint.h>

class CursorMap;

class CursorStoragePolicy {
public:
    virtual ~CursorStoragePolicy() {}
    virtual CursorMap* obtain() = 0;
    virtual void release(CursorMap*) = 0;

    class LocalCursors;
    class GlobalCursors;

    static std::unique_ptr<CursorStoragePolicy>& current()
    {
        static std::unique_ptr<CursorStoragePolicy> p;
        return p;
    }
};


class CursorMap {
public:
    void insert(std::unique_ptr<DataSource> ds)
    {
        std::unique_lock<io::sys::mutex> lock(mutex_);
        map_[ds->id()] = std::move(ds);
    }

    void erase(uint64_t id)
    {
        std::unique_lock<io::sys::mutex> lock(mutex_);
        map_.erase(id);
    }

    void erase(DataSource* ds) { erase(ds->id()); }

    DataSource* find(uint64_t id)
    {
        std::unique_lock<io::sys::mutex> lock(mutex_);
        auto i = map_.find(id);
        return (i != map_.end() && i->second->id() == id) ? i->second.get() : 0;
    }

private:
    std::unordered_map< uint64_t, std::unique_ptr<DataSource> > map_;
    mutable io::sys::mutex mutex_;

    struct Deleter {
        void operator()(CursorMap* p) { CursorStoragePolicy::current()->release(p); }
    };

public:
    typedef std::unique_ptr<CursorMap, Deleter> Ptr;
};


class CursorStoragePolicy::LocalCursors: public CursorStoragePolicy {
public:
    CursorMap* obtain() override { return new CursorMap; }
    void release(CursorMap* p) override { delete p; }
};

class CursorStoragePolicy::GlobalCursors: public CursorStoragePolicy {
public:
    CursorMap* obtain() override { return &map_; }
    void release(CursorMap*) override {}
private:
    CursorMap map_;
};

