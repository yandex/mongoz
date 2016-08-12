/**
 * cache.cpp -- a local cache for mongoz state.
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

#include "cache.h"
#include "log.h"
#include <fstream>
#include <utility>
#include <cstring>
#include <cerrno>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>

namespace {
    static const int CACHE_VERSION = 1;
}

Cache::Cache(std::string filename): filename_(filename)
{
    if (filename.empty())
        return;
    
    std::ifstream f(filename.c_str());
    std::vector<char> buf(sizeof(uint32_t), 0);
    if (!f.read(buf.data(), buf.size()))
        return;
    buf.resize(*reinterpret_cast<uint32_t*>(buf.data()));
    if (!f.read(buf.data() + sizeof(uint32_t), buf.size() - sizeof(uint32_t)))
        return;
    
    bson::Object cache = bson::Object::construct(buf.data(), buf.size());
    bson::Element version = cache["version"];
    if (!version.exists() || !version.is<int>() || version.as<int>() != CACHE_VERSION)
        return;
    
    std::map<std::string, bson::Object> map;
    for (bson::Element elt: cache) {
        if (std::string(elt.name()) == "version")
            continue;
        if (!elt.is<bson::Object>())
            return;
        
        map.insert(std::make_pair(elt.name(), elt.as<bson::Object>()));
    }
    
    data_ = std::move(map);
}


bson::Object Cache::get(const std::string& key)
{
    std::unique_lock<io::sys::mutex> lock(mutex_);
    auto i = data_.find(key);
    return (i != data_.end()) ? i->second : bson::Object();
}


void Cache::put(const std::string& key, bson::Object value)
{
    if (filename_.empty())
        return;

    std::unique_lock<io::sys::mutex> lock(mutex_);
    data_[key] = value;

    bson::ObjectBuilder b;
    b["version"] = CACHE_VERSION;
    for (auto&& kv: data_)
        b[kv.first] = kv.second;
    bson::Object obj = b.obj();
    
    // There is no std::ostream accepting permissions for file being created, so write by hand
    int fd = open((filename_ + ".tmp").c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
    if (fd != -1) {
        const char *p = obj.rawData(), *pe = p + obj.rawSize();
        while (p != pe) {
            ssize_t sz = ::write(fd, p, pe - p);
            if (sz > 0) {
                p += sz;
            } else {
                ::unlink((filename_ + ".tmp").c_str());
                return;
            }
        }
        ::close(fd);
        ::rename((filename_ + ".tmp").c_str(), filename_.c_str());
    } else {
        WARN() << "cannot update " << filename_ << ": " << strerror(errno);
    }
}

std::unique_ptr<Cache> g_cache;
