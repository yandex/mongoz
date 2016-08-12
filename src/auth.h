/**
 * auth.h -- authorization and authenticaton related stuff.
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
#include "options.h"
#include <bson/bson.h>
#include <syncio/syncio.h>
#include <string>
#include <map>
#include <mutex>
#include <cassert>

namespace auth {

const std::string& sharedSecret();

void loadSecret(const std::string& keyfile);


std::string makeDigest(const std::string& user, const std::string& passwd);

std::string makeAuthKey(const std::string& nonce, const std::string& user, const std::string& digest);

std::string mknonce();



enum class Privilege {
    READ             = 1, // Execute queries and basic commands
    WRITE            = 2, // Execute inserts, updates and deletes
    DB_ADMIN         = 3, // Execute most of the commands
    USER_ADMIN       = 4, // Access `<db>.system.users' in any way
    CLUSTER_ADMIN    = 5  // Access replica set and sharding config
};

class Privileges {
public:
    Privileges(): globalMask_(0) {}
    
    bool authorized(const std::string& db, Privilege p) const
    {
        unsigned bit = makeMask(p);
        if (globalMask_ & bit)
            return true;
        auto i = masks_.find(db);
        return i != masks_.end() && (i->second & bit);
    }
    
    void require(const std::string& db, Privilege p) const
    {
        if (options().auth && !authorized(db, p))
            throw errors::Unauthorized("unauthorized");
    }
    
    bool auth(const std::string& db, const bson::Object& obj);
    
    static const Privileges& root() { static const Privileges p = makeRoot(); return p; }
    
private:
    unsigned globalMask_;
    std::map<std::string, unsigned> masks_;
    
    static Privileges makeRoot()
    {
        Privileges p;
        p.globalMask_ = static_cast<unsigned>(-1);
        return p;
    }
    
    static unsigned makeMask(Privilege p) { return 1u << static_cast<unsigned>(p); }
    
    void applyRoles(const std::string& db, const bson::Array& roles);
};


class CredentialsCache {
public:
    static CredentialsCache& instance() { static CredentialsCache c; return c; }
    
    bson::Object find(const std::string& dbname, const std::string& user)
    {
        auto cache = map();
        if (!cache) {
            update();
            cache = map();
            assert(!!cache);
        }
        
        auto i = cache->find(std::make_pair(dbname, user));
        return i != cache->end() ? i->second : bson::Object();
    }
    
    void update();
    void keepUpdating();

private:
    typedef std::map<std::pair<std::string, std::string>, bson::Object> Map;
    std::shared_ptr<Map> cache_;
    mutable io::sys::mutex mutex_;
    
    CredentialsCache();
    
    std::shared_ptr<Map> map() const
    {
        std::unique_lock<io::sys::mutex> lock(mutex_);
        return cache_;
    }
    
    bson::Object fetch();
    std::shared_ptr<Map> parse(bson::Object obj);
};


} // namespace auth
