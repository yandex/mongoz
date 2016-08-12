/**
 * auth.cpp -- authorization and authentication related stuff.
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

#include "auth.h"
#include "log.h"
#include "config.h"
#include "operations.h"
#include "cache.h"
#include <bson/bson11.h>
#include <syncio/syncio.h>
#include <openssl/md5.h>
#include <fstream>
#include <string>
#include <stdexcept>

namespace auth {

namespace {
    std::string g_sharedSecret;
}

const std::string& sharedSecret() { return g_sharedSecret; }

std::string hex(const void* data, size_t size)
{
    static const char DIGITS[] = "0123456789abcdef";
    std::string ret;
    const unsigned char* p = static_cast<const unsigned char*>(data);
    for (; size--; p++) {
        ret.push_back(DIGITS[*p >> 4]);
        ret.push_back(DIGITS[*p & 15]);
    }
    return ret;
}

std::string md5hex(const std::string& str)
{
    unsigned char md5[MD5_DIGEST_LENGTH];
    MD5_CTX ctx;
    
    MD5_Init(&ctx);
    MD5_Update(&ctx, str.data(), str.size());
    MD5_Final(md5, &ctx);
    
    return hex(md5, MD5_DIGEST_LENGTH);
}

std::string makeDigest(const std::string& user, const std::string& passwd) {
    return md5hex(user + ":mongo:" + passwd);
}

std::string makeAuthKey(const std::string& nonce, const std::string& user, const std::string& digest) {
    return md5hex(nonce + user + digest);
}

void loadSecret(const std::string& filename)
{
    std::ifstream f(filename.c_str());
    std::string str;
    char c;
    
    while (f >> c) {
        if (isspace(c))
            continue;
        else if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '=' || c == '+' || c == '/')
            str.push_back(c);
        else
            throw std::runtime_error("bad key file: " + filename);
    }
    if (str.empty())
        throw std::runtime_error("bad key file: " + filename);
    
    g_sharedSecret = makeDigest("__system", str);
}

std::string mknonce()
{
    static std::ifstream g_devUrandom("/dev/urandom");
    uint64_t val;
    
    if (!g_devUrandom.read(reinterpret_cast<char*>(&val), sizeof(val))) {
        // fallback
        static const bool randomInitialized __attribute__((unused)) = []{
            srandom(time(NULL));
            return true;
        }();
        val = ((uint64_t) random() << 32) | random();
    }
    
    return hex(&val, sizeof(val));
}


void Privileges::applyRoles(const std::string& db, const bson::Array& roles)
{
    unsigned newmask = 0;
    unsigned newglobal = 0;
    bool isAdmin = (db == "admin");
    
    for (bson::Element elt: roles) {
        
        std::string localdb;
        std::string role;
        if (elt.is<std::string>()) {
            localdb = db;
            role = elt.as<std::string>();
        } else if (elt.is<bson::Object>()) {
            localdb = elt["db"].as<std::string>();
            role = elt["role"].as<std::string>();
        } else {
            WARN() << "cannot parse role definition: " << elt;
            continue;
        }
        
        if (role == "read")
            newmask |= makeMask(Privilege::READ);
        else if (role == "readWrite")
            newmask |= (makeMask(Privilege::READ) | makeMask(Privilege::WRITE));
        else if (role == "dbAdmin")
            newmask |= makeMask(Privilege::DB_ADMIN);
        else if (role == "userAdmin")
            newmask |= makeMask(Privilege::USER_ADMIN);
        else if (role == "dbOwner")
            newmask |= (makeMask(Privilege::DB_ADMIN) | makeMask(Privilege::USER_ADMIN) | makeMask(Privilege::READ) | makeMask(Privilege::WRITE));
        else if (isAdmin && role == "clusterAdmin")
            newglobal |= makeMask(Privilege::CLUSTER_ADMIN);
        else if (isAdmin && role == "readAnyDatabase")
            newglobal |= makeMask(Privilege::READ);
        else if (isAdmin && role == "readWriteAnyDatabase")
            newglobal |= (makeMask(Privilege::READ) | makeMask(Privilege::WRITE));
        else if (isAdmin && role == "userAdminAnyDatabase")
            newglobal |= makeMask(Privilege::USER_ADMIN);
        else if (isAdmin && role == "dbAdminAnyDatabase")
            newglobal |= makeMask(Privilege::DB_ADMIN);
        else
            throw errors::ShardConfigBroken("unknown privilege `" + role + "' for database `" + db + "'");
    }
    
    masks_[db] |= newmask;
    globalMask_ |= newglobal;
}


bool Privileges::auth(const std::string& db, const bson::Object& obj)
{
    if (!options().auth)
        return true;
    
    std::string username = obj["user"].as<std::string>();
    bson::Object user = CredentialsCache::instance().find(db, username);
    
    DEBUG(2) << "User object: " << user;
    
    std::string key;
    if (!user.empty()) {
        key = user["credentials"]["MONGODB-CR"].as<std::string>("");
        if (key.empty())
            key = user["pwd"].as<std::string>("");
    }
    
    if (key.empty() || obj["key"].as<std::string>("") != makeAuthKey(
        obj["nonce"].as<std::string>(""), username, key
    ))
        return false;
        
    if (user["roles"].exists()) {
        applyRoles(db, user["roles"].as<bson::Array>());
    } else {
        unsigned& mask = masks_[db];
        mask |= makeMask(Privilege::READ);
        bson::Element ro = user["readOnly"];
        if (ro.exists() && (
            (ro.is<bool>() && !ro.as<bool>())
            || (ro.is<int>() && !ro.as<int>())
        ))            
            mask |= makeMask(Privilege::WRITE);
    }
    
    if (user["otherDBRoles"].exists()) {
        for (bson::Element elt: user["otherDBRoles"].as<bson::Object>())
            applyRoles(elt.name(), elt.as<bson::Array>());
    }
    
    return true;
    
    // TODO: add support for `userSource'
}


CredentialsCache::CredentialsCache()
{
    bson::Object cache = g_cache->get("auth");
    if (!cache.empty()) {
        cache_ = parse(cache);
    } else {
        io::spawn([this]{ update(); }).detach();
    }
}

void CredentialsCache::update()
{
    bson::Object obj = fetch();
    std::shared_ptr<Map> newcache = parse(obj);
    {
        std::unique_lock<io::sys::mutex> lock(mutex_);
        cache_ = newcache;
    }
    g_cache->put("auth", obj);
}

void CredentialsCache::keepUpdating()
{
    for (;;) {
        io::sleep(options().confInterval);
        try {
            update();
        }
        catch (std::exception& e) {
            ERROR() << e.what();
        }
    }
}

std::shared_ptr<CredentialsCache::Map> CredentialsCache::parse(bson::Object obj)
{
    auto ret = std::make_shared<CredentialsCache::Map>();
    for (bson::Element db: obj)
        for (bson::Element user: db.as<bson::Array>()) {
            std::string dbname = (db.name() == std::string("admin") && user["db"].exists())
                ? user["db"].as<std::string>()
                : db.name();
            
            auto i = ret->insert(std::make_pair(std::make_pair(dbname, user["user"].as<std::string>()), user.as<bson::Object>()));
            if (!i.second && db.name() == "admin")
                i.first->second = user.as<bson::Object>();
        }
    return ret;
}

bson::Object CredentialsCache::fetch()
{
    try {
        DEBUG(1) << "Fetching credentials cache";
        bson::ObjectBuilder ret;

        std::shared_ptr<Config> config = g_config->get();
        for (const Config::Database& db: config->databases()) {

            DEBUG(1) << "Fetching credentials for database " << db.name();
            bson::ArrayBuilder users;

            bson::ObjectBuilder readPref;
            readPref["mode"] = "primaryPreferred";
            if (options().confTimeout != decltype(options().confTimeout)::max())
                readPref["timeoutMs"] = std::chrono::duration_cast<std::chrono::milliseconds>(options().confTimeout).count();
            if (options().confRetransmit != decltype(options().confRetransmit)::max())
                readPref["retransmitMs"] = std::chrono::duration_cast<std::chrono::milliseconds>(options().confRetransmit).count();

            for (auto it = operations::query(db.name() + ".system.users", bson::object(
                    "$query", bson::object(),
                    "$readPreference", readPref.obj()
                )); !it->atEnd(); it->advance()
            ){
                users << it->get();
            }
            
            ret[db.name()] = users.array();
        }

        DEBUG(1) << "Done fetching credentials cache";
        bson::Object obj = ret.obj();
        DEBUG(3) << "Credentials cache: " << obj;
        return obj;
    }
    catch (std::exception& e) {
        throw errors::Error(std::string("cannot fetch auth info: ") + e.what());
    }
}

} // namespace auth

