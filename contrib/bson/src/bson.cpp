/**
 * bson.cpp -- a BSON manipulation library
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

#include <bson/bson.h>
#include <atomic>
#include <ostream>
#include <fstream>
#include <limits>
#include <ctime>
#include <arpa/inet.h> // for htonl()
#include <sys/types.h>
#include <unistd.h>

namespace bson {
namespace impl {

uint64_t urandom()
{
    std::ifstream f;
    f.rdbuf()->pubsetbuf(0, 0);
    f.open("/dev/urandom");
    
    uint64_t ret;
    if (f.read(reinterpret_cast<char*>(&ret), sizeof(ret)))
        return ret;
    else
        throw std::runtime_error("cannot read a number from /dev/urandom");
}

uint64_t counter()
{
    static std::atomic<uint64_t> x(urandom());
    static std::atomic<pid_t> pid(getpid());
    
    if (pid != getpid()) {
        x = urandom();
        pid = getpid();
    }
    return ++x;
}

struct Storage::Impl {
    std::atomic<size_t> refcount { 0 };
    std::vector<char> data;
};

void Storage::reset(Storage::Impl* p)
{
    if (p)
        ++p->refcount;
    if (impl_ && !--impl_->refcount)
        delete impl_;
    impl_ = p;
}

Storage::Storage(): impl_(0) {}
Storage::Storage(const Storage& s): impl_(0) { reset(s.impl_); }
Storage& Storage::operator = (const Storage& s) { reset(s.impl_); return *this; }
Storage::~Storage() { reset(0); }

const char* Storage::data() const
{
    static const char ZERO = 0;
    return impl_ ? impl_->data.data() : &ZERO;
}

char* Storage::data()
{
    return impl_ ? impl_->data.data() : 0;
}

size_t Storage::size() const { return impl_ ? impl_->data.size() : 0; }

void Storage::push(const void* data, size_t size)
{
    const char* p = reinterpret_cast<const char*>(data);
    if (!impl_)
        reset(new Impl);
    impl_->data.insert(impl_->data.end(), p, p + size);
}

void Storage::resize(size_t size) { impl_->data.resize(size); }

void types::String::print(std::ostream& out, const std::string& str)
{
    out << "\"";
    for (char c: str) {
        if (c == '"') {
            out << "\\\"";
        } else if (c == '\n') {
            out << "\\n";
        } else if (c < 32) {
            out << "\\x" << std::hex << (int) (unsigned char) c << std::dec;
        } else {
            out << c;
        }
    }
    out << "\"";
}

void types::Binary::print(std::ostream& out, const std::vector<char>&)
{
    out << "<binary>";
}

} // namespace impl

namespace {
struct TzSetter {
    TzSetter() { ::tzset(); }
};

static TzSetter tz_setter;

static const char DIGITS[] = "0123456789abcdef";

unsigned char byteOfHexDigit(char c)
{
    if (c >= '0' && c <= '9') {
        return c - '0';
    }
    if (c >= 'a' && c <= 'f') {
        return 10u + (c - 'a');
    }
    if (c >= 'A' && c <= 'F') {
        return 10u + (c - 'A');
    }
    throw bson::Error("Invalid ObjectId: unknown character");
}

inline unsigned char byteOfHex(char msb, char lsb)
{
    return byteOfHexDigit(msb) << 4 | byteOfHexDigit(lsb);
}

void puthex(std::ostream& out, unsigned char ch)
{
    out << DIGITS[ch & 0x0F] << DIGITS[ch >> 4];
}

} // namespace

ObjectID::ObjectID(const char* hex)
{
    if (std::strlen(hex) != DataSize * 2) {
        throw bson::Error("Invalid object id: bad size");
    }
    for (size_t i = 0; i < DataSize; ++i) {
        data_[i] = byteOfHex(hex[2 * i], hex[2 * i + 1]);
    }
}

ObjectID::ObjectID(const std::string& hex)
{
    if (hex.size() != DataSize * 2) {
        throw bson::Error("Invalid object id: bad size");
    }
    for (size_t i = 0; i < DataSize; ++i) {
        data_[i] = byteOfHex(hex[2 * i], hex[2 * i + 1]);
    }
}

ObjectID::ObjectID(uint32_t time, uint64_t counter)
    : time_(htonl(time)), counter_(counter)
{
}

ObjectID ObjectID::generate()
{
    return ObjectID(static_cast<uint32_t>(::time(0)), impl::counter());
}

ObjectID ObjectID::minIdForTimestamp(time_t t)
{
    return ObjectID(t, 0);
}

ObjectID ObjectID::maxIdForTimestamp(time_t t)
{
    return ObjectID(t, std::numeric_limits<uint64_t>::max());
}

std::string ObjectID::toString() const
{
    std::string out(DataSize * 2, 0);
    for (size_t i = 0; i < DataSize; ++i) {
        unsigned char byte = static_cast<unsigned char>(data_[i]);
        out[2 * i] = DIGITS[byte >> 4];
        out[2 * i + 1] = DIGITS[byte & 0x0F];
    }
    return out;
}

time_t ObjectID::getTimestamp() const
{
    return static_cast<time_t>(ntohl(time_));
}

std::ostream& operator << (std::ostream& out, const ObjectID& id)
{
    out << "ObjectID(\"";
    for (size_t i = 0; i != ObjectID::DataSize; ++i)
        puthex(out, id.data_[i]);
    return out << "\")";
}

std::ostream& operator << (std::ostream& out, const Time& t)
{
    int tz = abs(::timezone);
    char buf[64];
    struct tm tm;
    time_t time = t.milliseconds() / 1000;
    strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", localtime_r(&time, &tm));
    char* p = buf + strlen(buf);
    snprintf(p, buf + sizeof(buf) - p, ".%03d %c%02d%02d",
        (int) (t.milliseconds() % 1000),
        (::timezone <= 0) ? '+' : '-',
        tz / 3600, (tz % 3600) / 60
    );
    return out << buf;
}

std::ostream& operator << (std::ostream& out, const Timestamp& ts)
{
    return out << "Timestamp(" << ts.first() << ", " << ts.second() << ")";
}

std::ostream& operator << (std::ostream& out, const Any&) { return out << "<any>"; }

} // namespace bson
