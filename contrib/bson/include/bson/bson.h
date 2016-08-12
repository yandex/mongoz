/**
 * bson.h -- a BSON manipulation library
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

#include "ordering.h"
#include "types.h"
#include <error.h>
#include <vector>
#include <string>
#include <cstring>
#include <ctime>
#include <memory>
#include <stdexcept>
#include <iostream>
#include <sstream>
#include <functional>
#include <stdint.h>

namespace bson {

#define BSON_DEFINE_ERROR(type,base) \
    class type: public base { \
    public: \
        type(const std::string& what): base(what) {} \
    };


BSON_DEFINE_ERROR(Error, std::runtime_error);
BSON_DEFINE_ERROR(TypeMismatch, Error);
BSON_DEFINE_ERROR(BrokenBson, Error);

#undef BSON_DEFINE_ERROR


namespace impl {

// In fact, std::shared_ptr<std::vector<char>>.
class Storage {
public:
    Storage();
    Storage(const Storage&);
    Storage& operator = (const Storage&);
    ~Storage();
    
    char* data();
    const char* data() const;
    size_t size() const;
    
    void push(const void* data, size_t size);
    void resize(size_t size);

private:
    struct Impl;
    Impl* impl_;
    
    void reset(Impl* p);
};

}

class Element: public impl::Ordered<Element> {
public:
    Element(): data_(0), value_(0) {}
    
    explicit Element(const impl::Storage& storage, const char* data):
        storage_(storage),
        data_(data),
        value_(data ? data + 2 + strlen(data + 1) : 0)
    {}
    
    bool exists() const { return data_ && *data_; }
    int8_t type() const { return *data_; }
    const char* name() const { return exists() ? (data_ + 1) : ""; }
    template<class T> bool is() const;
    template<class T> bool canBe() const { return is<T>(); }
    template<class T> T as() const;
    template<class T> T as(const T& dfltValue) const { return exists() ? as<T>() : dfltValue; }
    
    Element operator[](const char* field) const;
    Element operator[](size_t idx) const;

    const char* rawData() const { return data_; }
    size_t rawSize() const;
    void printTo(std::ostream& out) const;
    friend std::ostream& operator << (std::ostream& out, const Element& elt) { elt.printTo(out); return out; }
    
    const char* valueData() const { return exists() ? data_ + 2 + strlen(data_+1) : 0; }
    size_t valueSize() const { return rawData() + rawSize() - valueData(); }
    
    class CmpProxy: public impl::Ordered<CmpProxy> {
    public:
        CmpProxy(const Element* elt): elt_(elt) {}
    private:
        const Element* elt_;
        
        friend class impl::OrderedBase;
        template<template<class> class Cmp>
        static bool cmp(const CmpProxy& a, const CmpProxy& b)
        {
            return Element::cmpDisregardingNames<Cmp>(*a.elt_, *b.elt_);
        }
    };
    CmpProxy stripName() const { return CmpProxy(this); }

private:
    impl::Storage storage_;
    const char* data_;
    const char* value_;
    
    template<class T> T asNumber() const;
    bool isNumber() const;
    
    int8_t safeType() const { return exists() ? type() : 0; }
    
    template<template<class> class Cmp>
    static bool cmpValues(const Element& a, const Element& b);
    
    friend class impl::OrderedBase;
    friend class CmpProxy;

    template<template<class> class Cmp>
    static bool cmpDisregardingNames(const Element& a, const Element& b)
    {
        if (a.safeType() != b.safeType())
            return Cmp<int8_t>()(a.safeType(), b.safeType());
        if (!a.exists() && !b.exists())
            return Cmp<int>()(0, 0);
        return cmpValues<Cmp>(a, b);
    }    

    template<template<class> class Cmp>
    static bool cmp(const Element& a, const Element& b)
    {
        int ret = strcmp(a.name(), b.name());
        if (ret)
            return Cmp<int>()(ret, 0);
        return cmpDisregardingNames<Cmp>(a, b);
    }
};


namespace impl {

class Base: public Ordered<Base> {
public:    
    Base(): data_(EMPTY()) {}
    explicit Base(const Storage& storage, const char* data): storage_(storage), data_(data) {}
    
    class const_iterator: public std::iterator<std::forward_iterator_tag, Element> {
    public:
        const_iterator(const impl::Storage& storage, const char* data): storage_(&storage), data_(data) {}
        
        Element operator*() const { return Element(*storage_, data_); }
        
        class ArrowProxy: public Element {
        public:
            explicit ArrowProxy(const impl::Storage* storage, const char* data): Element(*storage, data) {}
            Element* operator ->() { return this; }
        };
        
        ArrowProxy operator ->() const { return ArrowProxy(storage_, data_); }
        
        bool operator == (const_iterator i) const { return data_ == i.data_; }
        bool operator != (const_iterator i) const { return data_ != i.data_; }
        
        const_iterator& operator++() { data_ += (**this).rawSize(); return *this; }
        const_iterator operator++(int) { const_iterator tmp(*this); ++*this; return tmp; }
        
    private:
        const Storage* storage_;
        const char* data_;
    };
    
    const_iterator begin() const { return const_iterator(storage_, rawData() + sizeof(int32_t)); }
    const_iterator end()   const { return const_iterator(storage_, rawData() + rawSize() - 1); }
    
    Element front() const { return empty() ? Element() : *begin(); }
    
    size_t size() const { return std::distance(begin(), end()); }
    bool empty() const { return begin() == end(); }
    
    const char* rawData() const { return data_; }
    size_t rawSize() const { return *reinterpret_cast<const int32_t*>(rawData()); }

protected:
    template<class T>
    static T construct(const char* data, size_t size)
    {
        if (size < 4 || *reinterpret_cast<const uint32_t*>(data) > size)
            throw BrokenBson("BSON object spans past buffer end");
        impl::Storage storage;
        storage.push(data, size);
        return T(storage, storage.data());
    }
    
private:
    Storage storage_;
    const char* data_;
    
    static const char* EMPTY() { return "\5\0\0\0\0"; }
    
    friend class OrderedBase;
    template<template<class> class Cmp>
    static bool cmp(const Base& a, const Base& b)
    {
        const_iterator ai = a.begin(), ae = a.end();
        const_iterator bi = b.begin(), be = b.end();
        for (; ai != ae && bi != be; ++ai, ++bi)
            if (*ai != *bi)
                return Cmp<Element>()(*ai, *bi);
        
        return Cmp<Element>()(
            ai != ae ? *ai : Element(),
            bi != be ? *bi : Element()
        );
    }
};


class Builder {
public:
    Builder(): finished_(false) { push<uint32_t>(0); }

    void push(const char* data, size_t size)
    {
        storage_.push(data, size);
    }
    
    template<class T>
    void push(const T& t) { push(reinterpret_cast<const char*>(&t), sizeof(T)); }
    
    impl::Storage finish()
    {
        if (!finished_) {
            push<char>(0);
            *reinterpret_cast<uint32_t*>(storage_.data()) = storage_.size();
            finished_ = true;
        }
        return storage_;
    }
    
private:
    impl::Storage storage_;
    bool finished_;
};

} // namespace impl

class Object: public impl::Base {
public:
    Object() {}
    explicit Object(const impl::Storage& storage): impl::Base(storage, storage.data()) {}
    Object(const impl::Storage& storage, const char* data): impl::Base(storage, data) {}

    static Object construct(const char* data, size_t size) { return impl::Base::construct<Object>(data, size); }
    
    Element operator[](const char* key) const
    {
        for (const_iterator i = begin(), ie = end(); i != ie; ++i)
            if (!strcmp(i->name(), key))
                return *i;
        return Element();
    }
    
    friend std::ostream& operator << (std::ostream& out, const Object& obj)
    {
        if (obj.empty())
            return out << "{}";
        
        out << "{ ";
        bool comma = false;
        for (const_iterator i = obj.begin(), ie = obj.end(); i != ie; ++i) {
            if (comma)
                out << ", ";
            else
                comma = true;
            out << i->name() << ": ";
            i->printTo(out);
        }
        return out << " }";
    }
};

class Array: public impl::Base {
public:
    Array() {}
    explicit Array(const impl::Storage& storage): impl::Base(storage, storage.data()) {}
    Array(const impl::Storage& storage, const char* data): Base(storage, data) {}
    
    static Array construct(const char* data, size_t size) { return impl::Base::construct<Array>(data, size); }
    
    Element operator[](size_t size) const
    {
        for (const_iterator i = begin(), ie = end(); i != ie; ++i)
            if (!size--)
                return *i;
        return Element();
    }

    friend std::ostream& operator << (std::ostream& out, const Array& array)
    {
        if (array.empty())
            return out << "[]";
        
        out << "[ ";
        bool comma = false;
        for (const_iterator i = array.begin(), ie = array.end(); i != ie; ++i) {
            if (comma)
                out << ", ";
            else
                comma = true;
            i->printTo(out);
        }
        return out << " ]";
    }
};


class ObjectBuilder {
public:    
    template<class T>
    void put(const std::string& key, const T& value);
    
    void put(const std::string& key, const char* value) { put(key, std::string(value)); }
    
    class Proxy {
    public:
        explicit Proxy(ObjectBuilder* b, const std::string& key): b_(b), key_(key) {}
        template<class T> const T& operator = (const T& t) { b_->put(key_, t); return t; }
    private:
        ObjectBuilder* b_;
        std::string key_;
    };
    
    Proxy operator[](const std::string& key) { return Proxy(this, key); }
    
    Object obj() { return Object(impl_.finish()); }
    
private:
    impl::Builder impl_;
};

class ArrayBuilder {
public:
    ArrayBuilder(): idx_(0) {}
    
    template<class T>
    void put(const T& value);

    void put(const char* value) { put(std::string(value)); }
    
    template<class T>
    ArrayBuilder& operator << (const T& t) { put(t); return *this; }
    
    Array array() { return Array(impl_.finish()); }
    
private:
    impl::Builder impl_;
    size_t idx_;
};


namespace impl {

struct Nil {};

template<int T> struct ID { static int8_t type() { return T; } };

template<class T>
struct InnerBSON {
    typedef T Type;
    static size_t size(const char* ptr) { return *reinterpret_cast<const uint32_t*>(ptr); }
    static T get(const impl::Storage& storage, const char* data) { return T(storage, data); }
    static void push(impl::Builder& b, const T& obj) { b.push(obj.rawData(), obj.rawSize()); }
    static void print(std::ostream& out, const T& obj) { out << obj; }
};

template<class T>
struct Fixed {
    typedef T Type;
    static size_t size(const char*) { return sizeof(T); }
    static T get(const impl::Storage&, const char* ptr) { return *reinterpret_cast<const T*>(ptr); }
    static void push(impl::Builder& b, const T& t) { b.push(t); }
    static void print(std::ostream& out, const T& t) { out << t; }
};

template<class T>
struct Empty {
    typedef T Type;
    static size_t size(const char*) { return 0; }
    static T get(const impl::Storage&, const char*) { return T(); }
    static void push(impl::Builder&, const T&) {}
};

template<class T> struct Next {
    typedef T NextType;
};


namespace types {

    struct Timestamp: public ID<17>, public Fixed<bson::Timestamp>, public Next<Nil> {};
    struct ObjectID: public ID<7>, public Fixed<bson::ObjectID>, public Next<Timestamp> {};

    struct Null: public ID<10>, public Empty<bson::Null>, public Next<ObjectID> {
        static void print(std::ostream& out, const bson::Null&) { out << "null"; }
    };

    struct Array: public ID<4>, public InnerBSON<bson::Array>, public Next<Null> {};
    struct Object: public ID<3>, public InnerBSON<bson::Object>, public Next<Array> {};

    struct Time: public ID<9>, public Fixed<bson::Time>, public Next<Object> {};
    struct Double: public ID<1>, public Fixed<double>, public Next<Time> {};
    struct Bool: public ID<8>, public Fixed<bool>, public Next<Double> {};
    struct Int64: public ID<18>, public Fixed<int64_t>, public Next<Bool> {};
    struct Int32: public ID<16>, public Fixed<int32_t>, public Next<Int64> {};

    struct String: public ID<2>, public Next<Int32> {
        
        typedef std::string Type;
        
        static size_t size(const char* ptr) { return sizeof(uint32_t) + *reinterpret_cast<const uint32_t*>(ptr); }
        
        static std::string get(const impl::Storage&, const char* data)
        {
            return std::string(
                data + sizeof(uint32_t),
                *reinterpret_cast<const uint32_t*>(data) - 1
            );
        }
        
        static void push(impl::Builder& b, const std::string& str)
        {
            b.push<uint32_t>(str.size() + 1);
            b.push(str.c_str(), str.size() + 1);
        }
        
        static void print(std::ostream& out, const std::string& str);
    };

    struct Binary: public ID<5>, public Next<String> {
        typedef std::vector<char> Type;
        
        static size_t size(const char* ptr) { return sizeof(uint32_t) + 1 + *reinterpret_cast<const uint32_t*>(ptr); }
        
        static std::vector<char> get(const impl::Storage&, const char* data)
        {
            return std::vector<char>(
                data + sizeof(uint32_t) + 1,
                data + size(data)
            );
        }
        
        static void push(impl::Builder& b, const std::vector<char>& data)
        {
            b.push<uint32_t>(data.size());
            b.push<uint8_t>(0);
            b.push(data.data(), data.size());
        }
         
        static void print(std::ostream& out, const std::vector<char>&);
    };

    struct MinKey: public ID<0xFF>, public Empty<bson::MinKey>, public Next<Binary> {
        static void print(std::ostream& out, const bson::MinKey&) { out << "$minKey"; }
    };
    struct MaxKey: public ID<0x7F>, public Empty<bson::MaxKey>, public Next<MinKey> {
        static void print(std::ostream& out, const bson::MaxKey&) { out << "$maxKey"; }
    };

    struct All { typedef MaxKey NextType; };

} // namespace types

template<class X, class Y> struct IsSame { static const bool value = false; };
template<class X> struct IsSame<X, X> { static const bool value = true; };

template<class T, class MT, bool Eq>
struct MongoTypeImpl {
    typedef typename MongoTypeImpl<
        T,
        typename MT::NextType,
        IsSame<T, typename MT::NextType::Type>::value
    >::Type Type;
};

template<class T, class MT>
struct MongoTypeImpl<T, MT, true> {
    typedef MT Type;
};

template<class T, bool Eq> struct MongoTypeImpl<T, Nil, Eq> {};

template<class T> struct MongoType {
    typedef typename MongoTypeImpl<T, types::All, false>::Type Type;
};

template<class MT>
inline size_t elementSize(char type, const char* data)
{
    return type == MT::type()
        ? MT::size(data)
        : elementSize<typename MT::NextType>(type, data);
}

template<>
inline size_t elementSize<Nil>(char, const char*)
{
    throw BrokenBson("unknown BSON element type");
}


template<class MT>
inline void printTo(std::ostream& out, char type, const impl::Storage& storage, const char* data)
{
    if (type == MT::type())
        MT::print(out, MT::get(storage, data));
    else
        printTo<typename MT::NextType>(out, type, storage, data);
}

template<>
inline void printTo<Nil>(std::ostream& out, char, const impl::Storage&, const char*)
{
    out << "<??" "?>";
}

template<template<class> class Cmp, class MT>
struct Compare {
    static bool cmp(const Element& a, const Element& b)
    {
        if (a.type() == MT::type())
            return Cmp<typename MT::Type>()(
                a.as<typename MT::Type>(),
                b.as<typename MT::Type>()
            );
        else
            return Compare<Cmp, typename MT::NextType>::cmp(a, b);
    }
};

template<template<class> class Cmp>
struct Compare<Cmp, Nil> {
    static bool cmp(const Element& a, const Element& b)
    {
        // Last resort: compare byte sequences
        size_t sza = a.rawSize();
        size_t szb = b.rawSize();
        int res = memcmp(a.rawData(), b.rawData(), std::min(sza, szb));
        if (res != 0)
            return Cmp<int>()(res, 0);
        else
            return Cmp<size_t>()(sza, szb);
    }
};

} // namespace impl




template<class T>
inline bool Element::is() const
{
    return exists() && (type() == impl::MongoType<T>::Type::type());
}

inline bool Element::isNumber() const
{
    return is<int32_t>() || is<int64_t>() || is<double>();
}

template<class T>
inline T Element::asNumber() const
{
    if (exists() && type() == impl::MongoType<int32_t>::Type::type())
        return static_cast<T>(impl::MongoType<int32_t>::Type::get(storage_, value_));
    else if (exists() && type() == impl::MongoType<int64_t>::Type::type())
        return static_cast<T>(impl::MongoType<int64_t>::Type::get(storage_, value_));
    else if (exists() && type() == impl::MongoType<double>::Type::type())
        return static_cast<T>(impl::MongoType<double>::Type::get(storage_, value_));
    else
        throw TypeMismatch("type mismatch");
}

template<class T>
inline T Element::as() const
{
    typedef typename impl::MongoType<T>::Type MT;
    if (!exists() || type() != MT::type())
        throw TypeMismatch("type mismatch");
    return MT::get(storage_, value_);
}

#define BSON_IMPL_INTEGER(type) \
    template<> inline type Element::as<type>() const { return asNumber<type>(); } \
    template<> inline bool Element::canBe<type>() const { return isNumber(); }

BSON_IMPL_INTEGER(signed short);
BSON_IMPL_INTEGER(signed int);
BSON_IMPL_INTEGER(signed long);
BSON_IMPL_INTEGER(signed long long);
BSON_IMPL_INTEGER(unsigned short);
BSON_IMPL_INTEGER(unsigned int);
BSON_IMPL_INTEGER(unsigned long);
BSON_IMPL_INTEGER(unsigned long long);
BSON_IMPL_INTEGER(float);
BSON_IMPL_INTEGER(double);
BSON_IMPL_INTEGER(long double);

#undef BSON_IMPL_INTEGER

template<> inline bool Element::canBe<bool>() const { return is<bool>() || isNumber(); }
template<> inline bool Element::as<bool>() const
{
    if (!exists())
        throw TypeMismatch("type mismatch");
    else if (is<bool>())
        return impl::MongoType<bool>::Type::get(storage_, value_);
    else if (isNumber())
        return asNumber<int>();
    else
        throw TypeMismatch("type mismatch");
}

template<> inline bool Element::is<Any>() const { return exists(); }

template<> inline Any Element::as<Any>() const
{
    if (!exists())
        throw TypeMismatch("type mismatch");
    return Any();
}

template<template<class> class Cmp>
inline bool Element::cmpValues(const Element& a, const Element& b)
{
    return impl::Compare<Cmp, impl::types::All::NextType>::cmp(a, b);
}


inline size_t Element::rawSize() const
{
    return exists() ? (value_ - data_) + impl::elementSize<impl::types::All::NextType>(*data_, value_) : 0;
}

inline void Element::printTo(std::ostream& out) const
{
    if (exists())
        impl::printTo<impl::types::All::NextType>(out, *data_, storage_, value_);
}

inline Element Element::operator[](const char* field) const
{
    if (is<bson::Object>())
        return as<bson::Object>()[field];
    else
        return Element();
}

inline Element Element::operator[](size_t idx) const
{
    if (is<bson::Array>())
        return as<bson::Array>()[idx];
    else
        return Element();
}


template<class T>
inline void ObjectBuilder::put(const std::string& key, const T& value)
{
    typedef typename impl::MongoType<T>::Type MT;
    impl_.push<int8_t>(MT::type());
    impl_.push(key.c_str(), key.size() + 1);
    MT::push(impl_, value);
}

template<>
inline void ObjectBuilder::put(const std::string& key, const Element& el)
{
    impl_.push<int8_t>(el.type());
    impl_.push(key.c_str(), key.size() + 1);
    impl_.push(el.valueData(), el.valueSize());
}

template<class T>
inline void ArrayBuilder::put(const T& value)
{
    std::ostringstream s;
    s << idx_++;
    std::string key = s.str();

    typedef typename impl::MongoType<T>::Type MT;
    impl_.push<int8_t>(MT::type());
    impl_.push(key.c_str(), key.size() + 1);
    MT::push(impl_, value);
}

template<>
inline void ArrayBuilder::put(const Element& el)
{
    std::ostringstream s;
    s << idx_++;
    std::string key = s.str();
    
    impl_.push<int8_t>(el.type());
    impl_.push(key.c_str(), key.size() + 1);
    impl_.push(el.valueData(), el.valueSize());
}

} // namespace bson
