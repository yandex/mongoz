/**
 * sorted_vector.h -- um... a sorted vector.
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

#include <vector>
#include <functional>
#include <stdexcept>
#include <algorithm>

/// Um, a sorted vector.
/// More effective than std::map<> if gets populated
/// just once and not modified anymore.
template<class T, class Key>
class SortedVector {
public:

    typedef std::function<Key(const T&)> Extractor;
    class Cmp {
    public:
        explicit Cmp(Extractor ex): ex_(std::move(ex)) {}
        
        template<class X, class Y>
        bool operator()(const X& x, const Y& y) const
        {
            return extractKey(x) < extractKey(y);
        }
        
    private:
        Extractor ex_;
        
        Key extractKey(const Key& k) const { return k; }
        Key extractKey(const T& t) const { return ex_(t); }
    };
    
    explicit SortedVector(std::function<Key(const T&)> extractor): extractor_(extractor), cmp_(extractor) {}
    
    std::vector<T>& vector() { return v_; }
    void finish() { std::sort(v_.begin(), v_.end(), cmp_); }

    typedef typename std::vector<T>::iterator iterator;
    iterator begin()  { return v_.begin(); }
    iterator end()    { return v_.end(); }

    typedef typename std::vector<T>::const_iterator const_iterator;
    const_iterator begin() const { return v_.begin(); }
    const_iterator end() const   { return v_.end(); }
    
    const_iterator lower_bound(const Key& k) const { return std::lower_bound(begin(), end(), k, cmp_); }
    const_iterator upper_bound(const Key& k) const { return std::upper_bound(begin(), end(), k, cmp_); }
    
    std::pair<const_iterator, const_iterator> equal_range(const Key& k) const {
        return std::equal_range(begin(), end(), k, cmp_);
    }
    
    const_iterator find(const Key& k) const
    {
        const_iterator i = lower_bound(k);
        return (i != end() && extractor_(*i) == k) ? i : end();
    }
    
    const T* findPtr(const Key& k) const
    {
        auto i = find(k);
        return i != end() ? &*i : 0;
    }
    
    const T& findRef(const Key& k) const
    {
        auto i = find(k);
        if (i == end())
            throw std::out_of_range("key not found");
        return *i;
    }
    
private:
    std::vector<T> v_;
    Extractor extractor_;
    Cmp cmp_;
};
