/**
 * version.h -- chunk version class
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

#include <bson/bson.h>
#include <iostream>

class ChunkVersion {
public:
    ChunkVersion() {}
    ChunkVersion(const bson::ObjectID& epoch, const bson::Timestamp& ts): epoch_(epoch), stamp_(ts) {}
    
    const bson::ObjectID& epoch() const { return epoch_; }
    const bson::Timestamp& stamp() const { return stamp_; }
    
    bool operator == (const ChunkVersion& v) const { return epoch_ == v.epoch_ && stamp_ == v.stamp_; }
    bool operator != (const ChunkVersion& v) const { return !(*this == v); }
    
    friend std::ostream& operator << (std::ostream& out, const ChunkVersion& v) {
        return out << "Version(" << v.epoch_ << ", " << v.stamp_ << ")";
    }
    
private:
    bson::ObjectID epoch_;
    bson::Timestamp stamp_;
};
