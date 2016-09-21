/**
 * monitor.h -- diagnostic messages for mongoz health monitoring
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

#include <string>

namespace monitoring {

class Status {
public:
    enum Level { OK = 0, WARNING = 1, CRITICAL = 2 };
    
    Status(): level_(OK) {}
    explicit Status(Level level): level_(level) {}
    Status(Level level, std::string msg): level_(level), msgs_(1, std::move(msg)) {}
    Status(Level level, std::vector<std::string> msgs): level_(level), msgs_(std::move(msgs)) {}
    
    static Status ok() { return Status(); }
    static Status warning(std::string msg) { return Status(WARNING, std::move(msg)); }
    static Status critical(std::string msg) { return Status(CRITICAL, std::move(msg)); }
    
    Level level() const { return level_; }
    const std::vector<std::string>& messages() const { return msgs_; }
    
    Status& merge(const Status& other)
    {
        level_ = std::max(level_, other.level_);
        msgs_.insert(msgs_.end(), other.msgs_.begin(), other.msgs_.end());
        return *this;
    }
    
private:
    Level level_;
    std::vector<std::string> msgs_;
};

Status check();

} // namespace monitoring
