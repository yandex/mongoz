/**
 * error.h -- various classes used to report different errors
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

#include <stdexcept>
#include <string>

namespace errors {

/// A base class of all errors.
/// Everything thrown which is not a subclass of Error
/// must be carefully logged and investigated.
class Error: public std::runtime_error {
public:
    Error(): std::runtime_error("Error") {}
    explicit Error(const std::string& errmsg): std::runtime_error(errmsg) {}
    Error(const std::string& reporter, const std::string& errmsg):
        std::runtime_error("``" + errmsg + "'' (reported by " + reporter + ")") {}
};

/// Error in internal logic
/// (deliberately not a subclass of Error)
class AssertionFailed: public std::runtime_error {
public:
    explicit AssertionFailed(const std::string& errmsg): std::runtime_error(errmsg) {}
};


#define DEFINE_ERROR(klass, base) \
    class klass: public base { \
    public: \
        klass(): base(#klass) {} \
        klass(std::string errmsg): base(std::move(errmsg)) {} \
        klass(std::string reporter, std::string errmsg): \
            base(std::move(reporter), std::move(errmsg)) \
        {} \
    }

/// Backend behaved the way it was not expected to
/// (broken messages, unexpecped replies, etc...)
DEFINE_ERROR(BackendInternalError, Error);

/// An error reported by backend in a sane way
DEFINE_ERROR(BackendClientError,   Error);

/// Subclasses of BackendClientError, clarifying error cause
DEFINE_ERROR(CursorNotFound,       BackendClientError);
DEFINE_ERROR(QueryFailure,         BackendClientError);
DEFINE_ERROR(ShardConfigStale,     BackendClientError);
DEFINE_ERROR(NotMaster,            BackendClientError);
DEFINE_ERROR(PermanentFailure,     BackendClientError);
DEFINE_ERROR(ConnectivityError,    BackendClientError);

DEFINE_ERROR(NoSuitableBackend,    Error);

DEFINE_ERROR(NoShardConfig,        Error);
DEFINE_ERROR(ShardConfigBroken,    Error);

DEFINE_ERROR(BadRequest,           Error);
DEFINE_ERROR(Unauthorized,         Error);
DEFINE_ERROR(NotImplemented,       Error);

} // namespace errors

#define REQUIRE(x) if (x) {} else throw errors::Error("Assertion failed: " #x);
#define ASSERT(x)  if (x) {} else throw errors::AssertionFailed("Assertion failed: " #x);
