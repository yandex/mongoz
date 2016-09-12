/**
 * mutex.h -- synchronization interface for syncio
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

#include <memory>
#include <mutex>
#include <atomic>
#include <cassert>
#include <pthread.h>

namespace io {
namespace impl { class Mutex; }

// This mutex can be used for performing i/o while holding it.
class mutex {
public:
    mutex();
    mutex(const mutex&) = delete;
    mutex& operator = (const mutex&) = delete;
    ~mutex();
    
    void lock();
    void unlock();

private:
    std::unique_ptr<impl::Mutex> impl_;
};

namespace sys {

namespace impl {
    void trackLock();
    void trackUnlock();
    void assertUnlocked();
} // namespace impl

// A wrapper for std::mutex, tracking attempts to do i/i
// while holding the mutex.
class mutex {
public:
    mutex(): lockCount_(0) {}
    ~mutex() { assert(!lockCount_); }

    void lock()   { mutex_.lock(); assert(!lockCount_++); impl::trackLock(); }
    void unlock() { impl::trackUnlock(); assert(!--lockCount_); mutex_.unlock(); }

private:
    std::mutex mutex_;
    std::atomic<int> lockCount_;
};

class shared_mutex {
public:
    shared_mutex(): lockCount_(0) { pthread_rwlock_init(&lock_, 0); }
    ~shared_mutex() { assert(!lockCount_); pthread_rwlock_destroy(&lock_); }
    
    void lock()          { pthread_rwlock_wrlock(&lock_); assert(!lockCount_--);     impl::trackLock();   }
    void lock_shared()   { pthread_rwlock_rdlock(&lock_); assert(lockCount_++ >= 0); impl::trackLock();   }
    void unlock()        { impl::trackUnlock(); assert(!++lockCount_);     pthread_rwlock_unlock(&lock_); }
    void unlock_shared() { impl::trackUnlock(); assert(--lockCount_ >= 0); pthread_rwlock_unlock(&lock_); }
    
private:
    pthread_rwlock_t lock_;
    std::atomic<int> lockCount_; // 0 -- unlocked; >=1 -- shared lock; -1 -- exclusive lock
    
    shared_mutex(const shared_mutex&) = delete;
    shared_mutex& operator = (const shared_mutex&) = delete;
};

} // namespace sys

template<class Mutex>
class shared_lock {
public:
    shared_lock(): mutex_(0), locked_(false) {}
    explicit shared_lock(Mutex& mutex): mutex_(&mutex), locked_(true) { mutex_->lock_shared(); }
    ~shared_lock() { unlock(); }
    
    shared_lock(shared_lock<Mutex>&& lk): mutex_(lk.mutex_), locked_(lk.locked_) { lk.mutex_ = 0; }
    shared_lock& operator = (shared_lock<Mutex>&& lk) { std::swap(mutex_, lk.mutex_); std::swap(locked_, lk.locked_); return *this; }
    
    void lock() { if (mutex_ && !locked_) { mutex_->lock_shared(); locked_ = true; } }
    void unlock() { if (mutex_ && locked_) { mutex_->unlock_shared(); locked_ = false; } }

private:
    Mutex* mutex_;
    bool locked_;

    shared_lock(const shared_lock<Mutex>&) = delete;
    shared_lock& operator = (const shared_lock<Mutex>&) = delete;
};


} // namespace io
