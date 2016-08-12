#include "mutex.h"
#include <syncio/mutex.h>

namespace io {

mutex::mutex(): impl_(new impl::Mutex) {}
mutex::~mutex() {}

void mutex::lock() { impl_->lock(); }
void mutex::unlock() { impl_->unlock(); }


namespace sys { namespace impl {

thread_local std::atomic<int> g_lockCount { 0 };

void trackLock() { ++g_lockCount; }
void trackUnlock() { --g_lockCount; }
void assertUnlocked() { assert(!g_lockCount); }

}} // namespace sys::impl

} // namespace io
