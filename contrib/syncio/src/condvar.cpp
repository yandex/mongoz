#include "condvar.h"
#include <syncio/condvar.h>

namespace io {

condition_variable::condition_variable(): impl_(new impl::CondVar) {}
condition_variable::~condition_variable() {}

void condition_variable::notify_one() { impl_->notifyOne(); }
void condition_variable::notify_all() { impl_->notifyAll(); }

void condition_variable::wait(std::unique_lock<mutex>& lock)
{
    impl_->wait(lock, {});
}

std::cv_status condition_variable::wait_until(std::unique_lock<mutex>& lock, timeout timeout)
{
    return impl_->wait(lock, timeout);
}

} // namespace io
