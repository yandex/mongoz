#include <syncio/syncio.h>
#include <atomic>
#include <cassert>

io::mutex m;
io::condition_variable cv;
std::atomic_int val;

io::task<void> spawnWaitAndIncrement()
{
    return io::spawn([&]{
        std::unique_lock<io::mutex> lock(m);
        cv.wait(lock);
        ++val;
    });
}

void test_notify()
{
    io::spawn([&]{
        cv.notify_all();
        cv.notify_one();
    }).join();
}

void test_wait_notify()
{
    val = 0;
    auto incTask = spawnWaitAndIncrement();
    io::spawn([&]{ cv.notify_all(); }).detach();

    io::wait(incTask);
    assert(val == 1);
}

void test_wait_notify_locked()
{
    val = 0;
    auto incTask = spawnWaitAndIncrement();
    io::spawn([&]{
        std::lock_guard<io::mutex> lock(m);
        cv.notify_all();
        assert(val == 0);
    }).detach();

    io::wait(incTask);
    assert(val == 1);
}

void test_cancel()
{
    val = 0;
    auto incTask = spawnWaitAndIncrement();
    io::spawn([&]{
        incTask.cancel();
        io::wait(incTask);
    }).detach();

    io::wait(incTask);
    assert(val == 0);
}

void test_cancel_locked()
{
    val = 0;
    auto incTask = spawnWaitAndIncrement();
    io::spawn([&]{
        std::lock_guard<io::mutex> lock(m);
        incTask.cancel();
        // NB: cannot wait while holding lock
    }).detach();

    io::wait(incTask);
    assert(val == 0);
}

void test_cancel_locked_notified()
{
    auto incTask = spawnWaitAndIncrement();
    io::spawn([&]{
        std::lock_guard<io::mutex> lock(m);
        cv.notify_all();
        incTask.cancel();
    }).detach();

    io::wait(incTask);
    assert(val == 0 || val == 1);
}

void test_wait_notify_all()
{
    val = 0;
    auto firstIncTask = spawnWaitAndIncrement();
    auto secondIncTask = spawnWaitAndIncrement();
    io::spawn([&]{ cv.notify_all(); }).detach();

    io::wait_all(firstIncTask, secondIncTask);
    assert(val == 2);
}

void test_wait_notify_one()
{
    val = 0;
    auto firstIncTask = spawnWaitAndIncrement();
    auto secondIncTask = spawnWaitAndIncrement();
    io::spawn([&]{ cv.notify_one(); }).join();
    firstIncTask.cancel();
    secondIncTask.cancel();

    io::wait_all(firstIncTask, secondIncTask);
    assert(val == 1);
}

void test_wait_for()
{
    val = 0;
    auto incTask = io::spawn([&]{
        std::unique_lock<io::mutex> lock(m);
        auto res = cv.wait_for(lock, 24_h);
        ++val;
        return res;
    });
    io::spawn([&]{ cv.notify_all(); }).detach();

    assert(incTask.join() == std::cv_status::no_timeout);
    assert(val == 1);
}

void test_wait_for_timeout()
{
    val = 0;
    auto incTask = io::spawn([&]{
        std::unique_lock<io::mutex> lock(m);
        auto res = cv.wait_for(lock, 0_s);
        ++val;
        return res;
    });

    assert(incTask.join() == std::cv_status::timeout);
    assert(val == 1);
}

void test_wait_pred()
{
    val = 0;
    std::atomic_bool flag{false};
    auto incTask = io::spawn([&]{
        std::unique_lock<io::mutex> lock(m);
        cv.wait(lock, [&flag]{ return flag.load(); });
        ++val;
    });

    io::spawn([&]{ cv.notify_all(); }).join();
    io::wait(incTask, 100_ms);
    assert(val == 0);

    flag = true;
    io::spawn([&]{ cv.notify_all(); }).detach();
    io::wait(incTask);
    assert(val == 1);
}

void test_wait_pred_preset()
{
    val = 0;
    auto incTask = io::spawn([&]{
        std::unique_lock<io::mutex> lock(m);
        cv.wait(lock, []{ return true; });
        ++val;
    });

    io::wait(incTask);
    assert(val == 1);
}

void test_wait_pred_fail()
{
    val = 0;
    std::atomic_bool flag{false};
    auto incTask = io::spawn([&]{
        std::unique_lock<io::mutex> lock(m);
        cv.wait(lock, [&flag]{ return flag.load(); });
        ++val;
    });

    io::spawn([&]{ cv.notify_all(); }).join();
    io::wait(incTask, 100_ms);
    incTask.cancel();

    io::wait(incTask);
    assert(val == 0);
}

void test_wait_for_pred()
{
    val = 0;
    std::atomic_bool flag{false};
    auto incTask = io::spawn([&]{
        std::unique_lock<io::mutex> lock(m);
        auto res = cv.wait_for(lock, 24_h, [&flag]{ return flag.load(); });
        ++val;
        return res;
    });

    io::spawn([&]{ cv.notify_all(); }).join();
    io::wait(incTask, 100_ms);
    assert(val == 0);

    flag = true;
    io::spawn([&]{ cv.notify_all(); }).detach();
    assert(incTask.join());
    assert(val == 1);
}

void test_wait_for_pred_preset()
{
    val = 0;
    auto incTask = io::spawn([&]{
        std::unique_lock<io::mutex> lock(m);
        cv.wait_for(lock, 24_h, []{ return true; });
        ++val;
    });

    io::wait(incTask);
    assert(val == 1);
}

void test_wait_for_pred_timeout()
{
    val = 0;
    std::atomic_bool flag{false};
    auto incTask = io::spawn([&]{
        std::unique_lock<io::mutex> lock(m);
        auto res = cv.wait_for(lock, 0_s, [&flag]{ return flag.exchange(true); });
        ++val;
        return res;
    });

    assert(incTask.join());
    assert(val == 1);
}

void test_wait_for_pred_fail_timeout()
{
    val = 0;
    auto incTask = io::spawn([&]{
        std::unique_lock<io::mutex> lock(m);
        auto res = cv.wait_for(lock, 0_s, []{ return false; });
        ++val;
        return res;
    });

    assert(!incTask.join());
    assert(val == 1);
}

int main()
{
    io::engine engine;
    engine.spawn([&]{
        test_notify();
        test_wait_notify();
        test_wait_notify_locked();
        test_cancel();
        test_cancel_locked();
        test_cancel_locked_notified();
        test_wait_notify_all();
        test_wait_notify_one();
        test_wait_for();
        test_wait_for_timeout();
        test_wait_pred();
        test_wait_pred_preset();
        test_wait_pred_fail();
        test_wait_for_pred();
        test_wait_for_pred_preset();
        test_wait_for_pred_timeout();
        test_wait_for_pred_fail_timeout();
    }).detach();
    engine.run();

    return 0;
}
