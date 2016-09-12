#include <syncio/syncio.h>
#include <cassert>

class TaskAliveChecker
{
public:
    TaskAliveChecker(io::task<void>& task): task_(task) {}
    ~TaskAliveChecker() { assert(io::wait(task_, 0_s) == -ETIMEDOUT); }

private:
    io::task<void>& task_;
};

/*
 * Checks that cancelling a task while waiting for another task completion
 * doesn't cause it to break top-to-bottom finalization order.
*/
int main()
{
    io::engine engine;

    engine.spawn([]{
        io::mutex mutex;
        io::condition_variable cv;

        io::task<void> middle;
        middle = io::spawn([&]{
            auto top = io::spawn([&]{
                TaskAliveChecker checkAlive(middle);
                io::sleep({});
            });

            io::sleep(0_s); // yield to top
            cv.notify_one();
        });

        {
            std::unique_lock<io::mutex> lock(mutex);
            cv.wait(lock);
        }

        middle.cancel();
        io::wait(middle);
    }).detach();

    engine.run();
    return 0;
}
