#include <syncio/syncio.h>
#include <thread>
#include <cassert>

int main()
{
    io::engine engine;
    
    std::atomic<size_t> counter { 0 };
    for (size_t i = 0; i != 1000; ++i) {
        engine.spawn([&counter]{
            for (int j = 0; j != 10; ++j) {
                ++counter;
                io::sleep(100_ms);
            }
        }).detach();
    }
    
    std::vector<std::thread> threads;
    for (size_t i = 0; i != std::thread::hardware_concurrency(); ++i)
        threads.emplace_back([&engine]{ engine.run(); });
    for (auto& th: threads)
        th.join();
    
    assert(counter == 10000);
    return 0;
}
