#include <syncio/syncio.h>
#include <string>
#include <cassert>
#include <algorithm>

int main()
{
    io::engine engine;
    engine.spawn([]{
            
        io::task<int> t = io::spawn([]{
            auto addrs = io::resolve("example.com:80");
            assert(!addrs.empty());
            io::stream http( io::connect(addrs.front()) );
            http << "HEAD / HTTP/1.0\r\nHost: example.com\r\n\r\n" << std::flush;

            std::string line;
            std::getline(http, line);
            
            std::vector<std::string> parts(1, "");
            for (char c: line) {
                if (c != ' ')
                    parts.back().push_back(c);
                else if (!parts.back().empty())
                    parts.emplace_back();
            }
            
            assert(parts.size() == 3);
            assert(std::all_of(parts[1].begin(), parts[1].end(), &isdigit));
            return atoi(parts[1].c_str());
        });
        io::wait(t, 5_s);
        assert(t.succeeded());
        t.get();
    }).detach();
    
    engine.run();
    return 0;
}
