/**
 * http.cpp -- HTTP-related stuff (monitoring, status and such)
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

#include "config.h"
#include "backend.h"
#include "shard.h"
#include <string>
#include <unordered_map>
#include <ostream>
#include <memory>

namespace http {

static const char* CSS = R"(<style type='text/css'><!--

body { font-family: sans-serif; }
tr.first td { padding-top: 0.5em; }
td { padding-right: 2em; }
th { text-align: left; }
tr.shard td { padding-top: 1em; padding-bottom: 0.5em; font-size: 120%; font-weight: bold; }
h1 { border-bottom: black 1px solid; }

--></style>)";

void showShards(std::unordered_map<std::string, std::string>& headers, std::ostream& response)
{
    headers["Content-Type"] = "text/html";
    response << "<html><head><title>mongoz shards</title>" << CSS << "<body>";
    
    auto status = monitoring::check();
    if (!status.messages().empty()) {
        response << "<h1>Issues</h1><ul>";
        for (const std::string& msg: status.messages())
            response << "<li>" << msg << "</li>";
        response << "</ul></h1>";
    }
        
    response << "<h1>Shards</h1><table>"
             << "<tr class='header'>"
             << "<th class='leftspacer'>&nbsp;</th>"
             << "<th>Backend</th>"
             << "<th>Status</th>"
             << "<th>Lag</th>"
             << "<th>Address</th>"
             << "<th>RTT</th></tr>";

    try {
        std::shared_ptr<Config> conf = g_config->get();
        for (const std::shared_ptr<Shard>& shard: conf->shards()) {
            response << "<tr class='shard'><td colspan='5'>" << shard->id() << "</td></tr>";
            for (const std::unique_ptr<Backend>& backend: shard->backends()) {
                bool first = true;
                for (const std::unique_ptr<Endpoint>& endpt: backend->endpoints()) {
                    if (first) {
                        auto lag = shard->replicationLag(backend.get());
                        response << "<tr class='first'><td class='leftspacer'>&nbsp;</td>"
                                 << "<td>" << backend->addr() << "</td>"
                                 << "<td>" << shard->status(backend.get()) << "</td>";
                        if (lag != decltype(lag)::max())
                            response << "<td>" << std::chrono::duration_cast<std::chrono::seconds>(lag).count() << " s</td>";
                        else
                            response << "<td>&mdash;</td>";
                        first = false;
                    } else {
                        response << "<tr><td class='leftspacer'>&nbsp;</td><td></td><td></td><td></td>";
                    }
                    response << "<td>" << endpt->addr() << "</td><td>";
                    if (endpt->alive())
                        response << std::chrono::duration_cast<std::chrono::milliseconds>(endpt->roundtrip()).count() << " ms";
                    else
                        response << "DEAD";
                    response << "</td></tr>";
                }
            }
        }

        response << "</table>";
    }
    catch (const errors::NoShardConfig&) {
        response << "<span style='color: red'>No shard config yet</span>";
    }

    response << "</body></html>";
}


void showMonitor(std::unordered_map<std::string, std::string>& headers, std::ostream& response)
{
    auto status = monitoring::check();
    
    headers["Content-Type"] = "text/plain";
    if (status.level() == monitoring::Status::OK)
        response << "OK\n";
    else if (status.level() == monitoring::Status::WARNING)
        response << "WARNING\n";
    else if (status.level() == monitoring::Status::CRITICAL)
        response << "CRITICAL\n";

    bool first = true;
    for (const std::string& msg: status.messages()) {
        if (first)
            first = false;
        else
            response << "; ";
        response << msg;
    }
    if (status.messages().empty() && status.level() == monitoring::Status::OK)
        response << "OK";
    response << "\n";
}


void dispatch(const std::string& query, std::unordered_map<std::string, std::string>& headers, std::ostream& response)
{
    if (query == "/")
        showShards(headers, response);
    else if (query == "/monitor")
        showMonitor(headers, response);
    else {
        response << "Not found";
        headers["Content-Type"] = "text/plain";
        headers["Status"] = "404";
    }
}

} // namespace http

