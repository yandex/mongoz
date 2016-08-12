/**
 * monitor.cpp -- diagnostic messages for mongoz health monitoring
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
#include "monitor.h"

namespace monitoring {

Status check()
{
    monitoring::Status status = monitoring::Status::ok();
    if (g_config->exists()) {
        std::shared_ptr<Config> config = g_config->get();
        for (const std::shared_ptr<Shard>& shard: config->shards())
            status.merge(shard->status());
        if (options().monitorConfigAge != decltype(Options::monitorConfigAge)::max()
            && SteadyClock::now() >= config->createdAt() + options().monitorConfigAge)
        {
            status.merge(monitoring::Status::critical("cannot update shard config for "
                + std::to_string(std::chrono::duration_cast<std::chrono::minutes>(SteadyClock::now() - config->createdAt()).count())
                + " min"));
        }
    } else {
        status.merge(monitoring::Status::critical("no config available"));
        status.merge(g_config->shard()->status());
    }

    return status;
}

} // namespace monitoring
