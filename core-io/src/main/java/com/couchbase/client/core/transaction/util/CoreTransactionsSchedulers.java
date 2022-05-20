/*
 * Copyright 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.core.transaction.util;

import com.couchbase.client.core.annotation.Stability;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * Mainly to aid debugging, transactions use their own pool of schedulers.  Though the underlying KV and query operations
 * are done using the standard SDK schedulers.
 */
@Stability.Internal
public class CoreTransactionsSchedulers {
    // Same as BoundedElasticScheduler.DEFAULT_TTL_SECONDS, which is private
    private final static int DEFAULT_TTL_SECONDS = 60;

    private final Scheduler schedulerCleanup = createScheduler(100, "cb-txn-cleanup");

    // The scheduler we run the lambda on.  In blocking mode, this will use up one thread per transaction.
    // Applications performing large numbers of concurrent transactions should therefore prefer the reactive API.
    //
    // The other key benefit to this scheduler is we run anything in 'user space' (e.g. including when passing back
    // control the lambda in reactive API) on this scheduler, rather than on a limited SDK one.  This lets the
    // user accidentally block, without deadlocking the SDK.
    private final Scheduler schedulerBlocking = createScheduler(100_000, "cb-txn");

    private Scheduler createScheduler(int threadCap, String name) {
        // Create daemon threads so we don't block the JVM from exiting if the user forgets cluster.disconnect()
        return Schedulers.newBoundedElastic(threadCap, Integer.MAX_VALUE, name, DEFAULT_TTL_SECONDS, true);
    }

    public Scheduler schedulerCleanup() {
        return schedulerCleanup;
    }

    public Scheduler schedulerBlocking() {
        return schedulerBlocking;
    }

    public void shutdown() {
        schedulerCleanup.dispose();
        schedulerBlocking.dispose();
    }
}
