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
import org.jspecify.annotations.NonNull;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Mainly to aid debugging, transactions use their own pool of schedulers.  Though the underlying KV and query operations
 * are done using the standard SDK schedulers.
 */
@Stability.Internal
public class CoreTransactionsSchedulers {
    private final static String BLOCKING_SYNC_THREAD_PREFIX = "cb-txnb";

    // The scheduler/executor used as-needed for transactional operations, which is an uncapped caching thread pool.
    //
    // A key benefit to this scheduler is we run anything in 'user space' (e.g. including when passing back
    // control the lambda in reactive API) on this scheduler, rather than on a limited SDK one.  This lets the
    // user accidentally block, without deadlocking the SDK.
    private final ExecutorService blockingExecutor = Executors.newCachedThreadPool(new BlockingSyncThreadFactory());
    private final Scheduler schedulerBlocking = Schedulers.fromExecutor(blockingExecutor);

    public Scheduler schedulerBlocking() {
        return schedulerBlocking;
    }

    public ExecutorService blockingExecutor() {
        return blockingExecutor;
    }

    public void shutdown() {
        schedulerBlocking.dispose();
        blockingExecutor.shutdown();
    }

    private static final class BlockingSyncThreadFactory implements ThreadFactory {
        private final AtomicInteger counter = new AtomicInteger();

        @Override
        public Thread newThread(@NonNull Runnable r) {
            Thread t = new Thread(r);
            t.setName(BLOCKING_SYNC_THREAD_PREFIX + "-" + counter.incrementAndGet());
            // Create daemon threads so we don't block the JVM from exiting if the user forgets cluster.disconnect()
            t.setDaemon(true);
            return t;
        }
    }
}
