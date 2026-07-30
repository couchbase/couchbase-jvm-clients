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

import java.util.concurrent.ExecutorService;

import static com.couchbase.client.core.util.CbThreads.unboundedExecutorService;

/**
 * Mainly to aid debugging, transactions use their own pool of schedulers.  Though the underlying KV and query operations
 * are done using the standard SDK schedulers.
 */
@Stability.Internal
public class CoreTransactionsSchedulers {
    private final static String BLOCKING_SYNC_THREAD_PREFIX = "cb-txnb-";

    // The scheduler/executor used as-needed for transactional operations, which is an uncapped caching thread pool.
    //
    // A key benefit to this scheduler is we run anything in 'user space' (e.g. including when passing back
    // control the lambda in reactive API) on this scheduler, rather than on a limited SDK one.  This lets the
    // user accidentally block, without deadlocking the SDK.
    private final ExecutorService blockingExecutor = unboundedExecutorService(BLOCKING_SYNC_THREAD_PREFIX);
    private final Scheduler schedulerBlocking = Schedulers.fromExecutor(blockingExecutor);

    public Scheduler schedulerBlocking() {
        return schedulerBlocking;
    }

    public ExecutorService blockingExecutor() {
        return blockingExecutor;
    }

    public static void requireTransactionBlockingThread() {
        if (!Thread.currentThread().getName().startsWith(BLOCKING_SYNC_THREAD_PREFIX)) {
            throw new IllegalStateException("This method can only be called in a blockable transactions I/O thread, but current thread is " + Thread.currentThread());
        }
    }

    public void shutdown() {
        schedulerBlocking.dispose();
        blockingExecutor.shutdown();
    }
}
