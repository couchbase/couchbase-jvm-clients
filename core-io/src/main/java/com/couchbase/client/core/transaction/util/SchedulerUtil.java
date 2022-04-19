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

@Stability.Internal
public class SchedulerUtil {
    private SchedulerUtil() {}

    // Same as BoundedElasticScheduler.DEFAULT_TTL_SECONDS, which is private
    private final static int DEFAULT_TTL_SECONDS = 60;

    // 100 is arbitrary.  There should be nothing blocking happening on this scheduler so only a handful of threads should be
    // created and used in practice
    public final static Scheduler scheduler = createScheduler(100, "cb-txn");
    public final static Scheduler schedulerCleanup = createScheduler(100, "cb-txn-cleanup");

    // AttemptContext will block on all operations, tying up a thread each time.  So, we don't (realistically) limit the thread pool.
    // The two schedulers are separated to help with diagnosing blocking problems.
    public final static Scheduler schedulerBlocking = createScheduler(100_000, "cb-txn-blocking");

    private static Scheduler createScheduler(int threadCap, String name) {
        // Create daemon threads, so we don't block the JVM from exiting if the user forgets cluster.disconnect()
        return Schedulers.newBoundedElastic(threadCap, Integer.MAX_VALUE, name, DEFAULT_TTL_SECONDS, true);
    }
    public static void shutdown() {
        // TODO ESI
//        SchedulerUtil.schedulerCleanup.dispose();
//        SchedulerUtil.scheduler.dispose();
//        SchedulerUtil.schedulerBlocking.dispose();
    }
}
