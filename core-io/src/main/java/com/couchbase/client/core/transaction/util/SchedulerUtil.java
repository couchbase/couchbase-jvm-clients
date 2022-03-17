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
    SchedulerUtil() {}

    // 100 is arbritary.  There should be nothing blocking happening on this scheduler so only a handful of threads should be
    // created and used in practice
    public final static Scheduler scheduler = Schedulers.newBoundedElastic(100, Integer.MAX_VALUE, "cb-txn");
    public final static Scheduler schedulerCleanup = Schedulers.newBoundedElastic(100, Integer.MAX_VALUE, "cb-txn-cleanup");

    // AttemptContext will block on all operations, tying up a thread each time.  So, we don't (realistically) limit the thread pool.
    // The two schedulers are separated to help with diagnosing blocking problems.
    public final static Scheduler schedulerBlocking = Schedulers.newBoundedElastic(100_000, Integer.MAX_VALUE, "cb-txn-blocking");

    public static void shutdown() {
        // TODO ESI
//        SchedulerUtil.schedulerCleanup.dispose();
//        SchedulerUtil.scheduler.dispose();
//        SchedulerUtil.schedulerBlocking.dispose();
    }
}
