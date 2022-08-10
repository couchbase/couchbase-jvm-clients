/*
 * Copyright (c) 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.performer.core.perf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * WorkloadsRunner creates however many threads need to run in tandem based on the given horizontal scaling value
 */
public class WorkloadsRunner {
    private static final Logger logger = LoggerFactory.getLogger(WorkloadsRunner.class);

    public static void run(com.couchbase.client.protocol.run.Workloads workloadRun,
                           PerRun perRun,
                           Function<PerHorizontalScaling, Thread> createThread) {
        try{
            var runners = new ArrayList<Thread>();
            for (int runnerIndex = 0; runnerIndex < workloadRun.getHorizontalScalingCount(); runnerIndex ++) {
                var perThread = workloadRun.getHorizontalScaling(runnerIndex);
                runners.add(createThread.apply(new PerHorizontalScaling(runnerIndex,
                        perThread,
                        perRun)));
            }

            for (Thread runner : runners) {
                runner.start();
            }
            logger.info("Started {} runner threads", runners.size());

            for (Thread runner : runners) {
                runner.join();
            }
            logger.info("All {} runner threads completed", runners.size());
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }

}

