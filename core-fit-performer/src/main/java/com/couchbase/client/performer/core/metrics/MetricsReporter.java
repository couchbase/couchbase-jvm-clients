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
package com.couchbase.client.performer.core.metrics;

import com.couchbase.client.performer.core.perf.WorkloadStreamingThread;
import com.couchbase.client.performer.core.util.TimeUtil;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Periodically sends metrics back to the driver
 */
public class MetricsReporter extends Thread {
    private final WorkloadStreamingThread writer;
    private final Logger logger = LoggerFactory.getLogger(MetricsReporter.class);
    private static final double CONVERT_BYTES_TO_MB = 1000000;

    public MetricsReporter(WorkloadStreamingThread writer) {
        this.writer = writer;
    }

    @Override
    public void run() {
        logger.info("Metrics thread started");
        boolean done = false;
        var lastThreadDumpTimeNanos = System.nanoTime();
        var gson = new Gson();

        while (!isInterrupted() && !done) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                done = true;
            }

            if (!done) {
                Map<String, Object> metrics = new HashMap<>();

                try {
                    var memory = ManagementFactory.getMemoryMXBean();
                    metrics.put("memHeapUsedMB", (float) memory.getHeapMemoryUsage().getUsed() / CONVERT_BYTES_TO_MB);
                    metrics.put("memHeapMaxMB", (float) memory.getHeapMemoryUsage().getMax() / CONVERT_BYTES_TO_MB);
                } catch (Throwable err) {
                    logger.warn("Memory metrics failed: {}", err.toString());
                }

                try {
                    var threadMXBean = ManagementFactory.getThreadMXBean();

                    metrics.put("threadCount", threadMXBean.getAllThreadIds().length);
                } catch (Throwable err) {
                    logger.warn("Metrics failed: {}", err.toString());
                }

                try {
                    var beans = ManagementFactory.getGarbageCollectorMXBeans();
                    // It seems that though multiple GC can be reported, only the first appears to used
                    for (int i = 0; i < 1; i++) {
                        var bean = beans.get(i);

                        metrics.put("gc" + i + "AccTimeMs", bean.getCollectionTime());
                        metrics.put("gc" + i + "Count", bean.getCollectionCount());
                    }
                } catch (Throwable err) {
                    logger.warn("Metrics failed: {}", err.toString());
                }

                try {
                    var bean = ManagementFactory.getPlatformMXBean(com.sun.management.OperatingSystemMXBean.class);
                    metrics.put("processCpu", bean.getProcessCpuLoad() * 100.0);
                    metrics.put("systemCpu", bean.getSystemCpuLoad() * 100.0);
                    metrics.put("freeSwapSizeMB", bean.getFreeSwapSpaceSize() / CONVERT_BYTES_TO_MB);
                } catch (Throwable err) {
                    logger.warn("Metrics failed: {}", err.toString());
                }

                try {
                    var pools = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);
                    for (BufferPoolMXBean pool : pools) {
                        if (pool.getName().equals("direct")) {
                            metrics.put("memDirectUsedMB", pool.getMemoryUsed() / CONVERT_BYTES_TO_MB);
                            metrics.put("memDirectMaxMB", pool.getTotalCapacity() / CONVERT_BYTES_TO_MB);
                        }
                    }
                } catch (Throwable err) {
                    logger.warn("Metrics failed: {}", err.toString());
                }

                writer.enqueue(com.couchbase.client.protocol.run.Result.newBuilder()
                        .setMetrics(com.couchbase.client.protocol.metrics.Result.newBuilder()
                                .setInitiated(TimeUtil.getTimeNow())
                                .setMetrics(gson.toJson(metrics)))
                        .build());

                if (TimeUnit.NANOSECONDS.toMinutes(System.nanoTime() - lastThreadDumpTimeNanos) >= 1) {
                    lastThreadDumpTimeNanos = System.nanoTime();
                    dumpThreads();
                }
            }
        }

        logger.info("Metrics thread finished");
    }

    // CBD-5008: try to get some insight into why there's so many threads
    private void dumpThreads() {
        var allTheads = Thread.getAllStackTraces();
        logger.info("Dumping all {} threads:", allTheads.size());
        allTheads.forEach((k, v) -> {
            logger.info("   Thread {} {} {}", k.getId(), k.getName(), v.length == 0 ? "-" : v[0].toString());
        });
    }
}
