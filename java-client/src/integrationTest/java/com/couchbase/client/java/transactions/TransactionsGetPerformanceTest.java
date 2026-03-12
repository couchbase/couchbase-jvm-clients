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
//package com.couchbase.client.java.transactions;
//
//import com.couchbase.client.java.Bucket;
//import com.couchbase.client.java.Cluster;
//import com.couchbase.client.java.Collection;
//import com.couchbase.client.java.json.JsonObject;
//import com.couchbase.client.java.util.JavaIntegrationTest;
//import com.couchbase.client.test.ClusterType;
//import com.couchbase.client.test.IgnoreWhen;
//import org.junit.jupiter.api.AfterAll;
//import org.junit.jupiter.api.BeforeAll;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Test;
//import org.junit.jupiter.api.Assumptions;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import reactor.core.publisher.Flux;
//import reactor.core.scheduler.Schedulers;
//
//import java.lang.management.ManagementFactory;
//import java.lang.management.MemoryMXBean;
//import java.lang.management.ThreadInfo;
//import java.lang.management.ThreadMXBean;
//import java.time.Instant;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.Objects;
//import java.util.UUID;
//import java.util.concurrent.CopyOnWriteArrayList;
//import java.util.concurrent.ExecutionException;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//import java.util.concurrent.Future;
//import java.util.concurrent.ScheduledExecutorService;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.atomic.AtomicInteger;
//import java.util.function.Function;
//import java.util.function.Supplier;
//import java.util.stream.Collectors;
//import java.util.stream.IntStream;
//
//@IgnoreWhen(clusterTypes = {ClusterType.MOCKED}, isProtostellarWillWorkLater = true)
//class TransactionsGetPerformanceTest extends JavaIntegrationTest {
//  private static final Logger LOGGER = LoggerFactory.getLogger(TransactionsGetPerformanceTest.class);
//
//  private static final int WARMUP_SECONDS = 3;
//  private static final int BENCHMARK_SECONDS = 10;
//  private static final int CONCURRENT_TRANSACTIONS = 20;
//
//  private static Cluster cluster;
//  private static Collection collection;
//  private static List<String> docIds;
//
//  private
//  record BenchmarkStats(int iterations, int failures) {}
//
//  private record FullBenchmarkStats(
//    String name,
//    int iterations,
//    int failures,
//    long maxThreads,
//    double avgCpu,
//    double maxCpu,
//    long avgHeapMB,
//    long maxHeapMB,
//    int durationSeconds,
//    Map<String, Long> maxThreadCountsByPrefix
//  ) {}
//
//  private static class StatsCollector {
//    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(r -> {
//      var t = new Thread(r, "perf-stats-collector");
//      t.setDaemon(true);
//      return t;
//    });
//    private long maxThreads = 0;
//    private double maxCpu = 0;
//    private double totalCpu = 0;
//    private long maxHeapMB = 0;
//    private long totalHeapMB = 0;
//    private int samples = 0;
//    private final Map<String, Long> maxThreadCountsByPrefix = new HashMap<>();
//
//    private final ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
//    private final MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
//    private final com.sun.management.OperatingSystemMXBean osBean =
//      (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
//
//    public void start() {
//      osBean.getProcessCpuLoad();
//
//      executor.scheduleAtFixedRate(() -> {
//        try {
//          long threads = threadBean.getThreadCount();
//          double cpu = osBean.getProcessCpuLoad() * 100;
//          long heap = memoryBean.getHeapMemoryUsage().getUsed() / 1024 / 1024;
//
//          var threadNames = Arrays.stream(threadBean.getThreadInfo(threadBean.getAllThreadIds()))
//            .filter(Objects::nonNull)
//            .map(ThreadInfo::getThreadName)
//            .toList();
//
//          Map<String, Long> currentCounts = threadNames.stream()
//            .map(v -> {
//              var lastHyphen = v.lastIndexOf('-');
//              return lastHyphen != -1 ? v.substring(0, lastHyphen) : v;
//            })
//            .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
//
//          synchronized (this) {
//            maxThreads = Math.max(maxThreads, threads);
//            if (cpu >= 0) {
//              maxCpu = Math.max(maxCpu, cpu);
//              totalCpu += cpu;
//            }
//            maxHeapMB = Math.max(maxHeapMB, heap);
//            totalHeapMB += heap;
//            samples++;
//            currentCounts.forEach((prefix, count) ->
//              maxThreadCountsByPrefix.merge(prefix, count, Math::max));
//          }
//        } catch (Exception ignored) {
//        }
//      }, 0, 500, TimeUnit.MILLISECONDS);
//    }
//
//    public void stop() {
//      executor.shutdownNow();
//      try {
//        executor.awaitTermination(1, TimeUnit.SECONDS);
//      } catch (InterruptedException e) {
//        Thread.currentThread().interrupt();
//      }
//    }
//
//    public synchronized long maxThreads() { return maxThreads; }
//    public synchronized double avgCpu() { return samples == 0 ? 0 : totalCpu / samples; }
//    public synchronized double maxCpu() { return maxCpu; }
//    public synchronized long avgHeapMB() { return samples == 0 ? 0 : totalHeapMB / samples; }
//    public synchronized long maxHeapMB() { return maxHeapMB; }
//    public synchronized Map<String, Long> maxThreadCountsByPrefix() { return new HashMap<>(maxThreadCountsByPrefix); }
//  }
//
//  @BeforeAll
//  static void beforeAll() {
//    cluster = createCluster();
//    Bucket bucket = cluster.bucket(config().bucketname());
//    bucket.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);
//    collection = bucket.defaultCollection();
//    docIds = IntStream.range(0, 10)
//      .mapToObj(i -> "txn-perf-" + i + "-" + UUID.randomUUID())
//      .collect(Collectors.toList());
//    seedDocs();
//  }
//
//  @AfterAll
//  static void afterAll() {
//    cluster.disconnect();
//  }
//
//  @BeforeEach
//  void beforeEach() {
//    seedDocs();
//  }
//
//  @Test
//  void test() {
//    Assumptions.assumeTrue(virtualThreadsAvailable(), "Virtual threads not supported on this runtime");
//
//    LOGGER.info("Starting warmups ({}s each, concurrency={})", WARMUP_SECONDS, CONCURRENT_TRANSACTIONS);
////    runBlockingParallel("warmup-blocking-parallel", WARMUP_SECONDS);
////    runVirtualThreadGets("warmup-virtual-threads", WARMUP_SECONDS);
////    runBlockingSerial("warmup-blocking-serial", WARMUP_SECONDS);
////    runReactiveParallel("warmup-reactive-parallel", WARMUP_SECONDS);
////    runReactiveSerial("warmup-reactive-serial", WARMUP_SECONDS);
//
//    var results = new ArrayList<FullBenchmarkStats>();
////    results.add(monitor("Blocking parallel (system threads)", BENCHMARK_SECONDS, () -> runBlockingParallel("blocking-parallel", BENCHMARK_SECONDS)));
////    results.add(monitor("Blocking parallel (virtual threads)", BENCHMARK_SECONDS, () -> runVirtualThreadGets("virtual-threads", BENCHMARK_SECONDS)));
////    results.add(monitor("Blocking serial", BENCHMARK_SECONDS, () -> runBlockingSerial("blocking-serial", BENCHMARK_SECONDS)));
//    results.add(monitor("Reactive parallel", BENCHMARK_SECONDS, () -> runReactiveParallel("reactive-parallel", BENCHMARK_SECONDS)));
////    results.add(monitor("Reactive serial", BENCHMARK_SECONDS, () -> runReactiveSerial("reactive-serial", BENCHMARK_SECONDS)));
//
//    printResults(results);
//  }
//
//  private FullBenchmarkStats monitor(String name, int seconds, Supplier<BenchmarkStats> benchmark) {
//    LOGGER.info("Starting test {}", name);
//    var collector = new StatsCollector();
//    collector.start();
//    try {
//      var stats = benchmark.get();
//      return new FullBenchmarkStats(
//        name,
//        stats.iterations(),
//        stats.failures(),
//        collector.maxThreads(),
//        collector.avgCpu(),
//        collector.maxCpu(),
//        collector.avgHeapMB(),
//        collector.maxHeapMB(),
//        seconds,
//        collector.maxThreadCountsByPrefix()
//      );
//    } finally {
//      collector.stop();
//    }
//  }
//
//  private void printResults(List<FullBenchmarkStats> results) {
//    LOGGER.info("=== Throughput and Resource Usage Results ===");
//    var header = String.format("%-40s | %10s | %10s | %11s | %10s | %10s | %10s | %10s",
//      "Test", "Ops/sec", "Failures", "Max Threads", "Avg CPU %", "Max CPU %", "Avg HeapMB", "Max HeapMB");
//    LOGGER.info(header);
//    LOGGER.info("-".repeat(header.length()));
//
//    for (var res : results) {
//      double opsPerSec = (double) res.iterations() / res.durationSeconds();
//      LOGGER.info(String.format("%-40s | %10.2f | %10d | %11d | %10.2f | %10.2f | %10d | %10d",
//        res.name(), opsPerSec, res.failures(), res.maxThreads(), res.avgCpu(), res.maxCpu(), res.avgHeapMB(), res.maxHeapMB()));
//
//      // Print thread prefix counts if they are relevant to the test
//      res.maxThreadCountsByPrefix().entrySet().stream()
//        .filter(e -> e.getKey().contains("perf-app") || e.getKey().contains("cb-") || e.getKey().contains("parallel"))
//        .sorted(Map.Entry.comparingByKey())
//        .forEach(e -> LOGGER.info("  - Thread Prefix: {} (Max Count: {})", e.getKey(), e.getValue()));
//    }
//  }
//
//  private BenchmarkStats runBlockingParallel(String testName, int seconds) {
//    var iterations = new AtomicInteger(0);
//    var failures = new AtomicInteger(0);
//    var threadCounter = new AtomicInteger();
//    var exec = Executors.newFixedThreadPool(CONCURRENT_TRANSACTIONS * (docIds.size() + 1), r -> {
//      var t = new Thread(r, "perf-app-" + testName + "-" + threadCounter.getAndIncrement());
//      t.setDaemon(true);
//      return t;
//    });
//    var end = Instant.now().plusSeconds(seconds);
//
//    try {
//      var futures = new ArrayList<Future<?>>();
//      for (int i = 0; i < CONCURRENT_TRANSACTIONS; i++) {
//        futures.add(exec.submit(() -> {
//          while (Instant.now().isBefore(end)) {
//            try {
//              cluster.transactions().run(ctx -> {
//                var gets = docIds.stream()
//                  .map(id -> exec.submit(() -> ctx.get(collection, id)))
//                  .collect(Collectors.toList());
//                for (var f : gets) {
//                  getFuture(f);
//                }
//                iterations.addAndGet(docIds.size());
//              });
//            } catch (Exception e) {
//              failures.incrementAndGet();
//            }
//          }
//        }));
//      }
//      for (var f : futures) {
//        try {
//          f.get(seconds + 5, TimeUnit.SECONDS);
//        } catch (Exception ignored) {}
//      }
//    } finally {
//      exec.shutdownNow();
//    }
//    return new BenchmarkStats(iterations.get(), failures.get());
//  }
//
//  private BenchmarkStats runBlockingSerial(String testName, int seconds) {
//    var iterations = new AtomicInteger(0);
//    var failures = new AtomicInteger(0);
//    var threadCounter = new AtomicInteger();
//    var exec = Executors.newFixedThreadPool(CONCURRENT_TRANSACTIONS, r -> {
//      var t = new Thread(r, "perf-app-" + testName + "-" + threadCounter.getAndIncrement());
//      t.setDaemon(true);
//      return t;
//    });
//    var end = Instant.now().plusSeconds(seconds);
//
//    try {
//      var futures = new ArrayList<Future<?>>();
//      for (int i = 0; i < CONCURRENT_TRANSACTIONS; i++) {
//        futures.add(exec.submit(() -> {
//          while (Instant.now().isBefore(end)) {
//            try {
//              cluster.transactions().run(ctx -> {
//                for (var id : docIds) {
//                  ctx.get(collection, id);
//                }
//                iterations.addAndGet(docIds.size());
//              });
//            } catch (Exception e) {
//              failures.incrementAndGet();
//            }
//          }
//        }));
//      }
//      for (var f : futures) {
//        try {
//          f.get(seconds + 5, TimeUnit.SECONDS);
//        } catch (Exception ignored) {}
//      }
//    } finally {
//      exec.shutdownNow();
//    }
//    return new BenchmarkStats(iterations.get(), failures.get());
//  }
//
//  private BenchmarkStats runReactiveParallel(String testName, int seconds) {
//    var iterations = new AtomicInteger(0);
//    var failures = new AtomicInteger(0);
//    var end = Instant.now().plusSeconds(seconds);
//
//    Flux.range(0, CONCURRENT_TRANSACTIONS)
//      .flatMap(i -> Flux.defer(() -> {
//          if (Instant.now().isAfter(end)) return Flux.empty();
//          return cluster.reactive().transactions().run(ctx -> Flux.fromIterable(docIds)
//              .flatMap(id -> ctx.get(collection.reactive(), id))
//              .collectList())
//            .doOnSuccess(v -> iterations.addAndGet(docIds.size()))
//            .doOnError(e -> failures.incrementAndGet())
//            .onErrorResume(e -> reactor.core.publisher.Mono.empty())
//            .repeat(() -> Instant.now().isBefore(end));
//        }).subscribeOn(Schedulers.parallel()), CONCURRENT_TRANSACTIONS)
//      .blockLast();
//
//    return new BenchmarkStats(iterations.get(), failures.get());
//  }
//
//  private BenchmarkStats runReactiveSerial(String testName, int seconds) {
//    var iterations = new AtomicInteger(0);
//    var failures = new AtomicInteger(0);
//    var end = Instant.now().plusSeconds(seconds);
//
//    Flux.range(0, CONCURRENT_TRANSACTIONS)
//      .flatMap(i -> Flux.defer(() -> {
//          if (Instant.now().isAfter(end)) return Flux.empty();
//          return cluster.reactive().transactions().run(ctx -> Flux.fromIterable(docIds)
//              .concatMap(id -> ctx.get(collection.reactive(), id))
//              .collectList())
//            .doOnSuccess(v -> iterations.addAndGet(docIds.size()))
//            .doOnError(e -> failures.incrementAndGet())
//            .onErrorResume(e -> reactor.core.publisher.Mono.empty())
//            .repeat(() -> Instant.now().isBefore(end));
//        }).subscribeOn(Schedulers.parallel()), CONCURRENT_TRANSACTIONS)
//      .blockLast();
//
//    return new BenchmarkStats(iterations.get(), failures.get());
//  }
//
//  private BenchmarkStats runVirtualThreadGets(String testName, int seconds) {
//    var iterations = new AtomicInteger(0);
//    var failures = new AtomicInteger(0);
//    var threadCounter = new AtomicInteger();
//    var end = Instant.now().plusSeconds(seconds);
//
//    var mainThreads = new ArrayList<Thread>();
//    for (int i = 0; i < CONCURRENT_TRANSACTIONS; i++) {
//      mainThreads.add(startVirtualThread("perf-app-" + testName + "-main-" + i, () -> {
//        while (Instant.now().isBefore(end)) {
//          try {
//            cluster.transactions().run(ctx -> {
//              var gets = new ArrayList<Thread>();
//              var errors = new CopyOnWriteArrayList<RuntimeException>();
//              for (var id : docIds) {
//                gets.add(startVirtualThread("perf-app-" + testName + "-get-" + threadCounter.getAndIncrement(), () -> {
//                  try {
//                    ctx.get(collection, id);
//                  } catch (RuntimeException err) {
//                    errors.add(err);
//                  }
//                }));
//              }
//              for (var t : gets) {
//                try {
//                  t.join();
//                } catch (InterruptedException e) {
//                  Thread.currentThread().interrupt();
//                }
//              }
//              if (!errors.isEmpty()) throw errors.get(0);
//              iterations.addAndGet(docIds.size());
//            });
//          } catch (Exception e) {
//            failures.incrementAndGet();
//          }
//        }
//      }));
//    }
//
//    for (var t : mainThreads) {
//      try {
//        t.join();
//      } catch (InterruptedException e) {
//        Thread.currentThread().interrupt();
//      }
//    }
//
//    return new BenchmarkStats(iterations.get(), failures.get());
//  }
//
//  private static boolean virtualThreadsAvailable() {
//    return true;
//  }
//
//  private static Thread startVirtualThread(String name, Runnable runnable) {
//    return Thread.ofVirtual().name(name).start(runnable);
//  }
//
//  private static TransactionGetResult getFuture(Future<TransactionGetResult> future) {
//    try {
//      return future.get();
//    } catch (InterruptedException e) {
//      Thread.currentThread().interrupt();
//      throw new RuntimeException(e);
//    } catch (ExecutionException e) {
//      throw new RuntimeException(e.getCause());
//    }
//  }
//
//  private static void seedDocs() {
//    for (String id : docIds) {
//      collection.upsert(id, JsonObject.create().put("value", id));
//    }
//  }
//}
