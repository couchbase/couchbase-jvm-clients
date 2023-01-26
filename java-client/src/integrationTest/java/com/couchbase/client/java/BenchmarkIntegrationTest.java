/*
 * Copyright (c) 2023 Couchbase, Inc.
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

package com.couchbase.client.java;

import com.couchbase.client.core.deps.org.HdrHistogram.Histogram;
import com.couchbase.client.core.deps.org.LatencyUtils.LatencyStats;
import com.couchbase.client.core.endpoint.ProtostellarEndpoint;
import com.couchbase.client.core.env.CouchbaseForkPoolThreadFactory;
import com.couchbase.client.core.error.TimeoutException;
import com.couchbase.client.core.protostellar.ProtostellarStatsCollector;
import com.couchbase.client.java.util.JavaIntegrationTest;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Handler;
import java.util.logging.LogManager;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


class GrpcLogInterceptor extends Handler {
  private final Pattern CHANNEL_CREATED = Pattern.compile("\\[Channel<(\\d*)>:.*? created");
  private final Pattern SUBCHANNEL_CREATED = Pattern.compile("\\[Channel<(\\d*)>:.*? Child Subchannel started");
  private final ProtostellarStatsCollector collector;

  public GrpcLogInterceptor(ProtostellarStatsCollector collector) {
    this.collector = collector;
  }

  @Override
  public void publish(LogRecord record) {
    Matcher matcher = CHANNEL_CREATED.matcher(record.getMessage());

    if (matcher.find()) {
      collector.channelAdded();
    }
    else {
      matcher = SUBCHANNEL_CREATED.matcher(record.getMessage());
      if (matcher.find()) {
        collector.subChannelAdded();
      }
    }
  }

  @Override
  public void flush() {
  }

  @Override
  public void close() throws SecurityException {
  }
}

/**
 * A very basic benchmarking test.
 * todo snremove remove this once we're done
 */
@Disabled // Disabled as this is for manual testing only
class BenchmarkIntegrationTest extends JavaIntegrationTest {

  @Test
  void testConcurrentOps() {
    int opsTotalBase = 1000;
    int runForSecs = 10;

    // Just for warmup, throw away
    run(1, opsTotalBase, runForSecs);

    run(1, opsTotalBase, runForSecs);
    run(2, opsTotalBase, runForSecs);
    run(3, opsTotalBase, runForSecs);
    run(5, opsTotalBase, runForSecs);
    run(10, opsTotalBase, runForSecs);
    run(20, opsTotalBase, runForSecs);
    run(50, opsTotalBase, runForSecs);
    run(100, opsTotalBase, runForSecs);
    run(200, opsTotalBase, runForSecs);
  }

  @Test
  void testDifferentSettings() throws IOException {
    int opsTotalBase = 1000;
    int runForSecs = 3;
    int opsDesiredInFlight = 200;

    // Just for warmup, throw away
//    run(0, opsDesiredInFlight, opsTotalBase, runForSecs, () -> {
//      System.out.println("JVM warmup");
//    });

    List<StatsRow> rows = new ArrayList<>();

//    {
//      String property = "com.couchbase.protostellar.numEndpoints";
//      for (int i = 1; i < 16; i *= 2) {
//        final int x = i;
//        System.setProperty(property, Integer.toString(i));
//        rows.add(run(i, opsDesiredInFlight, opsTotalBase, runForSecs, () -> {
//          System.out.println(property + "=" + x);
//        }));
//      }
//      System.clearProperty(property);
//
//      displayTable("numEndpoints", rows);
//    }

    {
      String property = "com.couchbase.protostellar.loadBalancing";
      System.setProperty("com.couchbase.protostellar.loadBalancingSingle", "false");
      for (int i = 1; i < 8; i *= 2) {
        final int x = i;
        System.setProperty(property, Integer.toString(i));
        rows.add(run(i, opsDesiredInFlight, opsTotalBase, runForSecs, () -> {
          System.out.println(property + "=" + x);
        }));
      }

      System.clearProperty(property);

      displayTable("loadBalancing", rows);
    }
  }

  private static void displayTable(String stat, List<StatsRow> rows) throws IOException {
    File file = new File(stat + ".html");
    FileWriter output = new FileWriter(file);

    StringBuilder out = new StringBuilder();
    out.append("<html><body>");
    System.getProperties().forEach((k, v) -> {
      if (k.toString().startsWith("com.couchbase.protostellar")) {
        out.append("<p>").append(k).append('=').append(v).append("</p>");
      }
    });
    out.append("<table><thead>");
    out.append("<th>").append(stat).append("</th>");
    out.append("<th>Min µs</th>");
    out.append("<th>Max µs</th>");
    out.append("<th>Mean µs</th>");
    out.append("<th>p99 µs</th>");
    out.append("<th>Max in-flight</th>");
    out.append("<th>Ops/sec</th>");
    out.append("<th>Ops sent</th>");
    out.append("<th>Run for</th>");
    out.append("<th>Channels</th>");
    out.append("<th>Subchannels</th>");
    out.append("<th>Outgoing</th>");
    out.append("<th>Incoming</th>");
    out.append("<th>Threads</th>");
    out.append("<th>Timeouts</th>");
    out.append("<th>Other errors</th>");
    out.append("</thead>");

    rows.forEach(row -> row.outputHtml(out));
    
    out.append("</table></body></html>");

    System.out.println("Writing results to " + file.getAbsolutePath());

    output.write(out.toString());
    output.close();
  }

  private StatsRow run(int opsDesiredInFlight, int opsTotalBase, int runForSecs) {
    return run(opsDesiredInFlight, opsDesiredInFlight, opsTotalBase, runForSecs, () -> {
      System.out.println("Ran with " + opsDesiredInFlight + " parallel");
    });
  }

  private StatsRow run(int stat, int opsDesiredInFlight, int opsTotalBase, int runForSecs, Runnable beforeResults) {
    ProtostellarStatsCollector collector = new ProtostellarStatsCollector();
    ProtostellarEndpoint.collector = collector;
    CouchbaseForkPoolThreadFactory.collector = collector;

    // LogManager.getLogManager().reset();
    Logger rootLogger = LogManager.getLogManager().getLogger("");
    rootLogger.addHandler(new GrpcLogInterceptor(collector));

    try (Cluster ps = Cluster.connect("protostellar://localhost", "Administrator", "password")) {
      Collection collection = ps.bucket(config().bucketname()).defaultCollection();

      AtomicInteger opsActuallyInFlight = new AtomicInteger();
      Set<Integer> concurrentlyOutgoingMessagesSeen = new ConcurrentSkipListSet<>();
      LatencyStats stats = new LatencyStats();
      AtomicInteger errorCount = new AtomicInteger();
      AtomicInteger errorCountTimeouts = new AtomicInteger();

      AtomicInteger opsSent = new AtomicInteger();

      long realStart = System.nanoTime();

      Flux.generate(() -> null,
          (state, sink) -> {
            String next = UUID.randomUUID().toString();
            sink.next(next);
            return null;
          })
        .takeWhile(v -> TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - realStart) <= runForSecs)
        .parallel(opsDesiredInFlight)
        // Defaults to 10*numProcessors threads
        .runOn(Schedulers.boundedElastic())
        .concatMap(id -> {
          concurrentlyOutgoingMessagesSeen.add(opsActuallyInFlight.incrementAndGet());

          long start = System.nanoTime();
          return collection.reactive().insert(id.toString(), "Hello, world")
            .onErrorResume(err -> {
              opsActuallyInFlight.decrementAndGet();
              if (err instanceof TimeoutException) {
                errorCountTimeouts.incrementAndGet();
              } else {
                errorCount.incrementAndGet();
              }
              return Mono.empty();
            })
            .doOnNext(v -> {
              opsActuallyInFlight.decrementAndGet();
              stats.recordLatency(System.nanoTime() - start);
              opsSent.incrementAndGet();
            });
        })
        .sequential()
        .blockLast();

//    System.out.println("Ran " + opsTotal + " with " + opsDesiredInFlight + " parallel");
      Histogram histogram = stats.getIntervalHistogram();
      beforeResults.run();
//    System.out.println("   Min: " + TimeUnit.NANOSECONDS.toMicros(histogram.getMinValue()));
//    System.out.println("   Max: " + TimeUnit.NANOSECONDS.toMicros(histogram.getMaxValue()));
      System.out.println("   Mean:                  " + TimeUnit.NANOSECONDS.toMicros((long) histogram.getMean()) + "µs");
//    System.out.println("   p50: " + TimeUnit.NANOSECONDS.toMicros(histogram.getValueAtPercentile(50)));
//    System.out.println("   p95: " + TimeUnit.NANOSECONDS.toMicros(histogram.getValueAtPercentile(95)));
//    System.out.println("   p99: " + TimeUnit.NANOSECONDS.toMicros(histogram.getValueAtPercentile(99)));
      System.out.println("   Max in-flight:         " + concurrentlyOutgoingMessagesSeen.stream().max(Comparator.comparingInt(v -> v)).get());
      System.out.println("   Ops/sec:               " + (int) ((double) opsSent.get() / runForSecs));
      System.out.println("   Ops sent:              " + opsSent.get());
      System.out.println("   Run for:               " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - realStart) + "ms");
      System.out.println("   Channels created:      " + collector.channelsCreated());
      System.out.println("   Subchannels created:   " + collector.subChannelsCreated());
      System.out.println("   Concurrent outgoing:   " + collector.inflightOutgoingMessagesMax());
      System.out.println("   Concurrent incoming:   " + collector.inflightIncomingMessagesMax());
      System.out.println("   Threads:               " + collector.maxThreadCount());
      if (errorCountTimeouts.get() != 0) {
        System.out.println("   Timeouts:              " + errorCountTimeouts.get());
      }
      if (errorCount.get() != 0) {
        System.out.println("   Other errors:          " + errorCount.get());
      }

      return new StatsRow(stat,
        collector,
        histogram,
        opsSent.get(),
        runForSecs,
        concurrentlyOutgoingMessagesSeen.stream().max(Comparator.comparingInt(v -> v)).get(),
        errorCountTimeouts.get(),
        errorCount.get());
    }
  }
}

class StatsRow {
  private final int stat;
  private final ProtostellarStatsCollector collector;
  private final Histogram histogram;
  private final int opsSent;
  private final int runForSecs;
  private final int maxInFlight;
  private final int errorCountTimeouts;
  private final int otherErrors;
  private final int channelsCreated;
  private final int subChannelsCreated;
  private final int inflightOutgoingMessagesMax;
  private final int inflightIncomingMessagesMax;
  private final int maxThreadCount;

  public StatsRow(int stat,
                  ProtostellarStatsCollector collector,
                  Histogram histogram,
                  int opsSent,
                  int runForSecs,
                  int maxInFlight,
                  int errorCountTimeouts,
                  int otherErrors) {
    this.stat = stat;
    this.collector = collector;
    this.histogram = histogram;
    this.opsSent = opsSent;
    this.runForSecs = runForSecs;
    this.maxInFlight = maxInFlight;
    this.errorCountTimeouts = errorCountTimeouts;
    this.otherErrors = otherErrors;
    this.channelsCreated = collector.channelsCreated();
    this.subChannelsCreated = collector.subChannelsCreated();
    this.inflightOutgoingMessagesMax = collector.inflightOutgoingMessagesMax();
    this.inflightIncomingMessagesMax = collector.inflightIncomingMessagesMax();
    this.maxThreadCount = collector.maxThreadCount();
  }

  public void outputHtml(StringBuilder sb) {
    sb.append("<tr>");
    sb.append(" <td>").append(stat).append("</td>");
    sb.append(" <td>").append(TimeUnit.NANOSECONDS.toMicros(histogram.getMinValue())).append("</td>");
    sb.append(" <td>").append(TimeUnit.NANOSECONDS.toMicros(histogram.getMaxValue())).append("</td>");
    sb.append(" <td>").append(TimeUnit.NANOSECONDS.toMicros((long) histogram.getMean())).append("</td>");
    sb.append(" <td>").append(TimeUnit.NANOSECONDS.toMicros((long) histogram.getValueAtPercentile(99))).append("</td>");
    sb.append(" <td>").append(maxInFlight).append("</td>");
    sb.append(" <td>").append((int) ((double) opsSent / runForSecs)).append("</td>");
    sb.append(" <td>").append(opsSent).append("</td>");
    sb.append(" <td>").append(runForSecs).append("</td>");
    sb.append(" <td>").append(channelsCreated).append("</td>");
    sb.append(" <td>").append(subChannelsCreated).append("</td>");
    sb.append(" <td>").append(inflightOutgoingMessagesMax).append("</td>");
    sb.append(" <td>").append(inflightIncomingMessagesMax).append("</td>");
    sb.append(" <td>").append(maxThreadCount).append("</td>");
    sb.append(" <td>").append(errorCountTimeouts).append("</td>");
    sb.append(" <td>").append(otherErrors).append("</td>");
    sb.append("</tr>");
  }
}
