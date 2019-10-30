/*
 * Copyright (c) 2019 Couchbase, Inc.
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

package com.couchbase.client.core.cnc.tracing;

import com.couchbase.client.core.cnc.EventBus;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.InternalSpan;
import com.couchbase.client.core.cnc.RequestTracer;
import com.couchbase.client.core.cnc.events.tracing.OverThresholdRequestsRecordedEvent;
import com.couchbase.client.core.deps.org.jctools.queues.MpscUnboundedArrayQueue;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.util.HostAndPort;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.couchbase.client.core.logging.RedactableArgument.redactSystem;

/**
 * The default tracing implementation, which tracks the top N slowest requests per service and dumps them at
 * configurable intervals.
 */
public class ThresholdRequestTracer implements RequestTracer {

  private static final AtomicInteger REQUEST_TRACER_ID = new AtomicInteger();
  private static final String KEY_TOTAL_MICROS = "total_us";
  private static final String KEY_DISPATCH_MICROS = "last_dispatch_us";
  private static final String KEY_ENCODE_MICROS = "encode_us";
  private static final String KEY_SERVER_MICROS = "server_us";

  private final AtomicBoolean running = new AtomicBoolean(false);
  private final Queue<Request<?>> overThresholdQueue;
  private final EventBus eventBus;
  private final Thread worker;

  private final long kvThreshold;
  private final long queryThreshold;
  private final long viewThreshold;
  private final long searchThreshold;
  private final long analyticsThreshold;
  private final long emitIntervalNanos;
  private final int sampleSize;

  /**
   * Creates a builder to customize this tracer.
   *
   * @param eventBus the event bus where the final events will be emitted into.
   * @return the builder to customize.
   */
  public static Builder builder(final EventBus eventBus) {
    return new Builder(eventBus);
  }

  /**
   * Short-hand to create the tracer with the event bus that needs to be used.
   *
   * @param eventBus the event bus where the final events will be emitted into.
   * @return the created tracer ready to be used.
   */
  public static ThresholdRequestTracer create(final EventBus eventBus) {
    return builder(eventBus).build();
  }

  /**
   * Internal constructor to build the tracer based on the config provided.
   *
   * @param builder the builder which contains the config for the tracer.
   */
  private ThresholdRequestTracer(final Builder builder) {
    this.eventBus = builder.eventBus;
    this.overThresholdQueue = new MpscUnboundedArrayQueue<>(builder.queueLength);
    kvThreshold = builder.kvThreshold.toNanos();
    analyticsThreshold = builder.analyticsThreshold.toNanos();
    searchThreshold = builder.searchThreshold.toNanos();
    viewThreshold = builder.viewThreshold.toNanos();
    queryThreshold = builder.queryThreshold.toNanos();
    sampleSize = builder.sampleSize;
    emitIntervalNanos = builder.emitInterval.toNanos();

    worker = new Thread(new Worker());
    worker.setDaemon(true);
  }

  @Override
  public InternalSpan span(final String operationName, final RequestSpan parent) {
    return new ThresholdInternalSpan(this, operationName, parent);
  }

  /**
   * Finishes the span (sends it off into the queue when over threshold).
   *
   * @param span the finished internal span from the toplevel request.
   */
  void finish(final ThresholdInternalSpan span) {
    final Request<?> request = span.requestContext().request();
    if (isOverThreshold(request)) {
      if (!overThresholdQueue.offer(request)) {
        // TODO: what to do if dropped because queue full? raise event?
      }
    }
  }

  /**
   * Helper method to calculate if the given request is over the configured threshold for this service.
   *
   * @param request the request to check.
   * @return true if over threshold, false otherwise.
   */
  private boolean isOverThreshold(final Request<?> request) {
    final long tookNanos = request.context().logicalRequestLatency();
    final ServiceType serviceType = request.serviceType();
    if (serviceType == ServiceType.KV && tookNanos >= kvThreshold) {
      return true;
    } else if (serviceType == ServiceType.QUERY && tookNanos >= queryThreshold) {
      return true;
    } else if (serviceType == ServiceType.ANALYTICS && tookNanos >= analyticsThreshold) {
      return true;
    } else if (serviceType == ServiceType.SEARCH && tookNanos >= searchThreshold) {
      return true;
    } else {
      return serviceType == ServiceType.VIEWS && tookNanos >= viewThreshold;
    }
  }

  @Override
  public Mono<Void> start() {
    return Mono.defer(() -> {
      if (running.compareAndSet(false, true)) {
        worker.start();
      }
      return Mono.empty();
    });
  }

  @Override
  public Mono<Void> stop(Duration timeout) {
    return Mono.defer(() -> {
      if (running.compareAndSet(true, false)) {
        worker.interrupt();
      }
      return Mono.empty();
    });
  }

  /**
   * The worker picks up requests from the queue and stores it in the per-service queues so that they can be dumped
   * when configured.
   */
  private class Worker implements Runnable {

    /**
     * Time this worker spends between check cycles. 100ms should be granular enough
     * but making it configurable, who knows...
     */
    private final long workerSleepMs = Long.parseLong(
      System.getProperty("com.couchbase.thresholdRequestTracerSleep", "100")
    );

    /**
     * Compares request by their logical request latency for the priority threshold queues.
     */
    private final Comparator<Request<?>> THRESHOLD_COMPARATOR = Comparator.comparingLong(
      o -> o.context().logicalRequestLatency()
    );

    private final Queue<Request<?>> kvThresholds = new PriorityQueue<>(THRESHOLD_COMPARATOR);
    private final Queue<Request<?>> n1qlThresholds = new PriorityQueue<>(THRESHOLD_COMPARATOR);
    private final Queue<Request<?>> viewThresholds = new PriorityQueue<>(THRESHOLD_COMPARATOR);
    private final Queue<Request<?>> ftsThresholds = new PriorityQueue<>(THRESHOLD_COMPARATOR);
    private final Queue<Request<?>> analyticsThresholds = new PriorityQueue<>(THRESHOLD_COMPARATOR);

    private long kvThresholdCount = 0;
    private long n1qlThresholdCount = 0;
    private long viewThresoldCount = 0;
    private long ftsThresholdCount = 0;
    private long analyticsThresholdCount = 0;

    private long lastThresholdLog;
    private boolean hasThresholdWritten;

    @Override
    public void run() {
      Thread.currentThread().setName("cb-tracing-" + REQUEST_TRACER_ID.incrementAndGet());

      while (running.get()) {
        try {
          handleOverThresholdQueue();
          Thread.sleep(workerSleepMs);
        } catch (final InterruptedException ex) {
          if (!running.get()) {
            return;
          } else {
            Thread.currentThread().interrupt();
          }
        } catch (final Exception ex) {
          // TODO: raise event
          // LOGGER.warn("Got exception on slow operation reporter, ignoring.", ex);
        }
      }
    }

    /**
     * Helper method which drains the queue, handles the lists and logs if needed.
     */
    private void handleOverThresholdQueue() {
      long now = System.nanoTime();
      if (now > (lastThresholdLog + emitIntervalNanos)) {
        prepareAndlogOverThreshold();
        lastThresholdLog = now;
      }

      while (true) {
        Request<?> request = overThresholdQueue.poll();
        if (request == null) {
          return;
        }
        final ServiceType serviceType = request.serviceType();
        if (serviceType == ServiceType.KV) {
          updateThreshold(kvThresholds, request);
          kvThresholdCount += 1;
        } else if (serviceType == ServiceType.QUERY) {
          updateThreshold(n1qlThresholds, request);
          n1qlThresholdCount += 1;
        } else if (serviceType == ServiceType.VIEWS) {
          updateThreshold(viewThresholds, request);
          viewThresoldCount += 1;
        } else if (serviceType == ServiceType.SEARCH) {
          updateThreshold(ftsThresholds, request);
          ftsThresholdCount += 1;
        } else if (serviceType == ServiceType.ANALYTICS) {
          updateThreshold(analyticsThresholds, request);
          analyticsThresholdCount += 1;
        } else {
          // TODO: log error
          // LOGGER.warn("Unknown service in span {}", service);
        }
      }
    }

    /**
     * Logs the over threshold data and resets the sets.
     */
    private void prepareAndlogOverThreshold() {
      if (!hasThresholdWritten) {
        return;
      }
      hasThresholdWritten = false;

      List<Map<String, Object>> output = new ArrayList<>();
      if (!kvThresholds.isEmpty()) {
        output.add(convertThresholdMetadata(kvThresholds, kvThresholdCount, SERVICE_IDENTIFIER_KV));
        kvThresholds.clear();
        kvThresholdCount = 0;
      }
      if (!n1qlThresholds.isEmpty()) {
        output.add(convertThresholdMetadata(n1qlThresholds, n1qlThresholdCount, SERVICE_IDENTIFIER_QUERY));
        n1qlThresholds.clear();
        n1qlThresholdCount = 0;
      }
      if (!viewThresholds.isEmpty()) {
        output.add(convertThresholdMetadata(viewThresholds, viewThresoldCount, SERVICE_IDENTIFIER_VIEW));
        viewThresholds.clear();
        viewThresoldCount = 0;
      }
      if (!ftsThresholds.isEmpty()) {
        output.add(convertThresholdMetadata(ftsThresholds, ftsThresholdCount, SERVICE_IDENTIFIER_SEARCH));
        ftsThresholds.clear();
        ftsThresholdCount = 0;
      }
      if (!analyticsThresholds.isEmpty()) {
        output.add(convertThresholdMetadata(analyticsThresholds, analyticsThresholdCount, SERVICE_IDENTIFIER_ANALYTICS));
        analyticsThresholds.clear();
        analyticsThresholdCount = 0;
      }
      logOverThreshold(output);
    }

    /**
     * Converts the metadata of the requests into the format that is suitable for dumping.
     *
     * @param requests the request data to convert
     * @param count the total count
     * @param ident the identifier to use
     * @return the converted map
     */
    private Map<String, Object> convertThresholdMetadata(final Queue<Request<?>> requests, final long count,
                                                         final String ident) {
      Map<String, Object> output = new HashMap<>();
      List<Map<String, Object>> top = new ArrayList<>();
      for (Request<?> request : requests) {
        Map<String, Object> entry = new HashMap<>();
        entry.put(KEY_TOTAL_MICROS, TimeUnit.NANOSECONDS.toMicros(request.context().logicalRequestLatency()));

        String operationId = request.operationId();
        if (operationId != null) {
          entry.put("last_operation_id", operationId);
        }

        // todo: does this need to be improved?
        entry.put("operation_name", request.getClass().getSimpleName());

        HostAndPort local = request.context().lastDispatchedFrom();
        HostAndPort peer = request.context().lastDispatchedTo();
        if (local != null) {
          entry.put("last_local_address", redactSystem(local).toString());
        }
        if (peer != null) {
          entry.put("last_remote_address", redactSystem(peer).toString());
        }

        // TODO
        /*
        String localId = span.request().lastLocalId();
        if (localId != null) {
          entry.put("last_local_id", redactSystem(localId).toString());
        }
         */

        long encode_duration = request.context().encodeLatency();
        if (encode_duration > 0) {
          entry.put(KEY_ENCODE_MICROS, encode_duration);
        }

        long dispatch_duration = request.context().dispatchLatency();
        if (dispatch_duration > 0) {
          entry.put(KEY_DISPATCH_MICROS, TimeUnit.NANOSECONDS.toMicros(dispatch_duration));
        }

        // TODO
        /*
        String server_duration = span.getBaggageItem(KEY_SERVER_MICROS);
        if (server_duration != null) {
          entry.put(KEY_SERVER_MICROS, Long.parseLong(server_duration));
        }
         */

        top.add(entry);
      }

      // The queue will keep the most expensive at the top, but sorted in ascending order.
      // this final sort will bring it into descending order as per spec so that the longest
      // calls will be shown first.
      top.sort((o1, o2) -> ((Long) o2.get(KEY_TOTAL_MICROS)).compareTo((Long) o1.get(KEY_TOTAL_MICROS)));

      output.put("service", ident);
      output.put("count", count);
      output.put("top", top);
      return output;
    }

    /**
     * This method is intended to be overridden in test implementations
     * to assert against the output.
     */
    void logOverThreshold(final List<Map<String, Object>> toLog) {
      eventBus.publish(new OverThresholdRequestsRecordedEvent(Duration.ofNanos(emitIntervalNanos), toLog));
    }

    /**
     * Helper method which updates the list with the span and ensures that the sample
     * size is respected.
     */
    private void updateThreshold(final Queue<Request<?>> thresholds, final Request<?> request) {
      thresholds.add(request);
      // Remove the element with the lowest duration, so we only keep the highest ones consistently
      while(thresholds.size() > sampleSize) {
        thresholds.remove();
      }
      hasThresholdWritten = true;
    }
  }

  /**
   * The builder used to configure the {@link ThresholdRequestTracer}.
   */
  public static class Builder {

    private final EventBus eventBus;

    private static final Duration DEFAULT_EMIT_INTERVAL = Duration.ofSeconds(10);
    private static final int DEFAULT_QUEUE_LENGTH = 1024;
    private static final Duration DEFAULT_KV_THRESHOLD = Duration.ofMillis(500);
    private static final Duration DEFAULT_QUERY_THRESHOLD = Duration.ofSeconds(1);
    private static final Duration DEFAULT_VIEW_THRESHOLD = Duration.ofSeconds(1);
    private static final Duration DEFAULT_SEARCH_THRESHOLD = Duration.ofSeconds(1);
    private static final Duration DEFAULT_ANALYTICS_THRESHOLD = Duration.ofSeconds(1);
    private static final int DEFAULT_SAMPLE_SIZE = 10;

    private Duration emitInterval = DEFAULT_EMIT_INTERVAL;
    private int queueLength = DEFAULT_QUEUE_LENGTH;
    private int sampleSize = DEFAULT_SAMPLE_SIZE;

    private Duration kvThreshold = DEFAULT_KV_THRESHOLD;
    private Duration queryThreshold = DEFAULT_QUERY_THRESHOLD;
    private Duration viewThreshold = DEFAULT_VIEW_THRESHOLD;
    private Duration searchThreshold = DEFAULT_SEARCH_THRESHOLD;
    private Duration analyticsThreshold = DEFAULT_ANALYTICS_THRESHOLD;

    Builder(final EventBus eventBus) {
      this.eventBus = eventBus;
    }

    public ThresholdRequestTracer build() {
      return new ThresholdRequestTracer(this);
    }

    /**
     * Allows to customize the emit interval
     *
     * @param emitInterval the interval to use.
     * @return this builder for chaining.
     */
    public Builder emitInterval(final Duration emitInterval) {
      if (emitInterval.isZero()) {
        throw new IllegalArgumentException("Emit interval needs to be greater than 0");
      }

      this.emitInterval = emitInterval;
      return this;
    }

    /**
     * Allows to configure the queue size for the individual span queues
     * used to track the spans over threshold.
     *
     * @param queueLength the queue size to use.
     * @return this builder for chaining.
     */
    public Builder queueLength(final int queueLength) {
      this.queueLength = queueLength;
      return this;
    }

    /**
     * Allows to customize the kvThreshold.
     *
     * @param kvThreshold the threshold to set.
     * @return this builder for chaining.
     */
    public Builder kvThreshold(final Duration kvThreshold) {
      this.kvThreshold = kvThreshold;
      return this;
    }

    /**
     * Allows to customize the n1qlThreshold.
     *
     * @param queryThreshold the threshold to set.
     * @return this builder for chaining.
     */
    public Builder queryThreshold(final Duration queryThreshold) {
      this.queryThreshold = queryThreshold;
      return this;
    }

    /**
     * Allows to customize the viewThreshold.
     *
     * @param viewThreshold the threshold to set.
     * @return this builder for chaining.
     */
    public Builder viewThreshold(final Duration viewThreshold) {
      this.viewThreshold = viewThreshold;
      return this;
    }

    /**
     * Allows to customize the ftsThreshold.
     *
     * @param searchThreshold the threshold to set.
     * @return this builder for chaining.
     */
    public Builder searchThreshold(final Duration searchThreshold) {
      this.searchThreshold = searchThreshold;
      return this;
    }

    /**
     * Allows to customize the analyticsThreshold.
     *
     * @param analyticsThreshold the threshold to set.
     * @return this builder for chaining.
     */
    public Builder analyticsThreshold(final Duration analyticsThreshold) {
      this.analyticsThreshold = analyticsThreshold;
      return this;
    }

    /**
     * Allows to customize the sample size per service.
     *
     * @param sampleSize the sample size to set.
     * @return this builder for chaining.
     */
    public Builder sampleSize(final int sampleSize) {
      this.sampleSize = sampleSize;
      return this;
    }

  }

}
