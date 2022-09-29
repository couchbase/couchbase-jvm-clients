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
import com.couchbase.client.core.cnc.RequestTracer;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.cnc.events.tracing.OverThresholdRequestsRecordedEvent;
import com.couchbase.client.core.deps.org.jctools.queues.MpscArrayQueue;
import com.couchbase.client.core.env.ThresholdLoggingTracerConfig;
import com.couchbase.client.core.error.TracerException;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.transaction.components.CoreTransactionRequest;
import com.couchbase.client.core.util.HostAndPort;
import com.couchbase.client.core.util.NanoTimestamp;
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
public class ThresholdLoggingTracer implements RequestTracer {

  private static final AtomicInteger REQUEST_TRACER_ID = new AtomicInteger();

  private static final String KEY_TOTAL_MICROS = "total_duration_us";
  private static final String KEY_DISPATCH_MICROS = "last_dispatch_duration_us";
  private static final String KEY_TOTAL_DISPATCH_MICROS = "total_dispatch_duration_us";
  private static final String KEY_ENCODE_MICROS = "encode_duration_us";
  private static final String KEY_SERVER_MICROS = "last_server_duration_us";
  private static final String KEY_TOTAL_SERVER_MICROS = "total_server_duration_us";
  private static final String KEY_OPERATION_ID = "operation_id";
  private static final String KEY_OPERATION_NAME = "operation_name";
  private static final String KEY_LAST_LOCAL_SOCKET = "last_local_socket";
  private static final String KEY_LAST_REMOTE_SOCKET = "last_remote_socket";
  private static final String KEY_LAST_LOCAL_ID = "last_local_id";
  private static final String KEY_TIMEOUT = "timeout_ms";

  private final AtomicBoolean running = new AtomicBoolean(false);
  private final Queue<Request<?>> overThresholdQueue;
  private final EventBus eventBus;
  private final Thread worker;

  private final ThresholdLoggingTracerConfig config;
  private final long kvThreshold;
  private final long queryThreshold;
  private final long viewThreshold;
  private final long searchThreshold;
  private final long analyticsThreshold;
  private final long transactionsThreshold;
  private final Duration emitInterval;
  private final int sampleSize;

  /**
   * Creates a builder to customize this tracer.
   *
   * @param eventBus the event bus where the final events will be emitted into.
   * @return the builder to customize.
   * @deprecated please use {@link #create(EventBus, ThresholdLoggingTracerConfig)} instead.
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
  public static ThresholdLoggingTracer create(final EventBus eventBus) {
    return create(eventBus, ThresholdLoggingTracerConfig.create());
  }

  /**
   * Creates a tracer with config and a reference to the event bus.
   *
   * @param eventBus the event bus where the final events will be emitted into.
   * @param config the config that should be used.
   * @return the created tracer ready to be used.
   */
  public static ThresholdLoggingTracer create(final EventBus eventBus, ThresholdLoggingTracerConfig config) {
    return new ThresholdLoggingTracer(eventBus, config);
  }

  /**
   * Internal constructor to build the tracer based on the config provided.
   *
   * @param eventBus the event bus that should be used.
   * @param config the tracer config from where to extract the values.
   */
  private ThresholdLoggingTracer(final EventBus eventBus, ThresholdLoggingTracerConfig config) {
    this.eventBus = eventBus;
    this.overThresholdQueue = new MpscArrayQueue<>(config.queueLength());
    kvThreshold = config.kvThreshold().toNanos();
    analyticsThreshold = config.analyticsThreshold().toNanos();
    searchThreshold = config.searchThreshold().toNanos();
    viewThreshold = config.viewThreshold().toNanos();
    queryThreshold = config.queryThreshold().toNanos();
    transactionsThreshold = config.transactionsThreshold().toNanos();
    sampleSize = config.sampleSize();
    emitInterval = config.emitInterval();
    this.config = config;

    worker = new Thread(new Worker());
    worker.setDaemon(true);
  }

  /**
   * Returns the current configuration.
   */
  public ThresholdLoggingTracerConfig config() {
    return config;
  }

  @Override
  public RequestSpan requestSpan(final String name, final RequestSpan parent) {
    try {
      return new ThresholdRequestSpan(this);
    } catch (Exception ex) {
      throw new TracerException("Failed to create ThresholdRequestSpan", ex);
    }
  }

  /**
   * Finishes the span (sends it off into the queue when over threshold).
   *
   * @param span the finished internal span from the toplevel request.
   */
  void finish(final ThresholdRequestSpan span) {
    try {
      if (span.requestContext() != null) {
        final Request<?> request = span.requestContext().request();
        if (isOverThreshold(request)) {
          if (!overThresholdQueue.offer(request)) {
            // TODO: what to do if dropped because queue full? raise event?
          }
        }
      }
    } catch (Exception ex) {
      throw new TracerException("Failed to finish ThresholdRequestSpan", ex);
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
    if (serviceType == null) {
      // Virtual service
      if (request instanceof CoreTransactionRequest) {
        return tookNanos >= transactionsThreshold;
      }
      return false;
    }
    else if (serviceType == ServiceType.KV && tookNanos >= kvThreshold) {
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

    private final boolean newOutputFormat = Boolean.parseBoolean(
      System.getProperty("com.couchbase.thresholdRequestTracerNewOutputFormat", "true")
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
    private final Queue<Request<?>> transactionsThresholds = new PriorityQueue<>(THRESHOLD_COMPARATOR);

    private long kvThresholdCount = 0;
    private long n1qlThresholdCount = 0;
    private long viewThresholdCount = 0;
    private long ftsThresholdCount = 0;
    private long analyticsThresholdCount = 0;
    private long transactionsThresholdCount = 0;

    private NanoTimestamp lastThresholdLog = NanoTimestamp.never();
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
      if (lastThresholdLog.hasElapsed(emitInterval)) {
        if (newOutputFormat) {
          prepareAndlogOverThresholdNew();
        } else {
          prepareAndlogOverThresholdOld();
        }
        lastThresholdLog = NanoTimestamp.now();
      }

      while (true) {
        Request<?> request = overThresholdQueue.poll();
        if (request == null) {
          return;
        }
        final ServiceType serviceType = request.serviceType();
        if (serviceType == null) {
          // Virtual service
          if (request instanceof CoreTransactionRequest) {
            updateThreshold(transactionsThresholds, request);
            transactionsThresholdCount += 1;
          }
        }
        else if (serviceType == ServiceType.KV) {
          updateThreshold(kvThresholds, request);
          kvThresholdCount += 1;
        } else if (serviceType == ServiceType.QUERY) {
          updateThreshold(n1qlThresholds, request);
          n1qlThresholdCount += 1;
        } else if (serviceType == ServiceType.VIEWS) {
          updateThreshold(viewThresholds, request);
          viewThresholdCount += 1;
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
    private void prepareAndlogOverThresholdNew() {
      if (!hasThresholdWritten) {
        return;
      }
      hasThresholdWritten = false;

      Map<String, Object> output = new HashMap<>();
      if (!kvThresholds.isEmpty()) {
        output.put(
          TracingIdentifiers.SERVICE_KV,
          convertThresholdMetadataNew(kvThresholds, kvThresholdCount)
        );
        kvThresholds.clear();
        kvThresholdCount = 0;
      }
      if (!n1qlThresholds.isEmpty()) {
        output.put(
          TracingIdentifiers.SERVICE_QUERY,
          convertThresholdMetadataNew(n1qlThresholds, n1qlThresholdCount)
        );
        n1qlThresholds.clear();
        n1qlThresholdCount = 0;
      }
      if (!viewThresholds.isEmpty()) {
        output.put(
          TracingIdentifiers.SERVICE_VIEWS,
          convertThresholdMetadataNew(viewThresholds, viewThresholdCount)
        );
        viewThresholds.clear();
        viewThresholdCount = 0;
      }
      if (!ftsThresholds.isEmpty()) {
        output.put(
          TracingIdentifiers.SERVICE_SEARCH,
          convertThresholdMetadataNew(ftsThresholds, ftsThresholdCount)
        );
        ftsThresholds.clear();
        ftsThresholdCount = 0;
      }
      if (!analyticsThresholds.isEmpty()) {
        output.put(
          TracingIdentifiers.SERVICE_ANALYTICS,
          convertThresholdMetadataNew(analyticsThresholds, analyticsThresholdCount)
        );
        analyticsThresholds.clear();
        analyticsThresholdCount = 0;
      }
      if (!transactionsThresholds.isEmpty()) {
        output.put(
                TracingIdentifiers.SERVICE_TRANSACTIONS,
                convertThresholdMetadataNew(transactionsThresholds, transactionsThresholdCount)
        );
        transactionsThresholds.clear();
        transactionsThresholdCount = 0;
      }
      logOverThreshold(output, null);
    }

    private void prepareAndlogOverThresholdOld() {
      if (!hasThresholdWritten) {
        return;
      }
      hasThresholdWritten = false;

      List<Map<String, Object>> output = new ArrayList<>();
      if (!kvThresholds.isEmpty()) {
        output.add(convertThresholdMetadataOld(kvThresholds, kvThresholdCount, TracingIdentifiers.SERVICE_KV));
        kvThresholds.clear();
        kvThresholdCount = 0;
      }
      if (!n1qlThresholds.isEmpty()) {
        output.add(convertThresholdMetadataOld(n1qlThresholds, n1qlThresholdCount, TracingIdentifiers.SERVICE_QUERY));
        n1qlThresholds.clear();
        n1qlThresholdCount = 0;
      }
      if (!viewThresholds.isEmpty()) {
        output.add(convertThresholdMetadataOld(viewThresholds, viewThresholdCount, TracingIdentifiers.SERVICE_VIEWS));
        viewThresholds.clear();
        viewThresholdCount = 0;
      }
      if (!ftsThresholds.isEmpty()) {
        output.add(convertThresholdMetadataOld(ftsThresholds, ftsThresholdCount, TracingIdentifiers.SERVICE_SEARCH));
        ftsThresholds.clear();
        ftsThresholdCount = 0;
      }
      if (!analyticsThresholds.isEmpty()) {
        output.add(convertThresholdMetadataOld(analyticsThresholds, analyticsThresholdCount, TracingIdentifiers.SERVICE_ANALYTICS));
        analyticsThresholds.clear();
        analyticsThresholdCount = 0;
      }
      if (!transactionsThresholds.isEmpty()) {
        output.add(convertThresholdMetadataOld(transactionsThresholds, transactionsThresholdCount, TracingIdentifiers.SERVICE_TRANSACTIONS));
        transactionsThresholds.clear();
        transactionsThresholdCount = 0;
      }
      logOverThreshold(null, output);
    }

    /**
     * Converts the metadata of the requests into the format that is suitable for dumping.
     *
     * @param requests the request data to convert
     * @param count the total count
     * @return the converted map
     */
    private Map<String, Object> convertThresholdMetadataNew(final Queue<Request<?>> requests, final long count) {
      Map<String, Object> output = new HashMap<>();
      List<Map<String, Object>> top = new ArrayList<>();
      for (Request<?> request : requests) {
        Map<String, Object> entry = new HashMap<>();
        entry.put(KEY_TOTAL_MICROS, TimeUnit.NANOSECONDS.toMicros(request.context().logicalRequestLatency()));

        String operationId = request.operationId();
        if (operationId != null) {
          entry.put(KEY_OPERATION_ID, operationId);
        }

        entry.put(KEY_OPERATION_NAME, request.name());

        HostAndPort local = request.context().lastDispatchedFrom();
        HostAndPort peer = request.context().lastDispatchedTo();
        if (local != null) {
          entry.put(KEY_LAST_LOCAL_SOCKET, redactSystem(local).toString());
        }
        if (peer != null) {
          entry.put(KEY_LAST_REMOTE_SOCKET, redactSystem(peer).toString());
        }

        String localId = request.context().lastChannelId();
        if (localId != null) {
          entry.put(KEY_LAST_LOCAL_ID, redactSystem(localId).toString());
        }

        long encodeDuration = request.context().encodeLatency();
        if (encodeDuration > 0) {
          entry.put(KEY_ENCODE_MICROS, TimeUnit.NANOSECONDS.toMicros(encodeDuration));
        }

        long dispatchDuration = request.context().dispatchLatency();
        if (dispatchDuration > 0) {
          entry.put(KEY_DISPATCH_MICROS, TimeUnit.NANOSECONDS.toMicros(dispatchDuration));
        }
        long totalDispatchDuration = request.context().totalDispatchLatency();
        if (totalDispatchDuration > 0) {
          entry.put(KEY_TOTAL_DISPATCH_MICROS, TimeUnit.NANOSECONDS.toMicros(totalDispatchDuration));
        }

        long serverDuration = request.context().serverLatency();
        if (serverDuration > 0) {
          entry.put(KEY_SERVER_MICROS, serverDuration);
        }
        long totalServerDuration = request.context().totalServerLatency();
        if (totalServerDuration > 0) {
          entry.put(KEY_TOTAL_SERVER_MICROS, totalServerDuration);
        }

        entry.put(KEY_TIMEOUT, request.timeout().toMillis());

        top.add(entry);
      }

      // The queue will keep the most expensive at the top, but sorted in ascending order.
      // this final sort will bring it into descending order as per spec so that the longest
      // calls will be shown first.
      top.sort((o1, o2) -> ((Long) o2.get(KEY_TOTAL_MICROS)).compareTo((Long) o1.get(KEY_TOTAL_MICROS)));

      output.put("total_count", count);
      output.put("top_requests", top);
      return output;
    }

    private Map<String, Object> convertThresholdMetadataOld(final Queue<Request<?>> requests, final long count,
                                                            final String ident) {
      Map<String, Object> output = new HashMap<>();
      List<Map<String, Object>> top = new ArrayList<>();
      for (Request<?> request : requests) {
        Map<String, Object> entry = new HashMap<>();
        entry.put("total_us", TimeUnit.NANOSECONDS.toMicros(request.context().logicalRequestLatency()));

        String operationId = request.operationId();
        if (operationId != null) {
          entry.put("last_operation_id", operationId);
        }

        entry.put("operation_name", request.getClass().getSimpleName());

        HostAndPort local = request.context().lastDispatchedFrom();
        HostAndPort peer = request.context().lastDispatchedTo();
        if (local != null) {
          entry.put("last_local_address", redactSystem(local).toString());
        }
        if (peer != null) {
          entry.put("last_remote_address", redactSystem(peer).toString());
        }

        String localId = request.context().lastChannelId();
        if (localId != null) {
          entry.put("last_local_id", redactSystem(localId).toString());
        }

        long encodeDuration = request.context().encodeLatency();
        if (encodeDuration > 0) {
          entry.put("encode_us", TimeUnit.NANOSECONDS.toMicros(encodeDuration));
        }

        long dispatchDuration = request.context().dispatchLatency();
        if (dispatchDuration > 0) {
          entry.put("last_dispatch_us", TimeUnit.NANOSECONDS.toMicros(dispatchDuration));
        }

        long serverDuration = request.context().serverLatency();
        if (serverDuration > 0) {
          entry.put("server_us", serverDuration);
        }

        top.add(entry);
      }

      // The queue will keep the most expensive at the top, but sorted in ascending order.
      // this final sort will bring it into descending order as per spec so that the longest
      // calls will be shown first.
      top.sort((o1, o2) -> ((Long) o2.get("total_us")).compareTo((Long) o1.get("total_us")));

      output.put("service", ident);
      output.put("count", count);
      output.put("top", top);
      return output;
    }

    /**
     * This method is intended to be overridden in test implementations
     * to assert against the output.
     */
    void logOverThreshold(final Map<String, Object> toLogNew, final List<Map<String, Object>> toLogOld) {
      eventBus.publish(new OverThresholdRequestsRecordedEvent(emitInterval, toLogNew, toLogOld));
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
   * The builder used to configure the {@link ThresholdLoggingTracer}.
   */
  public static class Builder {

    private final EventBus eventBus;

    private final ThresholdLoggingTracerConfig.Builder config = ThresholdLoggingTracerConfig.builder();

    Builder(final EventBus eventBus) {
      this.eventBus = eventBus;
    }

    public ThresholdLoggingTracer build() {
      return new ThresholdLoggingTracer(eventBus, config.build());
    }

    /**
     * Allows to customize the emit interval
     *
     * @param emitInterval the interval to use.
     * @return this builder for chaining.
     */
    public Builder emitInterval(final Duration emitInterval) {
      config.emitInterval(emitInterval);
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
      config.queueLength(queueLength);
      return this;
    }

    /**
     * Allows to customize the kvThreshold.
     *
     * @param kvThreshold the threshold to set.
     * @return this builder for chaining.
     */
    public Builder kvThreshold(final Duration kvThreshold) {
      config.kvThreshold(kvThreshold);
      return this;
    }

    /**
     * Allows to customize the n1qlThreshold.
     *
     * @param queryThreshold the threshold to set.
     * @return this builder for chaining.
     */
    public Builder queryThreshold(final Duration queryThreshold) {
      config.queryThreshold(queryThreshold);
      return this;
    }

    /**
     * Allows to customize the viewThreshold.
     *
     * @param viewThreshold the threshold to set.
     * @return this builder for chaining.
     */
    public Builder viewThreshold(final Duration viewThreshold) {
      config.viewThreshold(viewThreshold);
      return this;
    }

    /**
     * Allows to customize the ftsThreshold.
     *
     * @param searchThreshold the threshold to set.
     * @return this builder for chaining.
     */
    public Builder searchThreshold(final Duration searchThreshold) {
      config.searchThreshold(searchThreshold);
      return this;
    }

    /**
     * Allows to customize the analyticsThreshold.
     *
     * @param analyticsThreshold the threshold to set.
     * @return this builder for chaining.
     */
    public Builder analyticsThreshold(final Duration analyticsThreshold) {
      config.analyticsThreshold(analyticsThreshold);
      return this;
    }

    /**
     * Allows to customize the transactionsThreshold.
     *
     * @param transactionsThreshold the threshold to set.
     * @return this builder for chaining.
     */
    public Builder transactionsThreshold(final Duration transactionsThreshold) {
      config.transactionsThreshold(transactionsThreshold);
      return this;
    }

    /**
     * Allows to customize the sample size per service.
     *
     * @param sampleSize the sample size to set.
     * @return this builder for chaining.
     */
    public Builder sampleSize(final int sampleSize) {
      config.sampleSize(sampleSize);
      return this;
    }

  }

}
