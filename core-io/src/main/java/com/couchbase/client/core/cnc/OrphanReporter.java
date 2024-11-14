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

package com.couchbase.client.core.cnc;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.events.tracing.OrphanRecordDroppedEvent;
import com.couchbase.client.core.cnc.events.tracing.OrphanReporterFailureDetectedEvent;
import com.couchbase.client.core.cnc.events.tracing.OrphansRecordedEvent;
import com.couchbase.client.core.env.OrphanReporterConfig;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.UnmonitoredRequest;
import com.couchbase.client.core.msg.kv.KeyValueRequest;
import com.couchbase.client.core.msg.view.ViewRequest;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.util.HostAndPort;
import com.couchbase.client.core.util.NanoTimestamp;
import com.couchbase.client.core.util.NativeImageHelper;
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

@Stability.Internal
public class OrphanReporter {

  public static final String ORPHAN_TREAD_PREFIX = "cb-orphan-";

  private static final AtomicInteger ORPHAN_REPORTER_ID = new AtomicInteger();

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
  volatile Thread worker = null; // visible for testing
  private final Queue<Request<?>> orphanQueue;
  private final Duration emitInterval;
  private final int sampleSize;
  private final EventBus eventBus;
  private final boolean enabled;
  private final OrphanReporterConfig config;

  /**
   * Creates a new {@link OrphanReporter}.
   * <p>
   * Please do not instantiate this class directly, but rather let it be handled through the environment and
   * customize it through the {@link OrphanReporterConfig} which can also be provided on the environment.
   *
   * @param eventBus the event bus where the messages should be emitted on.
   * @param config the configuration for this reporter.
   */
  @Stability.Internal
  public OrphanReporter(final EventBus eventBus, final OrphanReporterConfig config) {
    this.eventBus = eventBus;
    this.orphanQueue = NativeImageHelper.createMpscArrayQueue(config.queueLength());
    this.emitInterval = config.emitInterval();
    this.sampleSize = config.sampleSize();
    this.enabled = config.enabled();
    this.config = config;

    // Spawn a thread only if the reporter is enabled.
    if (enabled) {
      worker = new Thread(new Worker());
      worker.setDaemon(true);
      worker.setName(ORPHAN_TREAD_PREFIX + ORPHAN_REPORTER_ID.incrementAndGet());
    }
  }

  /**
   * Returns the currently active configuration.
   */
  public OrphanReporterConfig config() {
    return config;
  }

  /**
   * Starts the orphan reporter.
   *
   * @return completes the Mono once it has been started properly.
   */
  public Mono<Void> start() {
    return Mono.defer(() -> {
      if (enabled && running.compareAndSet(false, true)) {
        worker.start();
      }
      return Mono.empty();
    });
  }

  /**
   * Stops the orphan reporter.
   *
   * @return completes the Mono once it has been stopped properly.
   */
  public Mono<Void> stop() {
    return Mono.defer(() -> {
      if (enabled && running.compareAndSet(true, false)) {
        worker.interrupt();
      }
      return Mono.empty();
    });
  }

  /**
   * Reports an orphaned {@link Request}.
   * <p>
   * If the orphan could not be recorded because the buffer is full, a {@link OrphanRecordDroppedEvent} will
   * be raised on the event bus.
   * <p>
   * If you wish to ignore certain requests as being considered in the orphan reporting, make sure they implement
   * the {@link UnmonitoredRequest} marker interface.
   *
   * @param request the request to report as orphan.
   */
  public void report(final Request<?> request) {
      if (!enabled || request instanceof UnmonitoredRequest) {
        return;
      }

      if (!orphanQueue.offer(request)) {
        eventBus.publish(new OrphanRecordDroppedEvent(request.getClass()));
      }
  }

  /**
   * Worker thread which runs in a separate thread and consumes the orphan queue, aggregates the infos
   * and puts them onto the event bus at regular intervals.
   */
  private class Worker implements Runnable {

    /**
     * Time this worker spends between check cycles. 100ms should be granular enough
     * but making it configurable, who knows...
     */
    private final long workerSleepMs = Long.parseLong(
      System.getProperty("com.couchbase.orphanReporterSleep", "100")
    );

    private final boolean newOutputFormat = Boolean.parseBoolean(
      System.getProperty("com.couchbase.orphanReporterNewOutputFormat", "true")
    );

    /**
     * Compares request by their logical request latency for the priority threshold queues.
     */
    private final Comparator<Request<?>> THRESHOLD_COMPARATOR = Comparator.comparingLong(
      o -> o.context().logicalRequestLatency()
    );

    private NanoTimestamp lastThresholdLog = NanoTimestamp.never();
    private boolean hasThresholdWritten;

    private final Queue<Request<?>> kvOrphans = new PriorityQueue<>(THRESHOLD_COMPARATOR);
    private final Queue<Request<?>> queryOrphans = new PriorityQueue<>(THRESHOLD_COMPARATOR);
    private final Queue<Request<?>> viewOrphans = new PriorityQueue<>(THRESHOLD_COMPARATOR);
    private final Queue<Request<?>> searchOrphans = new PriorityQueue<>(THRESHOLD_COMPARATOR);
    private final Queue<Request<?>> analyticsOrphans = new PriorityQueue<>(THRESHOLD_COMPARATOR);

    private long kvOrphanCount = 0;
    private long queryOrphanCount = 0;
    private long viewOrphanCount = 0;
    private long searchOrphanCount = 0;
    private long analyticsOrphanCount = 0;

    @Override
    public void run() {
      while (running.get()) {
        try {
          handleOrphanQueue();
          Thread.sleep(workerSleepMs);
        } catch (final InterruptedException ex) {
          if (!running.get()) {
            return;
          }
        } catch (final Exception ex) {
          eventBus.publish(new OrphanReporterFailureDetectedEvent(ex));
        }
      }
    }

    private void handleOrphanQueue() {
      if (lastThresholdLog.hasElapsed(emitInterval)) {
        if (newOutputFormat) {
          prepareAndLogOrphansNew();
        } else {
          prepareAndLogOrphansOld();
        }
        lastThresholdLog = NanoTimestamp.now();
      }

      while (true) {
        Request<?> request = orphanQueue.poll();
        if (request == null) {
          return;
        }
        final ServiceType serviceType = request.serviceType();
        if (serviceType == ServiceType.KV) {
          updateSet(kvOrphans, request);
          kvOrphanCount += 1;
        } else if (serviceType == ServiceType.QUERY) {
          updateSet(queryOrphans, request);
          queryOrphanCount += 1;
        } else if (serviceType == ServiceType.VIEWS) {
          updateSet(viewOrphans, request);
          viewOrphanCount += 1;
        } else if (serviceType == ServiceType.SEARCH) {
          updateSet(searchOrphans, request);
          searchOrphanCount += 1;
        } else if (serviceType == ServiceType.ANALYTICS) {
          updateSet(analyticsOrphans, request);
          analyticsOrphanCount += 1;
        }
      }
    }

    /**
     * Helper method which updates the list with the span and ensures that the sample
     * size is respected.
     */
    private void updateSet(final Queue<Request<?>> set, final Request<?> request) {
      set.add(request);
      // Remove the element with the lowest duration, so we only keep the highest ones consistently
      while(set.size() > sampleSize) {
        set.remove();
      }
      hasThresholdWritten = true;
    }

    private void prepareAndLogOrphansNew() {
      if (!hasThresholdWritten) {
        return;
      }
      hasThresholdWritten = false;

      Map<String, Object> output = new HashMap<>();
      if (!kvOrphans.isEmpty()) {
        output.put(TracingIdentifiers.SERVICE_KV, convertOrphanMetadataNew(kvOrphans, kvOrphanCount));
        kvOrphans.clear();
        kvOrphanCount = 0;
      }
      if (!queryOrphans.isEmpty()) {
        output.put(TracingIdentifiers.SERVICE_QUERY, convertOrphanMetadataNew(queryOrphans, queryOrphanCount));
        queryOrphans.clear();
        queryOrphanCount = 0;
      }
      if (!viewOrphans.isEmpty()) {
        output.put(TracingIdentifiers.SERVICE_VIEWS, convertOrphanMetadataNew(viewOrphans, viewOrphanCount));
        viewOrphans.clear();
        viewOrphanCount = 0;
      }
      if (!searchOrphans.isEmpty()) {
        output.put(TracingIdentifiers.SERVICE_SEARCH, convertOrphanMetadataNew(searchOrphans, searchOrphanCount));
        searchOrphans.clear();
        searchOrphanCount = 0;
      }
      if (!analyticsOrphans.isEmpty()) {
        output.put(TracingIdentifiers.SERVICE_ANALYTICS, convertOrphanMetadataNew(analyticsOrphans, analyticsOrphanCount));
        analyticsOrphans.clear();
        analyticsOrphanCount = 0;
      }
      logOrphans(output, null);
    }

    private void prepareAndLogOrphansOld() {
      if (!hasThresholdWritten) {
        return;
      }
      hasThresholdWritten = false;

      List<Map<String, Object>> output = new ArrayList<>();
      if (!kvOrphans.isEmpty()) {
        output.add(convertOrphanMetadataOld(kvOrphans, kvOrphanCount, TracingIdentifiers.SERVICE_KV));
        kvOrphans.clear();
        kvOrphanCount = 0;
      }
      if (!queryOrphans.isEmpty()) {
        output.add(convertOrphanMetadataOld(queryOrphans, queryOrphanCount, TracingIdentifiers.SERVICE_QUERY));
        queryOrphans.clear();
        queryOrphanCount = 0;
      }
      if (!viewOrphans.isEmpty()) {
        output.add(convertOrphanMetadataOld(viewOrphans, viewOrphanCount, TracingIdentifiers.SERVICE_VIEWS));
        viewOrphans.clear();
        viewOrphanCount = 0;
      }
      if (!searchOrphans.isEmpty()) {
        output.add(convertOrphanMetadataOld(searchOrphans, searchOrphanCount, TracingIdentifiers.SERVICE_SEARCH));
        searchOrphans.clear();
        searchOrphanCount = 0;
      }
      if (!analyticsOrphans.isEmpty()) {
        output.add(convertOrphanMetadataOld(analyticsOrphans, analyticsOrphanCount, TracingIdentifiers.SERVICE_ANALYTICS));
        analyticsOrphans.clear();
        analyticsOrphanCount = 0;
      }
      logOrphans(null,  output);
    }

    private Map<String, Object> convertOrphanMetadataNew(Queue<Request<?>> requests, long count) {
      Map<String, Object> output = new HashMap<>();
      List<Map<String, Object>> top = new ArrayList<>();
      for (Request<?> request : requests) {
        HashMap<String, Object> fieldMap = new HashMap<>();

        if (request != null) {
          fieldMap.put(KEY_TOTAL_MICROS, TimeUnit.NANOSECONDS.toMicros(request.context().logicalRequestLatency()));

          fieldMap.put(KEY_OPERATION_NAME, request.name());

          String operationId = request.operationId();
          if (operationId != null) {
            fieldMap.put(KEY_OPERATION_ID, operationId);
          }

          String localId = request.context().lastChannelId();
          if (localId != null) {
            fieldMap.put(KEY_LAST_LOCAL_ID, redactSystem(localId));
          }

          long encodeDuration = request.context().encodeLatency();
          if (encodeDuration > 0) {
            fieldMap.put(KEY_ENCODE_MICROS, TimeUnit.NANOSECONDS.toMicros(encodeDuration));
          }

          long dispatchDuration = request.context().dispatchLatency();
          if (dispatchDuration > 0) {
            fieldMap.put(KEY_DISPATCH_MICROS, TimeUnit.NANOSECONDS.toMicros(dispatchDuration));
          }
          long totalDispatchDuration = request.context().totalDispatchLatency();
          if (totalDispatchDuration > 0) {
            fieldMap.put(KEY_TOTAL_DISPATCH_MICROS, TimeUnit.NANOSECONDS.toMicros(totalDispatchDuration));
          }

          HostAndPort local = request.context().lastDispatchedFrom();
          HostAndPort peer = request.context().lastDispatchedTo();
          if (local != null) {
            fieldMap.put(KEY_LAST_LOCAL_SOCKET, redactSystem(local.toString()));
          }
          if (peer != null) {
            fieldMap.put(KEY_LAST_REMOTE_SOCKET, redactSystem(peer.toString()));
          }

          long serverDuration = request.context().serverLatency();
          if (serverDuration > 0) {
            fieldMap.put(KEY_SERVER_MICROS, serverDuration);
          }
          long totalServerDuration = request.context().totalServerLatency();
          if (totalServerDuration > 0) {
            fieldMap.put(KEY_TOTAL_SERVER_MICROS, totalServerDuration);
          }

          fieldMap.put(KEY_TIMEOUT, request.timeout().toMillis());
        }

        top.add(fieldMap);
      }
      output.put("total_count", count);
      output.put("top_requests", top);
      return output;
    }

    private Map<String, Object> convertOrphanMetadataOld(Queue<Request<?>> requests, long count, String serviceType) {
      Map<String, Object> output = new HashMap<>();
      List<Map<String, Object>> top = new ArrayList<>();
      for (Request<?> request : requests) {
        HashMap<String, Object> fieldMap = new HashMap<>();

        if (request != null) {
          String name = request.getClass().getSimpleName().replace("Request", "").toLowerCase();
          fieldMap.put("s", name);

          String operationId = request.operationId();
          if (operationId != null) {
            fieldMap.put("i", operationId);
          }
          if (request instanceof KeyValueRequest) {
            fieldMap.put("b", ((KeyValueRequest<?>) request).bucket());
          } else if (request instanceof ViewRequest) {
            fieldMap.put("b", ((ViewRequest) request).bucket());
          }
          String localId = request.context().lastChannelId();
          if (localId != null) {
            fieldMap.put("c", redactSystem(localId));
          }

          HostAndPort local = request.context().lastDispatchedFrom();
          HostAndPort peer = request.context().lastDispatchedTo();
          if (local != null) {
            fieldMap.put("l", redactSystem(local.toString()));
          }
          if (peer != null) {
            fieldMap.put("r", redactSystem(peer.toString()));
          }

          long serverDuration = request.context().serverLatency();
          if (serverDuration > 0) {
            fieldMap.put("d", serverDuration);
          }

          long timeout = request.timeout().toMillis();
          fieldMap.put("t", timeout);
        }

        top.add(fieldMap);
      }
      output.put("service", serviceType);
      output.put("count", count);
      output.put("top", top);
      return output;
    }

    /**
     * This method is intended to be overridden in test implementations
     * to assert against the output.
     */
    void logOrphans(final Map<String, Object> toLogNew, final List<Map<String, Object>> toLogOld) {
      eventBus.publish(new OrphansRecordedEvent(emitInterval, toLogNew, toLogOld));
    }

  }

}
