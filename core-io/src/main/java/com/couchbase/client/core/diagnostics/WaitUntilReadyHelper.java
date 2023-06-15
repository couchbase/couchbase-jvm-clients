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

package com.couchbase.client.core.diagnostics;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.Reactor;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.AbstractEvent;
import com.couchbase.client.core.cnc.EventBus;
import com.couchbase.client.core.cnc.events.core.WaitUntilReadyCompletedEvent;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ArrayNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.DefaultFullHttpRequest;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpVersion;
import com.couchbase.client.core.endpoint.EndpointState;
import com.couchbase.client.core.endpoint.http.CoreHttpPath;
import com.couchbase.client.core.error.UnambiguousTimeoutException;
import com.couchbase.client.core.error.context.CancellationErrorContext;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.msg.RequestTarget;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.manager.GenericManagerRequest;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.util.CbThrowables;
import com.couchbase.client.core.util.NanoTimestamp;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.couchbase.client.core.util.CbCollections.filter;
import static com.couchbase.client.core.util.CbCollections.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

/**
 * Helper class to perform the "wait until ready" logic.
 */
@Stability.Internal
public class WaitUntilReadyHelper {

  private static class WaitUntilReadyDiagnostic extends AbstractEvent {
    private final String message;

    protected WaitUntilReadyDiagnostic(String message) {
      super(Severity.DEBUG, Category.CORE.path() + ".WaitUntilReady", Duration.ZERO, null);
      this.message = requireNonNull(message);
    }

    @Override
    public String description() {
      return message;
    }
  }

  @Stability.Internal
  public interface WaitUntilReadyLogger {
    WaitUntilReadyLogger dummy = new WaitUntilReadyLogger() {};

    static WaitUntilReadyLogger create(EventBus eventBus) {
      String id = UUID.randomUUID().toString();

      return new WaitUntilReadyLogger() {
        @Override
        public void message(String message) {
          message = id + " " + message;
          if (EventBus.PublishResult.SUCCESS != eventBus.publish(new WaitUntilReadyDiagnostic(message))) {
            System.err.println("[WaitUntilReadyDiagnostic] " + message);
          }
        }
      };
    }

    default void message(String message) {
    }

    default void waitingBecause(String message) {
      message("Waiting because " + message);
    }
  }

  @Stability.Internal
  public static CompletableFuture<Void> waitUntilReady(final Core core, final Set<ServiceType> serviceTypes,
                                                       final Duration timeout, final ClusterState desiredState,
                                                       final Optional<String> bucketName) {
    // Some temporary glue until Core and CoreProtostellar are separated
    if (core.isProtostellar()) {
      return core.protostellar().waitUntilReady(serviceTypes, timeout, desiredState, bucketName.orElse(null));
    }

    WaitUntilReadyLogger log = WaitUntilReadyLogger.create(core.environment().eventBus());

    log.message("Starting WaitUntilReady." +
      " serviceTypes=" + serviceTypes + "" +
      ", timeout=" + timeout +
      ", desiredState=" + desiredState +
      ", bucketName=" + bucketName
    );

    final WaitUntilReadyState state = new WaitUntilReadyState(log);

    state.transition(WaitUntilReadyStage.CONFIG_LOAD);
    return Flux
      .interval(Duration.ofMillis(10), core.context().environment().scheduler())
      // There is a good chance that downstream demand is lower (taking longer than
      // the 10ms of the interval signal). We can just drop the ticks that we don't need,
      // since the interval acts like a "pacemaker" here and keeps us going with new tries
      // until the wait until ready completes or times out.
      .onBackpressureDrop()
      .filter(i -> {
          try {
            if (core.configurationProvider().bucketConfigLoadInProgress()) {
              log.waitingBecause("bucket config load is in progress");
              return false;
            }

            if (core.configurationProvider().globalConfigLoadInProgress()) {
              log.waitingBecause("global config load is in progress");
              return false;
            }

            if (bucketName.isPresent() && core.configurationProvider().collectionRefreshInProgress()) {
              log.waitingBecause("collection refresh is in progress for bucket " + bucketName.get());
              return false;
            }

            if (bucketName.isPresent() && core.clusterConfig().bucketConfig(bucketName.get()) == null) {
              log.waitingBecause("cluster config does not yet have config for bucket " + bucketName.get());
              return false;
            }

            return true;

          } catch (Throwable t) {
            log.message("Unexpected exception while waiting for config load: " + CbThrowables.getStackTraceAsString(t));
            throw t;
          }
        }
      )
      .flatMap(i -> {
        if (!bucketName.isPresent()) {
          log.message("Skipping node health check because no bucket name was specified.");
          return Flux.just(i);
        }

        state.transition(WaitUntilReadyStage.BUCKET_NODES_HEALTHY);
        // To avoid tmpfails on the bucket, we double check that all nodes from the nodes list are
        // in a healthy status - but for this we need to actually fetch the verbose config, since
        // the terse one doesn't have that status in it.
        String httpPath = CoreHttpPath.formatPath("/pools/default/buckets/{}", bucketName.get());
        GenericManagerRequest request = new GenericManagerRequest(
          core.context(),
          () -> new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, httpPath),
          true,
          null
        );
        log.message("Sending manager request to check bucket health; httpPath=" + httpPath);
        core.send(request);
        return Reactor.wrap(request, request.response(), true)
          .filter(response -> {
            try {
              if (response.status() != ResponseStatus.SUCCESS) {
                log.waitingBecause("Manager request to check bucket health failed with response status " + response.status() + ";" +
                  " httpStatusCode=" + response.httpStatus() +
                  ", responseBody=" + new String(response.content(), StandardCharsets.UTF_8) +
                  ", requestContext=" + request.context()
                );
                return false;
              }

              ObjectNode root = (ObjectNode) Mapper.decodeIntoTree(response.content());
              ArrayNode nodes = (ArrayNode) root.get("nodes");

              long healthy = StreamSupport
                .stream(nodes.spliterator(), false)
                .filter(node -> node.get("status").asText().equals("healthy"))
                .count();

              if (nodes.size() == healthy) {
                log.message("All " + healthy + " nodes are healthy");
                return true;
              }

              log.waitingBecause(healthy + " of " + nodes.size() + " nodes are healthy");
              return false;

            } catch (Throwable t) {
              log.message("Unexpected error while checking bucket health: " + CbThrowables.getStackTraceAsString(t));
              throw t;
            }
            })
            .map(ignored -> i);
      })
      .take(1)
      .flatMap(aLong -> {
        // There could be a scenario where a user calls waitUntilReady on the cluster object despite
        // running a server cluster pre 6.5 (which does not support cluster-level config). Per definition,
        // we cannot make progress. So we let WaitUntilReady complete (so a user can move on to open a bucket)
        // but we'll print a warning to make sure we clarify the situation.
        //
        // Note that an explicit decision has been made to not fail fast and let it pass through because individual
        // operations will fail anyways if no further bucket is being opened and there is just nothing to "wait for"
        // to being ready at this point. Bucket level wait until ready is the way to go there.
        if (!bucketName.isPresent() && !core.clusterConfig().hasClusterOrBucketConfig()) {
          log.message(
            "No bucket name is present, and no cluster or bucket config is present." +
              " This usually indicates cluster.waitUntilReady() was called against a pre-6.5 cluster." +
              " There's nothing to wait for, so consider this operation complete!"
          );
          state.transition(WaitUntilReadyStage.COMPLETE);
          WaitUntilReadyContext waitUntilReadyContext = new WaitUntilReadyContext(
            servicesToCheck(core, serviceTypes, bucketName, log),
            timeout,
            desiredState,
            bucketName,
            core.diagnostics().collect(Collectors.groupingBy(EndpointDiagnostics::type)),
            state
          );
          core.context().environment().eventBus().publish(new WaitUntilReadyCompletedEvent(
            waitUntilReadyContext, WaitUntilReadyCompletedEvent.Reason.CLUSTER_LEVEL_NOT_SUPPORTED
          ));
          return Flux.empty();
        }

        state.transition(WaitUntilReadyStage.PING);
        final Flux<ClusterState> diagnostics = Flux
          .interval(Duration.ofMillis(10), core.context().environment().scheduler())
          // Diagnostics are in-memory and should be quicker than 10ms, but just in case
          // make sure that slower downstream does not terminate the diagnostics interval
          // pacemaker.
          .onBackpressureDrop()
          .map(i -> diagnosticsCurrentState(core, log))
          .takeUntil(s -> {
            boolean done = s == desiredState;
            if (!done) {
              log.waitingBecause("current cluster state " + s + " does not satisfy desired state " + desiredState);
            } else {
              log.message("Done waiting for diagnostics! Current cluster state " + s + " satisfies desired state " + desiredState);
            }

            return done;
          });

        return Flux.concat(ping(core, servicesToCheck(core, serviceTypes, bucketName, log), timeout, bucketName, log), diagnostics);
      })
      .then()
      .timeout(
        timeout,
        Mono.defer(() -> {
          log.message("WaitUntilReady timed out :-(");

          WaitUntilReadyContext waitUntilReadyContext = new WaitUntilReadyContext(
            servicesToCheck(core, serviceTypes, bucketName, log),
            timeout,
            desiredState,
            bucketName,
            core.diagnostics().collect(Collectors.groupingBy(EndpointDiagnostics::type)),
            state
          );
          CancellationErrorContext errorContext = new CancellationErrorContext(waitUntilReadyContext);
          return Mono.error(new UnambiguousTimeoutException("WaitUntilReady timed out", errorContext));
        }),
        core.context().environment().scheduler()
      )
      .doOnSuccess(unused -> {
        log.message("WaitUntilReady succeeded :-)");

        state.transition(WaitUntilReadyStage.COMPLETE);
        WaitUntilReadyContext waitUntilReadyContext = new WaitUntilReadyContext(
          servicesToCheck(core, serviceTypes, bucketName, log),
          timeout,
          desiredState,
          bucketName,
          core.diagnostics().collect(Collectors.groupingBy(EndpointDiagnostics::type)),
          state
        );
        core.context().environment().eventBus().publish(
          new WaitUntilReadyCompletedEvent(waitUntilReadyContext, WaitUntilReadyCompletedEvent.Reason.SUCCESS));
      })
      .toFuture();
  }

  private static Set<ServiceType> servicesToCheck(final Core core, final Set<ServiceType> serviceTypes,
                                                  final Optional<String> bucketName,
                                                  final WaitUntilReadyLogger log) {
    return !isNullOrEmpty(serviceTypes)
      ? serviceTypes
      : HealthPinger
      .extractPingTargets(core.clusterConfig(), bucketName, log)
      .stream()
      .map(RequestTarget::serviceType)
      .collect(Collectors.toSet());
  }

  /**
   * Checks diagnostics and returns the current aggregated cluster state.
   */
  private static ClusterState diagnosticsCurrentState(
    final Core core,
    final WaitUntilReadyLogger log
  ) {
    List<EndpointDiagnostics> diagnosticsResult = core.diagnostics().collect(Collectors.toList());
    Map<ServiceType, List<EndpointDiagnostics>> groupedByService = diagnosticsResult.stream().collect(Collectors.groupingBy(EndpointDiagnostics::type));

    log.message("diagnostics: Raw results: " + diagnosticsResult);
    log.message("diagnostics: Raw results, grouped by service: " + groupedByService);
    log.message("diagnostics: Endpoints not in CONNECTED state: " + filter(diagnosticsResult, it -> it.state() != EndpointState.CONNECTED));

    return DiagnosticsResult.aggregateClusterState(groupedByService.values());
  }

  /**
   * Performs the ping part of the wait until ready (calling into the health pinger).
   */
  private static Flux<PingResult> ping(
    final Core core,
    final Set<ServiceType> serviceTypes,
    final Duration timeout,
    final Optional<String> bucketName,
    final WaitUntilReadyLogger log
  ) {
    return HealthPinger
      .ping(core, Optional.of(timeout), core.context().environment().retryStrategy(), serviceTypes, Optional.empty(), bucketName, log)
      .doOnNext(it -> log.message("ping: PingResult = " + it))
      .flux();
  }

  /**
   * Encapsulates the state of where a wait until ready flow is in.
   */
  @Stability.Internal
  public static class WaitUntilReadyState {

    private final Map<WaitUntilReadyStage, Long> timings = new ConcurrentHashMap<>();
    private final AtomicLong totalDuration = new AtomicLong();

    private volatile WaitUntilReadyStage currentStage = WaitUntilReadyStage.INITIAL;
    private volatile NanoTimestamp currentStart = NanoTimestamp.now();

    private final WaitUntilReadyLogger log;

    public WaitUntilReadyState(WaitUntilReadyLogger log) {
      this.log = requireNonNull(log);
    }

    void transition(final WaitUntilReadyStage next) {
      long timing = currentStart.elapsed().toMillis();
      if (currentStage != WaitUntilReadyStage.INITIAL) {
        timings.put(currentStage, timing);
        log.message("Stage '" + currentStage + "' took " + currentStart.elapsed());
      }
      totalDuration.addAndGet(timing);
      log.message("Transitioning from stage " + currentStage + " to stage " + next + ". Total elapsed time since waiting started: " + Duration.ofMillis(totalDuration.get()));
      currentStage = next;
      currentStart = NanoTimestamp.now();
    }

    public Map<String, Object> export() {
      Map<String, Object> toExport = new TreeMap<>();

      toExport.put("current_stage", currentStage);
      if (currentStage != WaitUntilReadyStage.COMPLETE) {
        long currentMs = currentStart.elapsed().toMillis();
        toExport.put("current_stage_since_ms", currentMs);
        toExport.put("total_ms", totalDuration.get() + currentMs);
      } else {
        toExport.put("total_ms", totalDuration.get());
      }
      toExport.put("timings_ms", timings);

      return toExport;
    }

    public long totalDuration() {
      return totalDuration.get();
    }

  }

  /**
   * Describes the different stages of wait until ready.
   */
  private enum WaitUntilReadyStage {
    /**
     * Not started yet, initial stage.
     */
    INITIAL,
    /**
     * Waits until all global and bucket level configs are loaded.
     */
    CONFIG_LOAD,
    /**
     * Waits until all the nodes in a bucket config are healthy.
     */
    BUCKET_NODES_HEALTHY,
    /**
     * Performs ping operations and checks their return values.
     */
    PING,
    /**
     * Completed successfully.
     */
    COMPLETE,
  }

}
