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
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.endpoint.http.CoreHttpPath;
import com.couchbase.client.core.error.UnambiguousTimeoutException;
import com.couchbase.client.core.error.context.CancellationErrorContext;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.msg.RequestTarget;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.manager.GenericManagerRequest;
import com.couchbase.client.core.retry.FailFastRetryStrategy;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.util.CbThrowables;
import com.couchbase.client.core.util.Deadline;
import com.couchbase.client.core.util.NanoTimestamp;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;
import reactor.util.retry.Retry;
import reactor.util.retry.RetryBackoffSpec;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.couchbase.client.core.diagnostics.HealthPinger.extractPingTargets;
import static com.couchbase.client.core.diagnostics.HealthPinger.pingTarget;
import static com.couchbase.client.core.util.CbCollections.setCopyOf;
import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

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

  private static class NotReadyYetException extends RuntimeException {
    public NotReadyYetException(String message) {
      super(message);
    }
  }

  private static RetryBackoffSpec retryWithMaxBackoff(Duration maxBackoff) {
    return Retry
      .backoff(Long.MAX_VALUE, Duration.ofMillis(10))
      .maxBackoff(maxBackoff)
      .jitter(0.5);
  }

  private static <T> Mono<T> retryUntilReady(
    final RetryBackoffSpec retrySpec,
    final String stageName,
    final WaitUntilReadyLogger log,
    final Mono<T> waitUntilReadyStage
  ) {
    return waitUntilReadyStage.retryWhen(retrySpec.filter(t -> {
      if (t instanceof NotReadyYetException) {
        log.waitingBecause(t.getMessage());
      } else {
        log.message("Unexpected exception while waiting for " + stageName + ": " + CbThrowables.getStackTraceAsString(t));
      }
      return true;
    }));
  }

  private static Mono<Void> waitForConfig(
    final Core core,
    final @Nullable String bucketName,
    final WaitUntilReadyLogger log
  ) {
    Mono<Void> stage = Mono.fromRunnable(() -> {
        if (core.configurationProvider().globalConfigLoadInProgress()) {
          throw new NotReadyYetException("global config load is in progress");
        }

        if (core.configurationProvider().bucketConfigLoadInProgress()) {
          throw new NotReadyYetException("bucket config load is in progress");
        }

        if (bucketName != null && core.configurationProvider().collectionRefreshInProgress()) {
          throw new NotReadyYetException("collection refresh is in progress for bucket " + bucketName);
        }

        if (bucketName != null && core.clusterConfig().bucketConfig(bucketName) == null) {
          throw new NotReadyYetException("cluster config does not yet have config for bucket " + bucketName);
        }
      }
    );

    return retryUntilReady(
      retryWithMaxBackoff(Duration.ofMillis(100)),
      "config load",
      log,
      stage
    );
  }

  private static Mono<Void> waitForNodeHealth(
    final Core core,
    final @Nullable String bucketName,
    final WaitUntilReadyLogger log
  ) {
    if (bucketName == null) {
      return Mono.fromRunnable(() ->
        log.message("Skipping node health check because no bucket name was specified.")
      );
    }

    Mono<Void> stage = Mono.defer(() -> {
      // To avoid tmpfails on the bucket, we double-check that all nodes from the nodes list are
      // in a healthy status - but for this we need to actually fetch the verbose config, since
      // the terse one doesn't have that status in it.
      String httpPath = CoreHttpPath.formatPath("/pools/default/buckets/{}", bucketName);
      GenericManagerRequest request = new GenericManagerRequest(
        core.context(),
        () -> new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, httpPath),
        true,
        null
      );
      log.message("Sending manager request to check bucket health; httpPath=" + httpPath);
      core.send(request);
      return Reactor.wrap(request, request.response(), true)
        .flatMap(response -> {
          if (response.status() != ResponseStatus.SUCCESS) {
            return Mono.error(new NotReadyYetException("Manager request to check bucket health failed with response status " + response.status() + ";" +
              " httpStatusCode=" + response.httpStatus() +
              ", responseBody=" + new String(response.content(), StandardCharsets.UTF_8) +
              ", requestContext=" + request.context()
            ));
          }

          ObjectNode root = (ObjectNode) Mapper.decodeIntoTree(response.content());
          ArrayNode nodes = (ArrayNode) root.get("nodes");

          long healthy = StreamSupport
            .stream(nodes.spliterator(), false)
            .filter(node -> node.get("status").asText().equals("healthy"))
            .count();

          if (nodes.size() != healthy) {
            return Mono.error(new NotReadyYetException(healthy + " of " + nodes.size() + " nodes are healthy"));
          }

          log.message("All " + healthy + " nodes are healthy");
          return Mono.empty();
        });
    });

    return retryUntilReady(
      retryWithMaxBackoff(Duration.ofSeconds(2)),
      "checking bucket health",
      log,
      stage
    );
  }

  public static CompletableFuture<Void> waitUntilReady(
    final Core core,
    @Nullable final Set<ServiceType> serviceTypes,
    final Duration timeout,
    final ClusterState desiredState,
    final Optional<String> bucketName
  ) {
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

    Deadline pingDeadline = Deadline.of(timeout, 0.9);

    WaitUntilReadyState state = new WaitUntilReadyState(log);

    AtomicReference<Set<ServiceType>> servicesToCheck = new AtomicReference<>(
      serviceTypes == null ? emptySet() : setCopyOf(serviceTypes)
    );

    Set<RequestTarget> remainingPingTargets = ConcurrentHashMap.newKeySet();

    return Mono.empty()
      .then(Mono.fromRunnable(() -> state.transition(WaitUntilReadyStage.WAIT_FOR_CONFIG)))
      .then(waitForConfig(core, bucketName.orElse(null), log))

      .then(Mono.fromRunnable(() -> state.transition(WaitUntilReadyStage.WAIT_FOR_HEALTHY_NODES)))
      .then(waitForNodeHealth(core, bucketName.orElse(null), log))

      .then(Mono.fromRunnable(() -> state.transition(WaitUntilReadyStage.WAIT_FOR_SUCCESSFUL_PING)))
      .then(waitForSuccessfulPing(core, bucketName.orElse(null), desiredState, servicesToCheck, pingDeadline, remainingPingTargets, log))

      .timeout(
        timeout,
        Mono.defer(() -> {
          log.message("WaitUntilReady timed out :-(");

          WaitUntilReadyContext waitUntilReadyContext = new WaitUntilReadyContext(
            servicesToCheck.get(),
            timeout,
            desiredState,
            bucketName,
            core.diagnostics().collect(Collectors.groupingBy(EndpointDiagnostics::type)),
            state,
            setCopyOf(remainingPingTargets)
          );
          CancellationErrorContext errorContext = new CancellationErrorContext(waitUntilReadyContext);
          return Mono.error(new UnambiguousTimeoutException(
            "WaitUntilReady timed out in stage " + state.currentStage
              + " (spent " + state.currentStart.elapsed() + " in that stage)", errorContext));
        }),
        core.context().environment().scheduler()
      )

      .doOnSuccess(completionReason -> {
        state.transition(WaitUntilReadyStage.COMPLETE);

        WaitUntilReadyContext waitUntilReadyContext = new WaitUntilReadyContext(
          servicesToCheck.get(),
          timeout,
          desiredState,
          bucketName,
          core.diagnostics().collect(Collectors.groupingBy(EndpointDiagnostics::type)),
          state,
          emptySet()
        );
        core.context().environment().eventBus().publish(
          new WaitUntilReadyCompletedEvent(waitUntilReadyContext, completionReason));
      })
      .then()
      .toFuture();
  }

  private static Mono<WaitUntilReadyCompletedEvent.Reason> waitForSuccessfulPing(
    Core core,
    @Nullable String bucketName,
    ClusterState desiredState,
    AtomicReference<Set<ServiceType>> serviceTypes,
    Deadline deadline,
    Set<RequestTarget> remainingPingTargets,
    WaitUntilReadyLogger log
  ) {
    return Mono.defer(() -> {
      // There could be a scenario where a user calls waitUntilReady on the cluster object despite
      // running a server cluster pre 6.5 (which does not support cluster-level config). Per definition,
      // we cannot make progress. So we let WaitUntilReady complete (so a user can move on to open a bucket)
      // but we'll print a warning to make sure we clarify the situation.
      //
      // Note that an explicit decision has been made to not fail fast and let it pass through because individual
      // operations will fail anyway if no further bucket is being opened and there is just nothing to "wait for"
      // to being ready at this point. Bucket level wait until ready is the way to go there.
      if (bucketName == null && !core.clusterConfig().hasClusterOrBucketConfig()) {
        log.message(
          "cluster.waitUntilReady() completed without action, because it was run against a Couchbase Server" +
            " version which does not support it (only supported with 6.5 and later)." +
            " Please open at least one bucket, and call bucket.waitUntilReady() instead."
        );

        return Mono.just(WaitUntilReadyCompletedEvent.Reason.CLUSTER_LEVEL_NOT_SUPPORTED);
      }

      // Start by finding all ping targets in the current cluster config.
      // Ignore any targets the user does not want to wait for.
      // Any nodes added to the cluster after this point are ignored.
      Optional<String> maybeBucketName = Optional.ofNullable(bucketName);
      Set<RequestTarget> initialPingTargets = setCopyOf(extractPingTargets(core.clusterConfig(), serviceTypes.get(), maybeBucketName, log));

      // Remember which targets we're still attempting to ping, so we can
      // include this info in the error message if WaitUntilReady times out.
      remainingPingTargets.addAll(initialPingTargets);

      // Similarly, the WaitUntilReady context wants to know which services we are
      // actually considering. This is how we pass that info back to the caller.
      serviceTypes.set(initialPingTargets.stream().map(RequestTarget::serviceType).collect(toSet()));

      // If the desired cluster state is "DEGRADED", we need to know when
      // we've successfully pinged at least one target for each service type.
      // At the start, mark all services as "offline".
      Set<ServiceType> offline = ConcurrentHashMap.newKeySet();
      offline.addAll(serviceTypes.get());

      // This flux completes successfully when we get a successful ping
      // for each target that's still part of the cluster (or, if the
      // desired state is "DEGRADED", when we get a successful ping
      // for each relevant service).
      return Flux.fromIterable(initialPingTargets)
        .flatMap(target -> {
            Mono<EndpointPingReport> ping = pingTarget(
              core,
              target,
              CoreCommonOptions.of(
                // Make at least one more attempt after the deadline (which is a percentage of total timeout).
                // The umbrella timeout for WaitUntilReady is enforced elsewhere.
                deadline.remaining().orElse(Duration.ofSeconds(10)),

                // Manual retry, so we can check the config between attempts.
                FailFastRetryStrategy.INSTANCE,

                // No parent span. Maybe one day.
                null
              ),
              log
            ).flatMap(report -> {
              if (report.state() == PingState.OK) {
                remainingPingTargets.remove(target);
                // Pass the report downstream in case we're aiming for DEGRADED.
                return Mono.just(report);
              }

              // Check whether the target is still part of the cluster. Don't log the
              // ping target extraction this time; that would be too noisy. (Note that
              // we _could_ try to look for a RequestCancelled exception with a reason
              // of TARGET_NODE_REMOVED, but that would couple us to the request dispatching
              // infrastructure, and it's easy enough to just scan the config).
              Set<RequestTarget> currentPingTargets = extractPingTargets(core.clusterConfig(), serviceTypes.get(), maybeBucketName, WaitUntilReadyLogger.dummy);
              if (!currentPingTargets.contains(target)) {
                log.message("Ignoring ping target " + target + " because it's no longer part of the cluster.");
                remainingPingTargets.remove(target);
                return Mono.empty();
              }

              return Mono.error(new NotReadyYetException("ping for target " + target + " failed with status: " + report.state()));
            });

            return retryUntilReady(
              retryWithMaxBackoff(Duration.ofSeconds(1)),
              "ping " + target,
              log,
              ping
            );
          }
        )
        .takeUntil(report -> {
          if (desiredState == ClusterState.ONLINE) {
            // take until all ping targets are either successful or removed from cluster
            return false;
          }

          // As each successful ping result is processed, remove the associated
          // service from the set. When the set is empty, we've achieved "DEGRADED".
          boolean firstSuccessfulPingForService = offline.remove(report.type());
          if (firstSuccessfulPingForService) {
            log.message("At least one " + report.type() + " ping was successful.");
          }

          if (offline.isEmpty()) {
            log.message("At least one ping was successful for each awaited service; desired cluster state 'DEGRADED' is now satisfied.");
            return true;
          }
          return false;
        })
        .then(Mono.just(WaitUntilReadyCompletedEvent.Reason.SUCCESS));
    });
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
      toExport.put("timings_ms", new TreeMap<>(timings));

      return toExport;
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
    WAIT_FOR_CONFIG,
    /**
     * Waits until all the nodes in a bucket config are healthy.
     */
    WAIT_FOR_HEALTHY_NODES,
    /**
     * Performs ping operations and checks their return values.
     */
    WAIT_FOR_SUCCESSFUL_PING,
    /**
     * Completed successfully.
     */
    COMPLETE,
  }

}
