/*
 * Copyright (c) 2018 Couchbase, Inc.
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

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.InternalSpan;
import com.couchbase.client.core.diagnostics.ClusterState;
import com.couchbase.client.core.diagnostics.DiagnosticsResult;
import com.couchbase.client.core.diagnostics.EndpointDiagnostics;
import com.couchbase.client.core.diagnostics.HealthPinger;
import com.couchbase.client.core.diagnostics.PingResult;
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.error.ReducedViewErrorContext;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.msg.view.ViewRequest;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.diagnostics.DiagnosticsOptions;
import com.couchbase.client.java.diagnostics.PingOptions;
import com.couchbase.client.java.diagnostics.WaitUntilReadyOptions;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.manager.collection.AsyncCollectionManager;
import com.couchbase.client.java.manager.view.AsyncViewIndexManager;
import com.couchbase.client.java.view.ViewAccessor;
import com.couchbase.client.java.view.ViewOptions;
import com.couchbase.client.java.view.ViewResult;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;
import static com.couchbase.client.java.ReactiveBucket.DEFAULT_VIEW_OPTIONS;
import static com.couchbase.client.java.ReactiveCluster.DEFAULT_PING_OPTIONS;
import static com.couchbase.client.java.ReactiveCluster.DEFAULT_WAIT_UNTIL_READY_OPTIONS;
import static com.couchbase.client.java.diagnostics.DiagnosticsOptions.diagnosticsOptions;
import static reactor.core.publisher.Mono.fromFuture;

/**
 * Provides access to a Couchbase bucket in an async fashion.
 */
public class AsyncBucket {

  /**
   * The name of the bucket.
   */
  private final String name;

  /**
   * The underlying attached environment.
   */
  private final ClusterEnvironment environment;

  /**
   * The core reference used.
   */
  private final Core core;

  private final AsyncCollectionManager collectionManager;

  private final AsyncViewIndexManager viewManager;

  private final Authenticator authenticator;

  /**
   * Creates a new {@link AsyncBucket}.
   *
   * @param name the name of the bucket.
   * @param core the underlying core.
   * @param environment the attached environment.
   */
  AsyncBucket(final String name, final Core core, final ClusterEnvironment environment) {
    this.core = core;
    this.environment = environment;
    this.name = name;
    this.collectionManager = new AsyncCollectionManager(core, name);
    this.viewManager = new AsyncViewIndexManager(core, name);
    this.authenticator = core.context().authenticator();
  }

  /**
   * Returns the name of the {@link AsyncBucket}.
   */
  public String name() {
    return name;
  }

  /**
   * Returns the attached {@link ClusterEnvironment}.
   */
  public ClusterEnvironment environment() {
    return environment;
  }

  /**
   * Provides access to the underlying {@link Core}.
   *
   * <p>This is advanced API, use with care!</p>
   */
  @Stability.Volatile
  public Core core() {
    return core;
  }

  @Stability.Volatile
  public AsyncCollectionManager collections() {
    return collectionManager;
  }

  @Stability.Volatile
  public AsyncViewIndexManager viewIndexes() {
    return viewManager;
  }

  /**
   * Opens the {@link AsyncScope} with the given name.
   *
   * @param name the name of the scope.
   * @return the {@link AsyncScope} once opened.
   */
  @Stability.Volatile
  public AsyncScope scope(final String name) {
    return new AsyncScope(name, this.name, core, environment);
  }

  /**
   * Opens the default {@link AsyncScope}.
   *
   * @return the {@link AsyncScope} once opened.
   */
  @Stability.Volatile
  public AsyncScope defaultScope() {
    return new AsyncScope(CollectionIdentifier.DEFAULT_SCOPE, name, core, environment);
  }

  /**
   * Opens the default collection for this {@link AsyncBucket}.
   *
   * @return the {@link AsyncCollection} once opened.
   */
  public AsyncCollection defaultCollection() {
    return defaultScope().defaultCollection();
  }

  /**
   * Opens the collection with the given name for this {@link AsyncBucket}.
   *
   * @return the {@link AsyncCollection} once opened.
   */
  @Stability.Volatile
  public AsyncCollection collection(final String collectionName) {
    return defaultScope().collection(collectionName);
  }

  public CompletableFuture<ViewResult> viewQuery(final String designDoc, final String viewName) {
    return viewQuery(designDoc, viewName, DEFAULT_VIEW_OPTIONS);
  }

  public CompletableFuture<ViewResult> viewQuery(final String designDoc, final String viewName, final ViewOptions options) {
    notNull(options, "ViewOptions", () -> new ReducedViewErrorContext(designDoc, viewName, name));
    ViewOptions.Built opts = options.build();
    JsonSerializer serializer = opts.serializer() == null ? environment.jsonSerializer() : opts.serializer();
    return ViewAccessor.viewQueryAsync(core, viewRequest(designDoc, viewName, opts), serializer);
  }

  ViewRequest viewRequest(final String designDoc, final String viewName, final ViewOptions.Built opts) {
    notNullOrEmpty(designDoc, "DesignDoc", () -> new ReducedViewErrorContext(designDoc, viewName, name));
    notNullOrEmpty(viewName, "ViewName", () -> new ReducedViewErrorContext(designDoc, viewName, name));

    String query = opts.query();
    Optional<byte[]> keysJson = Optional.ofNullable(opts.keys()).map(s -> s.getBytes(StandardCharsets.UTF_8));
    boolean development = opts.development();

    Duration timeout = opts.timeout().orElse(environment.timeoutConfig().analyticsTimeout());
    RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment.retryStrategy());

    final InternalSpan span = environment()
      .requestTracer()
      .internalSpan(ViewRequest.OPERATION_NAME, opts.parentSpan().orElse(null));
    ViewRequest request = new ViewRequest(timeout, core.context(), retryStrategy, authenticator, name, designDoc,
      viewName, query, keysJson, development, span);
    request.context().clientContext(opts.clientContext());
    return request;
  }


  /**
   * Performs application-level ping requests against services in the couchbase cluster.
   *
   * @return the {@link PingResult} once complete.
   */
  public CompletableFuture<PingResult> ping() {
    return ping(DEFAULT_PING_OPTIONS);
  }

  /**
   * Performs application-level ping requests with custom options against services in the couchbase cluster.
   *
   * @return the {@link PingResult} once complete.
   */
  public CompletableFuture<PingResult> ping(final PingOptions options) {
    notNull(options, "PingOptions");
    final PingOptions.Built opts = options.build();
    return HealthPinger.ping(
      core,
      opts.timeout(),
      opts.retryStrategy().orElse(environment.retryStrategy()),
      opts.serviceTypes(),
      opts.reportId(),
      false
    ).toFuture();
  }

  /**
   * Waits until the desired {@link ClusterState} is reached.
   * <p>
   * This method will wait until either the cluster state is "online", or the timeout is reached. Since the SDK is
   * bootstrapping lazily, this method allows to eagerly check during bootstrap if all of the services are online
   * and usable before moving on.
   *
   * @param timeout the maximum time to wait until readiness.
   * @return a completable future that completes either once ready or timeout.
   */
  public CompletableFuture<Void> waitUntilReady(final Duration timeout) {
    return waitUntilReady(timeout, DEFAULT_WAIT_UNTIL_READY_OPTIONS);
  }

  /**
   * Waits until the desired {@link ClusterState} is reached.
   * <p>
   * This method will wait until either the cluster state is "online" by default, or the timeout is reached. Since the
   * SDK is bootstrapping lazily, this method allows to eagerly check during bootstrap if all of the services are online
   * and usable before moving on. You can tune the properties through {@link WaitUntilReadyOptions}.
   *
   * @param timeout the maximum time to wait until readiness.
   * @param options the options to customize the readiness waiting.
   * @return a completable future that completes either once ready or timeout.
   */
  public CompletableFuture<Void> waitUntilReady(final Duration timeout, final WaitUntilReadyOptions options) {
    notNull(options, "WaitUntilReadyOptions");
    final WaitUntilReadyOptions.Built opts = options.build();

    return Flux
      .interval(Duration.ofMillis(10))
      .filter(i -> !(core.configurationProvider().bucketConfigLoadInProgress()
        || core.configurationProvider().globalConfigLoadInProgress()))
      .take(1)
      .flatMap(aLong -> {
        final Set<ServiceType> servicesToCheck = opts.serviceTypes() != null && !opts.serviceTypes().isEmpty()
          ? opts.serviceTypes()
          : HealthPinger
          .extractPingTargets(core.clusterConfig(), false)
          .stream()
          .map(HealthPinger.PingTarget::serviceType)
          .collect(Collectors.toSet());

        final Flux<PingResult> ping = Mono
          .fromFuture(ping(PingOptions.pingOptions().serviceTypes(servicesToCheck)))
          .flux();

        final Flux<DiagnosticsResult> diagnostics = Flux
          .interval(Duration.ofMillis(10))
          .flatMap(i -> fromFuture(diagnostics()))
          .takeUntil(d -> d.state() == opts.desiredState());

        return Flux.concat(ping, diagnostics);
      })
      .timeout(timeout)
      .then()
      .toFuture();
  }

  private CompletableFuture<DiagnosticsResult> diagnostics() {
    return Mono.defer(() -> Mono.just(new DiagnosticsResult(
      core.diagnostics().collect(Collectors.groupingBy(EndpointDiagnostics::type)),
      core.context().environment().userAgent().formattedShort(),
      UUID.randomUUID().toString()
    ))).toFuture();
  }

}
