/*
 * Copyright (c) 2017 Couchbase, Inc.
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

package com.couchbase.client.core.diag;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.Reactor;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.error.RequestTimeoutException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.msg.diagnostics.PingKVRequest;
import com.couchbase.client.core.msg.diagnostics.PingRequest;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.util.HostAndPort;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * The {@link HealthPinger} allows to "ping" individual services with
 * real operations for their health.
 * <p>
 * This can be used by up the stack code to assert the given state of
 * a connected cluster and/or bucket.
 *
 * @author Michael Nitschinger
 * @since 1.5.4
 */
public class HealthPinger {

  /**
   * Performs a service ping against all or (if given) the services provided.
   * <p>
   * First, all the services are contacted with:
   * <p>
   * - KV - NOOP
   * - N1QL - GET /admin/ping (expect 200 OK)
   * - CBFT - GET /api/ping (expect 200 OK)
   * - Views - GET / (expect 200 OK)
   * - Analytics - GET /admin/ping (expect 200 OK)
   * <p>
   * Afterwards, the responses are assembled into a {@link PingResult} and returned.
   *
   * @param env      the environment to use.
   * @param bucket   the bucket name
   * @param core     the core instance against to check.
   * @param id       the id of the ping to use, can be null.
   * @param timeout  the timeout for each individual and total ping report.
   * @param types    if present, limits the queried services for the given types.
   * @return
   */
  public static Mono<PingResult> ping(
          final CoreEnvironment env,
          final String bucket,
          final Core core,
          final String id,
          final Duration timeout,
          final RetryStrategy retryStrategy,
          final List<ServiceType> types) {

    List<Mono<PingServiceHealth>> services = new ArrayList<>();

    core.clusterConfig()
            .bucketConfigs()
            .values()
            .stream()
            .filter(v -> v.name().equals(bucket))
            .forEach(bucketConfig -> {
              bucketConfig
                      .nodes()
                      .forEach(node -> {
                        node.services()
                                .keySet()
                                .stream()
                                .filter(serviceType -> types == null || types.contains(serviceType))
                                .forEach(serviceType -> {
                                  switch (serviceType) {
                                    case KV:
                                      services.add(pingKV(bucket, core, timeout, retryStrategy));
                                      break;
                                    case VIEWS:
                                      services.add(pingViews(bucket, core, timeout, retryStrategy));
                                      break;
                                    case SEARCH:
                                      services.add(pingSearch(bucket, core, timeout, retryStrategy));
                                      break;
                                    case QUERY:
                                      services.add(pingQuery(bucket, core, timeout, retryStrategy));
                                      break;
                                    case ANALYTICS:
                                      services.add(pingAnalytics(bucket, core, timeout, retryStrategy));
                                      break;
                                  }
                                });
                      });
            });

    return Flux.merge(services)
            .collectList()
            .map(results -> {
              // rev is a hard-coded 0 right now, to be fixed under JCBC-1468
              return new PingResult(results, env.userAgent().formattedShort(), id, 0);
            });
  }

  public static Mono<PingServiceHealth> pingQuery(
          final String bucket,
          final Core core,
          final Duration timeout,
          final RetryStrategy retryStrategy) {
    return pingGeneric(bucket, core, timeout, retryStrategy, "/admin/ping", ServiceType.QUERY);
  }

  public static Mono<PingServiceHealth> pingSearch(
          final String bucket,
          final Core core,
          final Duration timeout,
          final RetryStrategy retryStrategy) {
    return pingGeneric(bucket, core, timeout, retryStrategy, "/api/ping", ServiceType.SEARCH);
  }

  public static Mono<PingServiceHealth> pingViews(
          final String bucket,
          final Core core,
          final Duration timeout,
          final RetryStrategy retryStrategy) {
    return pingGeneric(bucket, core, timeout, retryStrategy, "/", ServiceType.VIEWS);
  }

  /**
   * Pings the service and completes if successful - and fails if it didn't work
   * for some reason (reason is in the exception).
   */
  public static Mono<PingServiceHealth> pingAnalytics(
          final String bucket,
          final Core core,
          final Duration timeout,
          final RetryStrategy retryStrategy) {
    return pingGeneric(bucket, core, timeout, retryStrategy, "/admin/ping", ServiceType.ANALYTICS);
  }

  public static Mono<PingServiceHealth> pingGeneric(
          final String bucket,
          final Core core,
          final Duration timeout,
          final RetryStrategy retryStrategy,
          final String path,
          final ServiceType type) {

    final long creationTime = System.nanoTime();

    PingRequest req = new PingRequest(timeout,
            core.context(),
            retryStrategy,
            bucket,
            path,
            type);

    final String id = "0x" + Integer.toHexString(req.hashCode());
    core.send(req);
    return Reactor.wrap(req, req.response(), true)

            .map(response -> {
              return new PingServiceHealth(
                      type,
                      PingServiceHealth.PingState.OK,
                      id,
                      TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - creationTime),
                      req.context().lastDispatchedFrom().toString(),
                      req.context().lastDispatchedTo().toString(),
                      bucket);
            })

            .onErrorResume(err -> {
              PingServiceHealth.PingState state = PingServiceHealth.PingState.ERROR;

              if (err instanceof RequestTimeoutException) {
                state = PingServiceHealth.PingState.TIMEOUT;
              }

              return Mono.just(new PingServiceHealth(
                      type,
                      state,
                      id,
                      TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - creationTime),
                      req.context().lastDispatchedFrom().toString(),
                      req.context().lastDispatchedTo().toString(),
                      bucket));
            });
  }

  public static Mono<PingServiceHealth> pingKV(
          final String bucket,
          final Core core,
          final Duration timeout,
          final RetryStrategy retryStrategy) {

    final long creationTime = System.nanoTime();

    PingKVRequest req = new PingKVRequest(timeout,
            core.context(),
            retryStrategy,
            CollectionIdentifier.fromDefault(bucket));

    String id = "0x" + Integer.toHexString(req.hashCode());
    core.send(req);
    return Reactor.wrap(req, req.response(), true)

            .map(response -> {
              return new PingServiceHealth(
                      ServiceType.KV,
                      PingServiceHealth.PingState.OK,
                      id,
                      TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - creationTime),
                      req.context().lastDispatchedFrom().toString(),
                      req.context().lastDispatchedTo().toString(),
                      bucket
              );
            })

            .onErrorResume(err -> {
              PingServiceHealth.PingState state = PingServiceHealth.PingState.ERROR;

              if (err instanceof RequestTimeoutException) {
                state = PingServiceHealth.PingState.TIMEOUT;
              }

              HostAndPort lastDispatchedTo = req.context().lastDispatchedTo();
              HostAndPort lastDispatchedFrom = req.context().lastDispatchedFrom();
              return Mono.just(new PingServiceHealth(
                ServiceType.KV,
                state,
                id,
                TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - creationTime),
                lastDispatchedFrom == null ? null : lastDispatchedFrom.toString(),
                lastDispatchedTo == null ? null : lastDispatchedTo.toString(),
                bucket
              ));
            });
  }

}
