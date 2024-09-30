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

package com.couchbase.client.core;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.CoreCouchbaseOps;
import com.couchbase.client.core.api.kv.CoreKvBinaryOps;
import com.couchbase.client.core.api.kv.CoreKvOps;
import com.couchbase.client.core.api.manager.CoreBucketAndScope;
import com.couchbase.client.core.api.manager.search.CoreSearchIndexManager;
import com.couchbase.client.core.api.query.CoreQueryOps;
import com.couchbase.client.core.api.search.CoreSearchOps;
import com.couchbase.client.core.cnc.Context;
import com.couchbase.client.core.cnc.Event;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.cnc.ValueRecorder;
import com.couchbase.client.core.cnc.events.core.ShutdownCompletedEvent;
import com.couchbase.client.core.cnc.events.core.ShutdownInitiatedEvent;
import com.couchbase.client.core.diagnostics.ClusterState;
import com.couchbase.client.core.endpoint.ProtostellarEndpoint;
import com.couchbase.client.core.endpoint.ProtostellarPool;
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.manager.CoreBucketManagerOps;
import com.couchbase.client.core.manager.CoreCollectionManager;
import com.couchbase.client.core.protostellar.ProtostellarContext;
import com.couchbase.client.core.protostellar.kv.ProtostellarCoreKvBinaryOps;
import com.couchbase.client.core.protostellar.kv.ProtostellarCoreKvOps;
import com.couchbase.client.core.protostellar.manager.ProtostellarCoreBucketManager;
import com.couchbase.client.core.protostellar.manager.ProtostellarCoreCollectionManagerOps;
import com.couchbase.client.core.protostellar.manager.ProtostellarCoreSearchIndexManager;
import com.couchbase.client.core.protostellar.query.ProtostellarCoreQueryOps;
import com.couchbase.client.core.protostellar.search.ProtostellarCoreSearchOps;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.util.ConnectionString;
import com.couchbase.client.core.util.Deadline;
import com.couchbase.client.core.util.HostAndPort;
import com.couchbase.client.core.util.NanoTimestamp;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static com.couchbase.client.core.api.CoreCouchbaseOps.checkConnectionStringScheme;
import static com.couchbase.client.core.logging.RedactableArgument.redactSystem;
import static com.couchbase.client.core.util.CbCollections.mapOf;
import static com.couchbase.client.core.util.Validators.notNull;

@Stability.Internal
public class CoreProtostellar implements CoreCouchbaseOps {
  public static final int DEFAULT_PROTOSTELLAR_TLS_PORT = 18098;

  private final ProtostellarPool pool;
  private final ProtostellarContext ctx;

  public CoreProtostellar(
    final CoreEnvironment env,
    final Authenticator authenticator,
    final ConnectionString connectionString
  ) {
    CoreResources coreResources = () -> env.requestTracer();
    this.ctx = new ProtostellarContext(env, authenticator, coreResources);
    notNull(connectionString, "connectionString");

    checkConnectionStringScheme(connectionString, ConnectionString.Scheme.COUCHBASE2);

    if (connectionString.hosts().size() != 1) {
      throw InvalidArgumentException.fromMessage(
        "Connection string with scheme '" + ConnectionString.Scheme.COUCHBASE2.name().toLowerCase(Locale.ROOT) +
          "' must have exactly one host, but got: " + connectionString.original()
      );
    }

    ConnectionString.UnresolvedSocket first = connectionString.hosts().get(0);
    first.portType().ifPresent(type -> {
      throw InvalidArgumentException.fromMessage(
        "Invalid port type for scheme " + connectionString.scheme() + ": " + type + " ; " + connectionString.original()
      );
    });

    int port = first.port() == 0 ? DEFAULT_PROTOSTELLAR_TLS_PORT : first.port();
    HostAndPort remote = new HostAndPort(first.host(), port);

    // After argument validation, before allocating resources.
    CoreLimiter.incrementAndVerifyNumInstances(env.eventBus());

    this.pool = new ProtostellarPool(ctx, remote);

    logCoreCreatedEvent(connectionString);
  }

  private void logCoreCreatedEvent(ConnectionString connectionString) {
    LoggerFactory.getLogger(Event.Category.CORE.path()).info(
      "[CoreCreatedEvent] {} {}",
      Mapper.encodeAsString(mapOf(
        "coreId", ctx.hexId(),
        "connectionString", redactSystem(connectionString.original())
      )),
      environment().exportAsString(Context.ExportFormat.JSON)
    );
  }

  public ProtostellarContext context() {
    return ctx;
  }

  @Override
  public Mono<Void> shutdown(final Duration timeout) {
    // This will block, locking up a scheduler thread - but since all we're interested in doing is shutting down, that doesn't matter.
    return Mono.fromRunnable(() -> {
      NanoTimestamp start = NanoTimestamp.now();
      try {
        environment().eventBus().publish(new ShutdownInitiatedEvent(ctx));
        pool.shutdown(timeout);
      } finally {
        CoreLimiter.decrement();
        environment().eventBus().publish(new ShutdownCompletedEvent(start.elapsed(), ctx));
      }
    });
  }

  public ProtostellarEndpoint endpoint() {
    return pool.endpoint();
  }

  public ProtostellarPool pool() {
    return pool;
  }

  private final Map<Core.ResponseMetricIdentifier, ValueRecorder> responseMetrics = new ConcurrentHashMap<>();

  @Stability.Internal
  public ValueRecorder responseMetric(final Core.ResponseMetricIdentifier rmi) {
    return responseMetrics.computeIfAbsent(rmi, key -> {
      Map<String, String> tags = new HashMap<>(4);
      tags.put(TracingIdentifiers.ATTR_SERVICE, key.serviceType());
      tags.put(TracingIdentifiers.ATTR_OPERATION, key.requestName());
      return ctx.environment().meter().valueRecorder(TracingIdentifiers.METER_OPERATIONS, tags);
    });
  }

  @Override
  public CoreKvOps kvOps(CoreKeyspace keyspace) {
    return new ProtostellarCoreKvOps(this, keyspace);
  }

  @Override
  public CoreQueryOps queryOps() {
    return new ProtostellarCoreQueryOps(this);
  }

  @Override
  public CoreSearchOps searchOps(@Nullable CoreBucketAndScope scope) {
    return new ProtostellarCoreSearchOps(this, scope);
  }

  @Override
  public CoreKvBinaryOps kvBinaryOps(CoreKeyspace keyspace) {
    return new ProtostellarCoreKvBinaryOps(this, keyspace);
  }

  @Override
  public CoreBucketManagerOps bucketManager() {
    return new ProtostellarCoreBucketManager(this);
  }

  @Override
  public CoreCollectionManager collectionManager(String bucketName) {
    return new ProtostellarCoreCollectionManagerOps(this, bucketName);
  }

  @Override
  public CoreSearchIndexManager clusterSearchIndexManager() {
    return new ProtostellarCoreSearchIndexManager(this, null);
  }

  @Override
  public CoreSearchIndexManager scopeSearchIndexManager(CoreBucketAndScope scope) {
    return new ProtostellarCoreSearchIndexManager(this, scope);
  }

  @Override
  public CoreEnvironment environment() {
    return context().environment();
  }

  @Override
  public CoreResources coreResources() {
    return context().coreResources();
  }

  @Override
  public CompletableFuture<Void> waitUntilReady(
    Set<ServiceType> serviceTypes,
    Duration timeout,
    ClusterState desiredState,
    @Nullable String bucketName
  ) {
    Deadline deadline = Deadline.of(timeout);
    List<CompletableFuture<Void>> cfs = new ArrayList<>();
    pool().endpoints().forEach(endpoint -> cfs.add(endpoint.waitUntilReady(deadline, desiredState != ClusterState.OFFLINE)));
    if (desiredState == ClusterState.DEGRADED) {
      return CompletableFuture.anyOf(cfs.toArray(new CompletableFuture[0])).thenRun(() -> {
      });
    }
    return CompletableFuture.allOf(cfs.toArray(new CompletableFuture[0]));
  }
}
