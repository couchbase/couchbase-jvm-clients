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

package com.couchbase.client.core.node;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.Event;
import com.couchbase.client.core.cnc.events.node.NodeConnectedEvent;
import com.couchbase.client.core.cnc.events.node.NodeCreatedEvent;
import com.couchbase.client.core.cnc.events.node.NodeDisconnectIgnoredEvent;
import com.couchbase.client.core.cnc.events.node.NodeDisconnectedEvent;
import com.couchbase.client.core.cnc.events.node.NodeStateChangedEvent;
import com.couchbase.client.core.cnc.events.service.ServiceAddIgnoredEvent;
import com.couchbase.client.core.cnc.events.service.ServiceAddedEvent;
import com.couchbase.client.core.cnc.events.service.ServiceRemoveIgnoredEvent;
import com.couchbase.client.core.cnc.events.service.ServiceRemovedEvent;
import com.couchbase.client.core.diagnostics.EndpointDiagnostics;
import com.couchbase.client.core.diagnostics.InternalEndpointDiagnostics;
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.retry.RetryOrchestrator;
import com.couchbase.client.core.retry.RetryReason;
import com.couchbase.client.core.service.AnalyticsService;
import com.couchbase.client.core.service.AnalyticsServiceConfig;
import com.couchbase.client.core.service.BackupService;
import com.couchbase.client.core.service.EventingService;
import com.couchbase.client.core.service.KeyValueService;
import com.couchbase.client.core.service.KeyValueServiceConfig;
import com.couchbase.client.core.service.ManagerService;
import com.couchbase.client.core.service.QueryService;
import com.couchbase.client.core.service.QueryServiceConfig;
import com.couchbase.client.core.service.SearchService;
import com.couchbase.client.core.service.SearchServiceConfig;
import com.couchbase.client.core.service.Service;
import com.couchbase.client.core.service.ServiceScope;
import com.couchbase.client.core.service.ServiceState;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.service.ViewService;
import com.couchbase.client.core.service.ViewServiceConfig;
import com.couchbase.client.core.util.CompositeStateful;
import com.couchbase.client.core.util.HostAndPort;
import com.couchbase.client.core.util.NanoTimestamp;
import com.couchbase.client.core.util.AtomicEnumSet;
import com.couchbase.client.core.util.Stateful;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static com.couchbase.client.core.logging.RedactableArgument.redactSystem;

public class Node implements Stateful<NodeState> {

  /**
   * Identifier for global scope services, there is no bucket name like this.
   */
  private static final String GLOBAL_SCOPE = "_$GLOBAL$_";

  /**
   * When a bucket-scoped request/service is used but it is not tied to a specific bucket yet.
   *
   * <p>Yep this is weird, it is basically used for "gcccp" requests, so config requests when they
   * are not tied to an actual bucket name.</p>
   */
  private static final String BUCKET_GLOBAL_SCOPE = "_$BUCKET_GLOBAL$_";

  private final NodeIdentifier identifier;
  private final NodeContext ctx;
  private final Authenticator authenticator;
  private final Map<String, Map<ServiceType, Service>> services;
  private final AtomicBoolean disconnect;

  /**
   * Holds the endpoint states and as a result the internal service state.
   */
  private final CompositeStateful<Service, ServiceState, NodeState> serviceStates;

  /**
   * Contains the enabled {@link Service}s on a node level.
   */
  private final AtomicEnumSet<ServiceType> enabledServices = AtomicEnumSet.noneOf(ServiceType.class);

  public static Node create(final CoreContext ctx, final NodeIdentifier identifier) {
    return new Node(ctx, identifier);
  }

  protected Node(final CoreContext ctx, final NodeIdentifier identifier) {
    this.identifier = identifier;
    this.ctx = new NodeContext(ctx, identifier);
    this.authenticator = ctx.authenticator();
    this.services = new ConcurrentHashMap<>();
    this.disconnect = new AtomicBoolean(false);
    this.serviceStates = CompositeStateful.create(NodeState.DISCONNECTED, serviceStates -> {
      if (serviceStates.isEmpty()) {
        return NodeState.DISCONNECTED;
      }

      int connected = 0;
      int connecting = 0;
      int disconnecting = 0;
      int idle = 0;
      int degraded = 0;
      for (ServiceState service : serviceStates) {
        switch (service) {
          case CONNECTED:
            connected++;
            break;
          case CONNECTING:
            connecting++;
            break;
          case DISCONNECTING:
            disconnecting++;
            break;
          case DEGRADED:
            degraded++;
            break;
          case IDLE:
            idle++;
            break;
          case DISCONNECTED:
            // Intentionally ignored.
            break;
          default:
            throw InvalidArgumentException.fromMessage("Unknown unhandled state " + service
              + ", this is a bug!");
        }
      }
      if (serviceStates.size() == idle) {
        return NodeState.IDLE;
      } else if (serviceStates.size() == (connected + idle)) {
        return NodeState.CONNECTED;
      } else if (connected > 0 || degraded > 0) {
        return NodeState.DEGRADED;
      } else if (connecting > 0) {
        return NodeState.CONNECTING;
      } else if (disconnecting > 0) {
        return NodeState.DISCONNECTING;
      } else {
        return NodeState.DISCONNECTED;
      }
    },
      (from, to) -> ctx.environment().eventBus().publish(new NodeStateChangedEvent(this.ctx, from, to))
    );

    ctx.environment().eventBus().publish(new NodeCreatedEvent(Duration.ZERO, this.ctx));
    // Also publish the deprecated version with the misleading name, in case a user is listening for it.
    //noinspection deprecation
    ctx.environment().eventBus().publish(new NodeConnectedEvent(Duration.ZERO, this.ctx));
  }

  /**
   * Instruct this {@link Node} to disconnect.
   *
   * <p>This method is async and will return immediately. Use the other methods available to
   * inspect the current state of the node, signaling potential successful disconnection
   * attempts.</p>
   */
  public synchronized Mono<Void> disconnect() {
    return Mono.defer(() -> {
      if (disconnect.compareAndSet(false, true)) {
        final AtomicLong start = new AtomicLong();
        return Flux
          .fromIterable(services.entrySet())
          .flatMap(entry -> {
            start.set(System.nanoTime());
            return Flux
              .fromIterable(entry.getValue().keySet())
              .flatMap(serviceType ->
                Mono.fromRunnable(() -> removeService(serviceType, Optional.of(entry.getKey()), true))
              );
          })
          .then()
          .doOnTerminate(() ->
            ctx.environment().eventBus().publish(new NodeDisconnectedEvent(
              Duration.ofNanos(System.nanoTime() - start.get()),
              ctx
            ))
          );
      } else {
        ctx.environment().eventBus().publish(new NodeDisconnectIgnoredEvent(
          Event.Severity.DEBUG,
          NodeDisconnectIgnoredEvent.Reason.DISCONNECTED,
          ctx
        ));
      }
      return Mono.empty();
    });
  }

  /**
   * Adds a {@link Service} to this {@link Node}.
   *
   * @param type the type of the service.
   * @param port the port of the service.
   * @param bucket the bucket name (if present).
   * @return a {@link Mono} that completes once the service is added.
   */
  public Mono<Void> addService(
    final ServiceType type,
    final int port,
    final Optional<String> bucket
  ) {
    return Mono.fromRunnable(() -> {
      if (disconnect.get()) {
        ctx.environment().eventBus().publish(new ServiceAddIgnoredEvent(
          Event.Severity.DEBUG,
          ServiceAddIgnoredEvent.Reason.DISCONNECTED,
          ctx
        ));
        return;
      }

      HostAndPort newServiceAddress = new HostAndPort(
        identifier.address(),
        port
      );

      String name = type.scope() == ServiceScope.CLUSTER ? GLOBAL_SCOPE : bucket.orElse(BUCKET_GLOBAL_SCOPE);
      Map<ServiceType, Service> localMap = services.computeIfAbsent(name, key -> new ConcurrentHashMap<>());

      Service existingService = localMap.get(type);
      if (existingService != null && !existingService.address().equals(newServiceAddress)) {
        // Service moved to a different port. Remove the old one!
        removeService(type, bucket, true);
      }

      if (localMap.containsKey(type)) {
        ctx.environment().eventBus().publish(new ServiceAddIgnoredEvent(
          Event.Severity.VERBOSE,
          ServiceAddIgnoredEvent.Reason.ALREADY_ADDED,
          ctx
        ));
        return;
      }

      NanoTimestamp start = NanoTimestamp.now();
      Service service = createService(type, newServiceAddress, bucket);
      serviceStates.register(service, service);
      localMap.put(type, service);
      enabledServices.add(type);
      // todo: only return once the service is connected?
      service.connect();
      ctx.environment().eventBus().publish(
        new ServiceAddedEvent(start.elapsed(), service.context())
      );
    });
  }

  /**
   * Removes a {@link Service} from this {@link Node}.
   *
   * @param type the type of service.
   * @param bucket the bucket name if present.
   * @return a mono once completed.
   */
  public Mono<Void> removeService(final ServiceType type, final Optional<String> bucket) {
    return Mono.fromRunnable(() -> removeService(type, bucket, false));
  }

  private void removeService(
    final ServiceType type,
    final Optional<String> bucket,
    final boolean ignoreDisconnect
  ) {
      if (disconnect.get() && !ignoreDisconnect) {
        ctx.environment().eventBus().publish(new ServiceRemoveIgnoredEvent(
          Event.Severity.DEBUG,
          ServiceRemoveIgnoredEvent.Reason.DISCONNECTED,
          ctx
        ));
        return;
      }

      String name = type.scope() == ServiceScope.CLUSTER ? GLOBAL_SCOPE : bucket.orElse(BUCKET_GLOBAL_SCOPE);
      Map<ServiceType, Service> localMap = services.get(name);
      if (localMap == null || !localMap.containsKey(type)) {
        ctx.environment().eventBus().publish(new ServiceRemoveIgnoredEvent(
          Event.Severity.DEBUG,
          ServiceRemoveIgnoredEvent.Reason.NOT_PRESENT,
          ctx
        ));
        return;
      }

      Service service = localMap.remove(type);
      serviceStates.deregister(service);
      long start = System.nanoTime();
      if (serviceCanBeDisabled(service.type())) {
        enabledServices.remove(service.type());
      }
      // todo: only return once the service is disconnected?
      service.disconnect();
      long end = System.nanoTime();
      ctx.environment().eventBus().publish(
        new ServiceRemovedEvent(Duration.ofNanos(end - start), service.context())
      );
  }

  /**
   * Checks if a service can be removed from the {@link #enabledServices} cache.
   * <p>
   * Note that th check is considerably simple: we iterate through all th services and if the same
   * service is present somewhere else (the precondition of this method is that the service in question
   * has already been removed), it cannot be disabled.
   *
   * @param serviceType the service to verify.
   * @return true if it can be removed, false otherwise.
   */
  private boolean serviceCanBeDisabled(final ServiceType serviceType) {
    return services.values().stream().noneMatch(m -> m.containsKey(serviceType));
  }

  @Override
  public Flux<NodeState> states() {
    return serviceStates.states();
  }

  @Override
  public NodeState state() {
    return serviceStates.state();
  }

  /**
   * If present, returns a flux that allows to monitor the state changes of a specific service.
   *
   * @param type the type of service.
   * @param bucket the bucket, if present.
   * @return if found, a flux with the service states.
   */
  public Optional<Flux<ServiceState>> serviceState(final ServiceType type, final Optional<String> bucket) {
    String name = type.scope() == ServiceScope.CLUSTER ? GLOBAL_SCOPE : bucket.orElse(BUCKET_GLOBAL_SCOPE);
    Map<ServiceType, Service> s = services.get(name);
    if (s == null) {
      return Optional.empty();
    }
    return Optional.ofNullable(s.get(type)).map(Stateful::states);
  }

  /**
   * Sends the request into this {@link Node}.
   *
   * <p>Note that there is no guarantee that the request will actually dispatched, based on the
   * state this node is in.</p>
   *
   * @param request the request to send.
   */
  public <R extends Request<? extends Response>> void send(final R request) {
    String bucket;
    if (request.serviceType().scope() == ServiceScope.BUCKET) {
      bucket = request.bucket();
      if (bucket == null) {
        // no bucket name present so this is a kv request that is not attached
        // to a specific bucket
        bucket = BUCKET_GLOBAL_SCOPE;
      }
     } else {
      bucket = GLOBAL_SCOPE;
    }

    Map<ServiceType, Service> scope = services.get(bucket);
    if (scope == null) {
      sendIntoRetry(request);
      return;
    }

    Service service = scope.get(request.serviceType());
    if (service == null) {
      sendIntoRetry(request);
      return;
    }

    request.context().lastDispatchedToNode(identifier);
    service.send(request);
  }

  /**
   * Retries the request.
   *
   * <p>This is a separate method because in test it is overridden to do easy assertions.</p>
   *
   * @param request the request to retry.
   */
  protected <R extends Request<? extends Response>> void sendIntoRetry(final R request) {
    RetryOrchestrator.maybeRetry(ctx, request, RetryReason.SERVICE_NOT_AVAILABLE);
  }

  /**
   * Returns the node identifier.
   */
  public NodeIdentifier identifier() {
    return identifier;
  }

  /**
   * If a given {@link ServiceType} is enabled on this node.
   *
   * @param type the service type to check.
   * @return true if enabled, false otherwise.
   */
  public boolean serviceEnabled(final ServiceType type) {
    return enabledServices.contains(type);
  }

  public boolean hasServicesEnabled() {
    return !enabledServices.isEmpty();
  }

  /**
   * Helper method to create the {@link Service} based on the service type provided.
   *
   * @param serviceType the type of service to create.
   * @param address the host and port for that service.
   * @param bucket optionally the bucket name.
   * @return a created service, but not yet connected or anything.
   */
  protected Service createService(final ServiceType serviceType, final HostAndPort address,
                                  final Optional<String> bucket) {
    CoreEnvironment env = ctx.environment();

    String host = address.host();
    int port = address.port();

    switch (serviceType) {
      case KV:
        return new KeyValueService(
          KeyValueServiceConfig.endpoints(env.ioConfig().numKvConnections()).build(), ctx, host, port, bucket, authenticator);
      case MANAGER:
        return new ManagerService(ctx, host, port);
      case QUERY:
        return new QueryService(QueryServiceConfig
          .maxEndpoints(env.ioConfig().maxHttpConnections())
          .idleTime(env.ioConfig().idleHttpConnectionTimeout())
          .build(),
          ctx, host, port
        );
      case VIEWS:
        return new ViewService(ViewServiceConfig
          .maxEndpoints(env.ioConfig().maxHttpConnections())
          .idleTime(env.ioConfig().idleHttpConnectionTimeout())
          .build(),
          ctx, host, port);
      case SEARCH:
        return new SearchService(SearchServiceConfig
          .maxEndpoints(env.ioConfig().maxHttpConnections())
          .idleTime(env.ioConfig().idleHttpConnectionTimeout())
          .build(),
          ctx, host, port);
      case ANALYTICS:
        return new AnalyticsService(AnalyticsServiceConfig
          .maxEndpoints(env.ioConfig().maxHttpConnections())
          .idleTime(env.ioConfig().idleHttpConnectionTimeout())
          .build(),
          ctx, host, port);
      case EVENTING:
        return new EventingService(ctx, host, port);
      case BACKUP:
        return new BackupService(ctx, host, port);
      default:
        throw InvalidArgumentException.fromMessage("Unsupported ServiceType: " + serviceType);
    }
  }

  public Stream<EndpointDiagnostics> diagnostics() {
    return services.values()
            .stream()
            .flatMap(services -> services.values().stream())
            .flatMap(service -> service.diagnostics());
  }

  @Stability.Internal
  public Stream<InternalEndpointDiagnostics> internalDiagnostics() {
    return services.values()
      .stream()
      .flatMap(services -> services.values().stream())
      .flatMap(service -> service.internalDiagnostics());
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Node node = (Node) o;
    return Objects.equals(identifier, node.identifier);
  }

  @Override
  public int hashCode() {
    return Objects.hash(identifier);
  }

  @Override
  public String toString() {
    return "Node{" +
      "identifier=" + redactSystem(identifier) +
      ", ctx=" + ctx +
      ", services=" + services +
      ", disconnect=" + disconnect +
      ", serviceStates=" + serviceStates +
      ", enabledServices=" + enabledServices +
      '}';
  }
}
