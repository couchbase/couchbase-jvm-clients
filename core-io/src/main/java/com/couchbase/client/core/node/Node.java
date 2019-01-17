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
import com.couchbase.client.core.cnc.Event;
import com.couchbase.client.core.cnc.events.service.ServiceAddIgnoredEvent;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.Credentials;
import com.couchbase.client.core.io.NetworkAddress;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.msg.ScopedRequest;
import com.couchbase.client.core.service.AnalyticsService;
import com.couchbase.client.core.service.KeyValueService;
import com.couchbase.client.core.service.ManagerService;
import com.couchbase.client.core.service.QueryService;
import com.couchbase.client.core.service.SearchService;
import com.couchbase.client.core.service.Service;
import com.couchbase.client.core.service.ServiceScope;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.service.ViewService;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class Node {

  /**
   * Identifier for global scope services, there is no bucket name like this.
   */
  private static final String GLOBAL_SCOPE = "_$GLOBAL$_";

  private final NetworkAddress address;
  private final CoreContext ctx;
  private final Credentials credentials;

  private final Map<String, Map<ServiceType, Service>> services;

  private final AtomicBoolean disconnect;

  /**
   * Contains the enabled {@link Service}s on a node level.
   */
  private volatile int enabledServices;

  public static Node create(final CoreContext ctx, final NetworkAddress address,
                            final Credentials credentials) {
    return new Node(ctx, address, credentials);
  }

  private Node(final CoreContext ctx, final NetworkAddress address, final Credentials credentials) {
    this.address = address;
    this.ctx = ctx;
    this.credentials = credentials;
    this.services = new ConcurrentHashMap<>();
    this.disconnect = new AtomicBoolean(false);
  }

  /**
   * Instruct this {@link Node} to connect.
   *
   * <p>This method is async and will return immediately. Use the other methods available to
   * inspect the current state of the node, signaling potential successful connection
   * attempts.</p>
   */
  public void connect() {

  }

  /**
   * Instruct this {@link Node} to disconnect.
   *
   * <p>This method is async and will return immediately. Use the other methods available to
   * inspect the current state of the node, signaling potential successful disconnection
   * attempts.</p>
   */
  public void disconnect() {
    if (disconnect.compareAndSet(false, true)) {
      // handle disconnect
    }
  }

  /**
   * Adds a {@link Service} to this {@link Node}.
   *
   * @param type the type of the service.
   * @param port the port of the service.
   * @param bucket the bucket name (if present).
   * @return a {@link Mono} that completes once the service is added.
   */
  public synchronized Mono<Void> addService(final ServiceType type, final int port,
                                            final Optional<String> bucket) {
    return Mono.defer(() -> {
      if (disconnect.get()) {
        ctx.environment().eventBus().publish(new ServiceAddIgnoredEvent(
          Event.Severity.DEBUG,
          ServiceAddIgnoredEvent.Reason.DISCONNECTED,
          ctx
        ));
        return Mono.empty();
      }

      String name = type.scope() == ServiceScope.CLUSTER ? GLOBAL_SCOPE : bucket.get();
      Map<ServiceType, Service> localMap = services.get(name);
      if (localMap == null) {
        localMap = new ConcurrentHashMap<>();
        services.put(name, localMap);
      }
      if (!localMap.containsKey(type)) {
        Service service = createService(type, port, bucket);
        localMap.put(type, service);
        enabledServices |= 1 << type.ordinal();
        service.connect();
        // todo: only return once the service is connected?
        return Mono.empty();
      } else {
        ctx.environment().eventBus().publish(new ServiceAddIgnoredEvent(
          Event.Severity.VERBOSE,
          ServiceAddIgnoredEvent.Reason.ALREADY_ADDED,
          ctx
        ));
        return Mono.empty();
      }
    });
  }

  public void removeService() {
    // todo: don't forget enabledServices &= ~(1 << service.type().ordinal());
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
    String bucket = request.serviceType().scope() == ServiceScope.BUCKET
      ? ((ScopedRequest) request).bucket()
      : GLOBAL_SCOPE;
    Service service = services.get(bucket).get(request.serviceType());
    service.send(request);
  }

  public NodeState state() {
    // TODO: fixme
    return NodeState.CONNECTED;
  }

  public NetworkAddress address() {
    return address;
  }

  public boolean serviceEnabled(final ServiceType type) {
    return (enabledServices & (1 << type.ordinal())) != 0;
  }

  protected Service createService(final ServiceType serviceType, final int port,
                                  final Optional<String> bucket) {

    CoreEnvironment env = ctx.environment();
    switch (serviceType) {
      case KV:
        return new KeyValueService(env.keyValueServiceConfig(), ctx, address, port,
          bucket.get(), credentials);
      case MANAGER:
        return new ManagerService(ctx, address, port);
      case QUERY:
        return new QueryService(env.queryServiceConfig(), ctx, address, port);
      case VIEWS:
        return new ViewService(env.viewServiceConfig(), ctx, address, port);
      case SEARCH:
        return new SearchService(env.searchServiceConfig(), ctx, address, port);
      case ANALYTICS:
        return new AnalyticsService(env.analyticsServiceConfig(), ctx, address, port);
      default:
        throw new IllegalArgumentException("Unsupported ServiceType: " + serviceType);
    }
  }
}
