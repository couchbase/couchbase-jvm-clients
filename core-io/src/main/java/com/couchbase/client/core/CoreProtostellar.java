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
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.cnc.ValueRecorder;
import com.couchbase.client.core.endpoint.ProtostellarEndpoint;
import com.couchbase.client.core.endpoint.ProtostellarPool;
import com.couchbase.client.core.env.SeedNode;
import com.couchbase.client.core.protostellar.ProtostellarContext;
import com.couchbase.client.core.util.HostAndPort;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.couchbase.client.core.util.Validators.notNullOrEmpty;
import static java.util.Objects.requireNonNull;

@Stability.Internal
public class CoreProtostellar {
  public static final int DEFAULT_PROTOSTELLAR_TLS_PORT = 18098;

  private final ProtostellarPool pool;
  private final ProtostellarContext ctx;

  public CoreProtostellar(ProtostellarContext ctx, final Set<SeedNode> seedNodes) {
    this.ctx = requireNonNull(ctx);
    notNullOrEmpty(seedNodes, "seed nodes");

    SeedNode first = seedNodes.iterator().next();
    int port = first.protostellarPort().orElse(DEFAULT_PROTOSTELLAR_TLS_PORT);
    HostAndPort remote = new HostAndPort(first.address(), port);

    this.pool = new ProtostellarPool(ctx, remote);
  }

  public ProtostellarContext context() {
    return ctx;
  }

  public void shutdown(final Duration timeout) {
    pool.shutdown(timeout);
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
}
