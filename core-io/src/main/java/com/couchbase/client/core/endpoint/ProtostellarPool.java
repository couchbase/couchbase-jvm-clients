/*
 * Copyright (c) 2023 Couchbase, Inc.
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

package com.couchbase.client.core.endpoint;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.protostellar.ProtostellarContext;
import com.couchbase.client.core.util.HostAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Collections.unmodifiableList;

/**
 * Maintains a pool of ProtostellarEndpoints.
 */
@Stability.Internal
public class ProtostellarPool {
  private static final Logger logger = LoggerFactory.getLogger(ProtostellarPool.class);

  private final List<ProtostellarEndpoint> endpoints;
  private final AtomicLong lastUsed = new AtomicLong(0);

  public ProtostellarPool(ProtostellarContext ctx, HostAndPort remote) {
    // JVMCBC-1196: assuming ProtostellarPool is kept, add configuration options for it.
    int numEndpoints = Integer.parseInt(System.getProperty("com.couchbase.protostellar.numEndpoints", "3"));
    logger.info("creating with endpoints {}", numEndpoints);
    List<ProtostellarEndpoint> endpoints = new ArrayList<>(numEndpoints);
    for (int i = 0; i < numEndpoints; i++) {
      endpoints.add(new ProtostellarEndpoint(ctx, remote));
    }
    this.endpoints = unmodifiableList(endpoints);
  }

  public void shutdown(Duration timeout) {
    endpoints.forEach(endpoint -> endpoint.shutdown(timeout));
  }

  public ProtostellarEndpoint endpoint() {
    // Just using a basic round-robin strategy for now
    int index = (int) ((lastUsed.getAndIncrement() & 0x7fffffffffffffffL) % endpoints.size());
    // logger.info("Using endpoint {}", index);
    return endpoints.get(index);
  }

  public List<ProtostellarEndpoint> endpoints() {
    return endpoints;
  }
}
