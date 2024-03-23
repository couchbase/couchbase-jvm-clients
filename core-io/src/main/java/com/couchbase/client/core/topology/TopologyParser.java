/*
 * Copyright 2024 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.topology;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.node.MemcachedHashingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.couchbase.client.core.logging.RedactableArgument.redactSystem;
import static java.util.Objects.requireNonNull;

@Stability.Internal
public class TopologyParser {
  private static final Logger log = LoggerFactory.getLogger(TopologyParser.class);

  private final NetworkSelector networkSelector;
  private final PortSelector portSelector;
  private final MemcachedHashingStrategy memcachedHashingStrategy;

  public TopologyParser(
    NetworkSelector networkSelector,
    PortSelector portSelector,
    MemcachedHashingStrategy memcachedHashingStrategy
  ) {
    this.networkSelector = requireNonNull(networkSelector);
    this.portSelector = requireNonNull(portSelector);
    this.memcachedHashingStrategy = requireNonNull(memcachedHashingStrategy);
  }

  public ClusterTopology parse(String json, String originHost) {
    return parse(
      JacksonHelper.readObject(json),
      originHost
    );
  }

  public ClusterTopology parse(ObjectNode json, String originHost) {
    log.debug("Parsing topology JSON from origin '{}' : {}", redactSystem(originHost), redactSystem(json));

    ClusterTopology result = ClusterTopologyParser.parse(
      json,
      originHost,
      portSelector,
      networkSelector,
      memcachedHashingStrategy
    );

    log.debug("Parsed topology: {}", redactSystem(result));
    return result;
  }
}
