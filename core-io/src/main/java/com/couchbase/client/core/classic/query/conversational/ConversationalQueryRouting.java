/*
 * Copyright (c) 2026 Couchbase, Inc.
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

package com.couchbase.client.core.classic.query.conversational;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.ChatCurrentlyUnavailableException;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.FeatureNotAvailableException;
import com.couchbase.client.core.error.ServiceNotAvailableException;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.topology.ClusterCapability;
import com.couchbase.client.core.topology.ClusterTopology;
import com.couchbase.client.core.topology.HostAndServicePorts;
import com.couchbase.client.core.util.HostAndPort;
import reactor.util.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Selects the query node that a conversational query chat operation is sent to.
 */
@Stability.Internal
class ConversationalQueryRouting {
  private ConversationalQueryRouting() {
  }

  // Used for BEGIN.
  static HostAndServicePorts selectChatNode(ClusterTopology topology) {
    return selectFrom(chatNodes(topology));
  }

  // Used for ASK, PAUSE and END, which must go to the retained owner.
  static HostAndServicePorts resolveOwner(ClusterTopology topology,
                                          String chatId,
                                          @Nullable String owner,
                                          String operation) {
    List<HostAndServicePorts> candidates = chatNodes(topology);
    if (owner == null) {
      // Deliberate RFC divergence: with no retained owner this is a usage error, not ChatCurrentlyUnavailableException.
      throw new CouchbaseException("Cannot " + operation + " chat " + chatId + " as routing information is not available.  One possible cause: if an operation other than resume is being performed on a chat that has been paused.");
    }
    return findOwner(candidates, owner)
        .orElseThrow(() -> new ChatCurrentlyUnavailableException(
            "The query node for this chat is no longer in the cluster", null));
  }

  // Used for RESUME, which per RFC goes to the retained owner if it is resolvable, else selects a node as for BEGIN.
  static HostAndServicePorts resolveOwnerOrSelectChatNode(ClusterTopology topology, @Nullable String owner) {
    List<HostAndServicePorts> candidates = chatNodes(topology);
    if (owner != null) {
      Optional<HostAndServicePorts> found = findOwner(candidates, owner);
      if (found.isPresent()) {
        return found.get();
      }
    }
    return selectFrom(candidates);
  }

  private static Optional<HostAndServicePorts> findOwner(List<HostAndServicePorts> candidates, String owner) {
    return candidates.stream()
        .filter(node -> owner.equals(node.uuid()))
        .findFirst();
  }

  private static HostAndServicePorts selectFrom(List<HostAndServicePorts> candidates) {
    if (candidates.isEmpty()) {
      throw new ServiceNotAvailableException("No query node is currently available for a conversational query chat", null);
    }

    // Randomly choose an available node where there's a choice.
    return candidates.get(ThreadLocalRandom.current().nextInt(candidates.size()));
  }

  // The query nodes a chat can be routed to.  The checks here take precedence over any owner resolution.
  // Includes a check that this is a topology that query service supports for this feature - in particular
  // this excludes Capella Private Links.
  private static List<HostAndServicePorts> chatNodes(ClusterTopology topology) {
    if (!topology.hasCapability(ClusterCapability.N1QL_CONVERSATIONAL_QUERY)) {
      throw new FeatureNotAvailableException("This cluster does not support conversational query chats");
    }

    List<HostAndServicePorts> candidates = new ArrayList<>();
    Map<HostAndPort, String> uuidsByAddress = new HashMap<>();
    for (HostAndServicePorts node : topology.nodes()) {
      if (node.inaccessible() || !node.has(ServiceType.QUERY) || node.uuid() == null || node.uuid().isEmpty()) {
        continue;
      }
      HostAndPort address = new HostAndPort(node.host(), node.port(ServiceType.QUERY).getAsInt());
      String existing = uuidsByAddress.putIfAbsent(address, node.uuid());
      if (existing != null && !existing.equals(node.uuid())) {
        throw new FeatureNotAvailableException("Conversational query chats are not supported on cluster topologies in which multiple query nodes share the same port");
      }
      candidates.add(node);
    }
    return candidates;
  }
}
