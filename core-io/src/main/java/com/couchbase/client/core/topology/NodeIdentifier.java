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
import com.couchbase.client.core.util.HostAndPort;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

@Stability.Internal
public class NodeIdentifier {
  private final HostAndPort canonical; // manager host:port on default network
  private final String hostForNetworkConnections;

  public NodeIdentifier(String host, int port, String hostForNetworkConnections) {
    this(new HostAndPort(host, port), hostForNetworkConnections);
  }

  public NodeIdentifier(HostAndPort canonical, String hostForNetworkConnections) {
    this.canonical = requireNonNull(canonical);
    this.hostForNetworkConnections = requireNonNull(hostForNetworkConnections);
  }

  @Deprecated
  public com.couchbase.client.core.node.NodeIdentifier toLegacy() {
    return new com.couchbase.client.core.node.NodeIdentifier(canonical, hostForNetworkConnections);
  }

  public String hostForNetworkConnections() {
    return hostForNetworkConnections;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    NodeIdentifier that = (NodeIdentifier) o;
    return canonical.equals(that.canonical);
  }

  @Override
  public int hashCode() {
    return Objects.hash(canonical);
  }

  @Override
  public String toString() {
    return "NodeID{" +
      "canonical=" + canonical +
      ", hostForNetworkConnections='" + hostForNetworkConnections + '\'' +
      '}';
  }
}
