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
import org.jspecify.annotations.Nullable;

import static java.util.Objects.requireNonNull;

@Stability.Internal
public class NodeIdentifier {
  private final HostAndPort canonical;
  private final String hostForNetworkConnections;
  private final String uuid;

  /**
   * Creates an instance with a synthetic UUID derived from the canonical host and port.
   */
  public NodeIdentifier(
    String canonicalHost,
    int canonicalPort,
    String hostForNetworkConnections
  ) {
    this(canonicalHost, canonicalPort, hostForNetworkConnections, null);
  }

  /**
   * @param uuid The node's UUID, or null to derive a synthetic UUID from the canonical host and port.
   */
  public NodeIdentifier(
    String canonicalHost,
    int canonicalPort,
    String hostForNetworkConnections,
    @Nullable String uuid
  ) {
    this(new HostAndPort(canonicalHost, canonicalPort), hostForNetworkConnections, uuid);
  }

  /**
   * @param uuid The node's UUID, or null to derive a synthetic UUID from the canonical host and port.
   */
  public NodeIdentifier(
    HostAndPort canonical,
    String hostForNetworkConnections,
    @Nullable String uuid
  ) {
    this.canonical = requireNonNull(canonical);
    this.hostForNetworkConnections = requireNonNull(hostForNetworkConnections);
    this.uuid = uuid == null
      ? canonical().format() // Fall back to manager host:port
      : uuid; // Couchbase Server 8 and later!
  }

  public static NodeIdentifier forBootstrap(String bootstrapHost, int bootstrapPort) {
    // This address isn't really "canonical", since it may be an "external" address.
    // If it's an external address, the node created from this identifier will be discarded
    // when the config with the _real_ canonical addresses is applied.
    return new NodeIdentifier(new HostAndPort(bootstrapHost, bootstrapPort), bootstrapHost, null);
  }

  /**
   * Returns an opaque string that uniquely identifies this node within the cluster.
   */
  public String uuid() {
    return uuid;
  }

  /**
   * Returns manager host:port on the default network.
   * <p>
   * The SDK should not attempt to connect to this address, because it might not be accessible on the client's network.
   * If you're looking for an address to connect to, see {@link #hostForNetworkConnections}.
   */
  public HostAndPort canonical() {
    return canonical;
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
    return canonical.hashCode();
  }

  @Override
  public String toString() {
    return "NodeIdentifier{" +
      "canonical=" + canonical +
      ", hostForNetworkConnections='" + hostForNetworkConnections + '\'' +
      ", uuid='" + uuid + '\'' +
      '}';
  }
}
