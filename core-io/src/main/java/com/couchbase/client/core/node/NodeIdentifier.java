/*
 * Copyright (c) 2019 Couchbase, Inc.
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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.util.HostAndPort;
import reactor.util.annotation.Nullable;

import java.util.NoSuchElementException;
import java.util.Objects;

import static com.couchbase.client.core.logging.RedactableArgument.redactSystem;
import static java.util.Objects.requireNonNull;

/**
 * Uniquely identifies a node within the cluster, using the node's
 * host and manager port from the default network.
 *
 * @deprecated In favor of {@link com.couchbase.client.core.topology.NodeIdentifier}
 */
@Deprecated
public class NodeIdentifier {

  private final String canonicalHost;
  private final int canonicalManagerPort;

  // Null only when created by a legacy global/bucket config parser
  @Nullable private final com.couchbase.client.core.topology.NodeIdentifier topologyNodeIdentifier;

  @Deprecated
  public NodeIdentifier(
    final String canonicalHost,
    final int canonicalManagerPort
  ) {
    this.canonicalHost = canonicalHost;
    this.canonicalManagerPort = canonicalManagerPort;
    this.topologyNodeIdentifier = null;
  }

  public NodeIdentifier(
    HostAndPort canonicalAddress,
    String hostForNetworkConnections
  ) {
    this.canonicalHost = canonicalAddress.host();
    this.canonicalManagerPort = canonicalAddress.port();
    this.topologyNodeIdentifier = new com.couchbase.client.core.topology.NodeIdentifier(canonicalHost, canonicalManagerPort, hostForNetworkConnections);
  }

  /**
   * Returns this node's host on the selected network.
   * <p>
   * If the default network was selected, this is the same as the canonical host.
   *
   * @throws NoSuchElementException if this info is not available
   */
  public String hostForNetworkConnections() throws NoSuchElementException {
    return asTopologyNodeIdentifier().hostForNetworkConnections();
  }

  @Stability.Internal
  public com.couchbase.client.core.topology.NodeIdentifier asTopologyNodeIdentifier() {
    if (topologyNodeIdentifier == null) {
      throw new NoSuchElementException("This NodeIdentifier (" + this + ") doesn't have the host to use for network connections." +
        " It might have been created by a legacy config parser or some other component that did not specify it."
      );
    }
    return topologyNodeIdentifier;
  }

  /**
   * Returns the node's host on the default network.
   */
  public String address() {
    return canonicalHost;
  }

  public int managerPort() {
    return canonicalManagerPort;
  }

  @Override
  public String toString() {
    return "NodeIdentifier{" +
      "canonicalAddress=" + redactSystem(canonicalHost + ":" + canonicalManagerPort) +
      ", hostForNetworkConnections=" + (topologyNodeIdentifier == null ? null : hostForNetworkConnections()) +
      "}";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    NodeIdentifier that = (NodeIdentifier) o;
    return canonicalManagerPort == that.canonicalManagerPort && Objects.equals(canonicalHost, that.canonicalHost);
  }

  @Override
  public int hashCode() {
    return Objects.hash(canonicalHost, canonicalManagerPort);
  }

}
