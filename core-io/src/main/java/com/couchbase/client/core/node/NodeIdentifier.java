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
 */
public class NodeIdentifier {

  private final String canonicalHost;
  private final int canonicalManagerPort;

  // Nullable only when created by  a
  @Nullable private final String hostForNetworkConnections;

  @Deprecated
  public NodeIdentifier(
    final String canonicalHost,
    final int canonicalManagerPort
  ) {
    this.canonicalHost = canonicalHost;
    this.canonicalManagerPort = canonicalManagerPort;
    this.hostForNetworkConnections = null;
  }

  @Stability.Internal
  public static NodeIdentifier forBootstrap(String bootstrapHost, int bootstrapPort) {
    // This address isn't really "canonical", since it may be an "external" address.
    // If it's an external address, the node created from this identifier will be discarded
    // when the config with the _real_ canonical addresses is applied.
    return new NodeIdentifier(new HostAndPort(bootstrapHost, bootstrapPort), bootstrapHost);
  }

  public NodeIdentifier(
    HostAndPort canonicalAddress,
    String hostForNetworkConnections
  ) {
    this.canonicalHost = canonicalAddress.host();
    this.canonicalManagerPort = canonicalAddress.port();
    this.hostForNetworkConnections = requireNonNull(hostForNetworkConnections);
  }

  /**
   * Returns this node's host on the selected network.
   * <p>
   * If the default network was selected, this is the same as the canonical host.
   *
   * @throws NoSuchElementException if this info is not available
   */
  public String hostForNetworkConnections() throws NoSuchElementException {
    if (hostForNetworkConnections == null) {
      throw new NoSuchElementException(
        "This NodeIdentifier (" + this + ") doesn't have the host to use for network connections." +
          " It might have been created by a legacy config parser or some other component that did not specify it."
      );
    }
    return hostForNetworkConnections;
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
      ", hostForNetworkConnections=" + hostForNetworkConnections +
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
