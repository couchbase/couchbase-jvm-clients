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
import com.couchbase.client.core.env.SeedNode;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.util.HostAndPort;
import reactor.util.annotation.Nullable;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;

import static com.couchbase.client.core.logging.RedactableArgument.redactSystem;
import static com.couchbase.client.core.util.CbCollections.newEnumMap;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

/**
 * Used for locating the services running on a node.
 * <p>
 * Consists of a host (hostname or IP literal) and a map from service to port number.
 * <p>
 * The ports are either all TLS ports, or all non-TLS ports, depending on
 * the {@link PortSelector} used by the config parser.
 */
@Stability.Internal
public class HostAndServicePorts implements KetamaRingNode {
  // Placeholder for a node that can't be reached because it doesn't have an alternate address
  // for the requested network. (Can't just ignore it, because bucket config refers to nodes by index.)
  public static final HostAndServicePorts INACCESSIBLE = new HostAndServicePorts(
    "<inaccessible>",
    emptyMap(),
    new NodeIdentifier("<inaccessible>", 0, "<inaccessible>"),
    null,
    null
  );

  private final String host;
  private final Map<ServiceType, Integer> ports;
  private final NodeIdentifier id;
  @Nullable private final HostAndPort ketamaAuthority;
  @Nullable private final String serverGroup;

  public HostAndServicePorts(
    String host,
    Map<ServiceType, Integer> ports,
    NodeIdentifier id,
    @Nullable HostAndPort ketamaAuthority,
    @Nullable String serverGroup
  ) {
    this.host = requireNonNull(host);
    this.ports = unmodifiableMap(newEnumMap(ServiceType.class, ports));
    this.id = requireNonNull(id);
    this.ketamaAuthority = ketamaAuthority;
    this.serverGroup = serverGroup;
  }

  public boolean inaccessible() {
    return this == INACCESSIBLE;
  }

  public NodeIdentifier id() {
    return id;
  }

  public String host() {
    return host;
  }

  /**
   * Returns the host and non-TLS KV port from the "default" network.
   * <p>
   * Used with Memcached buckets to determine which document IDs
   * this node is responsible for.
   * <p>
   * If the node has no non-TLS KV port, then this method returns
   * null, and the node cannot participate in a ketama ring.
   */
  @Nullable
  public HostAndPort ketamaAuthority() {
    return ketamaAuthority;
  }

  public OptionalInt port(ServiceType serviceType) {
    Integer port = ports.get(serviceType);
    return port == null ? OptionalInt.empty() : OptionalInt.of(port);
  }

  public Map<ServiceType, Integer> ports() {
    return ports;
  }

  public @Nullable String serverGroup() {
    return serverGroup;
  }

  public boolean has(ServiceType serviceType) {
    return ports.containsKey(serviceType);
  }

  @Stability.Internal
  public HostAndServicePorts without(ServiceType service, ServiceType... moreServices) {
    if (!has(service) && Arrays.stream(moreServices).noneMatch(this::has)) {
      return this;
    }

    EnumMap<ServiceType, Integer> temp = newEnumMap(ServiceType.class, ports());
    temp.remove(service);
    for (ServiceType t : moreServices) {
      temp.remove(t);
    }

    return new HostAndServicePorts(this.host, temp, this.id, this.ketamaAuthority, this.serverGroup);
  }

  @Stability.Internal
  public HostAndServicePorts withKetamaAuthority(@Nullable HostAndPort ketamaAuthority) {
    if (Objects.equals(this.ketamaAuthority, ketamaAuthority)) {
      return this;
    }
    return new HostAndServicePorts(this.host, this.ports, this.id, ketamaAuthority, this.serverGroup);
  }

  boolean matches(SeedNode seedNode) {
    return this.host.equals(seedNode.address()) &&
      (portEquals(ServiceType.KV, seedNode.kvPort().orElse(0)) ||
        portEquals(ServiceType.MANAGER, seedNode.clusterManagerPort().orElse(0)));
  }

  private boolean portEquals(ServiceType serviceType, int port) {
    int actualPort = port(serviceType).orElse(0);
    return actualPort != 0 && actualPort == port;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    HostAndServicePorts that = (HostAndServicePorts) o;
    return host.equals(that.host) && ports.equals(that.ports) && Objects.equals(ketamaAuthority, that.ketamaAuthority);
  }

  @Override
  public int hashCode() {
    return Objects.hash(host, ports);
  }

  @Override
  public String toString() {
    return "HostAndServicePorts{" +
      "host='" + redactSystem(host) + '\'' +
      ", ports=" + redactSystem(ports) +
      ", id=" + redactSystem(id) +
      ", ketamaAuthority=" + redactSystem(ketamaAuthority) +
      '}';
  }

}
