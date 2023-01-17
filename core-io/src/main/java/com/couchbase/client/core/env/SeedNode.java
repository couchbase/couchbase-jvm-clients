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

package com.couchbase.client.core.env;

import com.couchbase.client.core.annotation.Stability;
import reactor.util.annotation.Nullable;

import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;

/**
 * The {@link SeedNode} represents a combination of hostname/ip and port that is used during the SDK bootstrap.
 * <p>
 * Note that this class is used mostly internally but can be used during bootstrap to override the default ports on
 * a per-node basis. Most of the time you want to use the connection string bootstrap instead.
 */
public class SeedNode {

  /**
   * Seed node set pointing to localhost with default ports.
   */
  public static final Set<SeedNode> LOCALHOST = Collections.singleton(SeedNode.create("127.0.0.1"));

  /**
   * The hostname or IP used.
   */
  private final String address;

  /**
   * If present, an alternate KV port.
   */
  private final Optional<Integer> kvPort;

  /**
   * If present, an alternate HTTP ("cluster manager") port.
   */
  private final Optional<Integer> clusterManagerPort;


  /**
   * If present, an alternate Protostellar port.
   */
  private final Optional<Integer> protostellarPort;

  /**
   * Creates a seed node from a hostname and the default ports.
   *
   * @param address the hostname or IP of the seed node.
   * @return the created {@link SeedNode}.
   */
  public static SeedNode create(final String address) {
    return create(address, Optional.empty(), Optional.empty());
  }

  /**
   * Creates a seed node from a hostname and custom ports.
   *
   * @param address the hostname or IP of the seed node.
   * @return the created {@link SeedNode}.
   */
  public static SeedNode create(final String address, final Optional<Integer> kvPort,
                                final Optional<Integer> clusterManagerPort) {
    return new SeedNode(address, kvPort, clusterManagerPort, Optional.empty());
  }

  private SeedNode(final String address,
                   final Optional<Integer> kvPort,
                   final Optional<Integer> clusterManagerPort,
                   final Optional<Integer> protostellarPort) {
    this.address = notNullOrEmpty(address, "Address");
    this.kvPort = notNull(kvPort, "KvPort");
    this.clusterManagerPort = notNull(clusterManagerPort, "ClusterManagerPort");
    this.protostellarPort = notNull(protostellarPort, "ProtostellarPort");
  }

  /**
   * Returns a copy of this seed node, with the given KV port.
   *
   * @param port (nullable) null means absent KV port.
   */
  public SeedNode withKvPort(@Nullable Integer port) {
    return SeedNode.create(address(), Optional.ofNullable(port), clusterManagerPort());
  }

  /**
   * Returns a copy of this seed node, with the given Manager port.
   *
   * @param port (nullable) null means absent Manager port.
   */
  public SeedNode withManagerPort(@Nullable Integer port) {
    return SeedNode.create(address(), kvPort(), Optional.ofNullable(port));
  }

  /**
   * Returns a copy of this seed node, with the given Protostellar port.
   *
   * @param port (nullable) null means absent Protostellar port.
   */
  @Stability.Volatile
  public SeedNode withProtostellarPort(@Nullable Integer port) {
    return new SeedNode(address(), kvPort(), clusterManagerPort(), Optional.ofNullable(port));
  }

  /**
   * The ip address or hostname of this seed node.
   */
  public String address() {
    return address;
  }

  /**
   * If present, the kv port.
   */
  public Optional<Integer> kvPort() {
    return kvPort;
  }

  /**
   * If present, the cluster manager port.
   */
  public Optional<Integer> clusterManagerPort() {
    return clusterManagerPort;
  }

  /**
   * If present, the Protostellar port.
   */
  @Stability.Volatile
  public Optional<Integer> protostellarPort() {
    return protostellarPort;
  }

  @Override
  public String toString() {
    return "SeedNode{" +
      "address='" + address + '\'' +
      ", kvPort=" + kvPort +
      ", mgmtPort=" + clusterManagerPort +
      ", psPort=" + protostellarPort +
      '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SeedNode seedNode = (SeedNode) o;
    return Objects.equals(address, seedNode.address) &&
      Objects.equals(kvPort, seedNode.kvPort) &&
      Objects.equals(clusterManagerPort, seedNode.clusterManagerPort) &&
      Objects.equals(protostellarPort, seedNode.protostellarPort);
  }

  @Override
  public int hashCode() {
    return Objects.hash(address, kvPort, clusterManagerPort, protostellarPort);
  }

}
