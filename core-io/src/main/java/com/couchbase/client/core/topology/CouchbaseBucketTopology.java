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
import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

@Stability.Internal
public class CouchbaseBucketTopology extends AbstractBucketTopology {
  static final int PARTITION_NOT_EXISTENT = -2;

  private final boolean ephemeral;
  private final int replicas;
  private final PartitionMap partitions;
  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private final Optional<PartitionMap> partitionsForward;
  private final Set<NodeIdentifier> primaryPartitionNodeIds;

  public CouchbaseBucketTopology(
    String name,
    String uuid,
    Set<BucketCapability> capabilities,
    List<HostAndServicePorts> nodes,
    boolean ephemeral,
    int replicas,
    PartitionMap partitions,
    @Nullable PartitionMap partitionsForward
  ) {
    super(name, uuid, capabilities, nodes);
    this.replicas = replicas;
    this.partitions = requireNonNull(partitions);
    this.partitionsForward = Optional.ofNullable(partitionsForward);
    this.ephemeral = ephemeral;

    this.primaryPartitionNodeIds = unmodifiableSet(
      partitions.values().stream()
        .map(it -> it.active().map(HostAndServicePorts::id).orElse(null))
        .filter(Objects::nonNull)
        .collect(toSet())
    );
  }

  public boolean ephemeral() {
    return ephemeral;
  }

  public int numberOfPartitions() {
    return partitions.size();
  }

  public int numberOfReplicas() {
    return replicas;
  }

  public PartitionMap partitions() {
    return partitions;
  }

  public Optional<PartitionMap> partitionsForward() {
    return partitionsForward;
  }

  private PartitionMap partitions(boolean forward) {
    return forward
      ? partitionsForward().orElseThrow(() -> new IllegalStateException("Config has no forward partition map."))
      : partitions();
  }

  public boolean hasPrimaryPartitionsOnNode(final NodeIdentifier id) {
    return primaryPartitionNodeIds.contains(id);
  }

  @Deprecated // temporary bridge?
  public int nodeIndexForActive(int partition, boolean forward) {
    return partitions(forward)
      .get(partition)
      .nodeIndexForActive()
      .orElse(PARTITION_NOT_EXISTENT);
  }

  @Deprecated // temporary bridge?
  public int nodeIndexForReplica(int partition, int replica, boolean forward) {
    return partitions(forward)
      .get(partition)
      .nodeIndexForReplica(replica)
      .orElse(PARTITION_NOT_EXISTENT);
  }

  @Override
  public String toString() {
    return "CouchbaseBucketTopology{" +
      "name='" + redactMeta(name()) + '\'' +
      ", uuid='" + uuid() + '\'' +
      ", ephemeral=" + ephemeral +
      ", capabilities=" + capabilities() +
      ", replicas=" + replicas +
      ", partitions=" + partitions +
      ", partitionsForward=" + partitionsForward +
      '}';
  }
}
