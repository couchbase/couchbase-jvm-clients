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
import reactor.util.annotation.Nullable;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.couchbase.client.core.config.CouchbaseBucketConfig.PARTITION_NOT_EXISTENT;
import static com.couchbase.client.core.util.CbCollections.copyToUnmodifiableList;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

/**
 * Knows which nodes host the active and replicas for one partition.
 */
@Stability.Internal
public class PartitionTopology {
  static final PartitionTopology ABSENT = new PartitionTopology(null, emptyList(), emptyList());

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private final Optional<HostAndServicePorts> active;
  private final List<HostAndServicePorts> availableReplicas;

  /**
   * The node indexes for this partition, just as they appear
   * in the corresponding element of the bucket config's "vBucketMap" array.
   * <p>
   * Zero-th element is the index of the node hosting the active partition.
   * Remaining elements are the indexes of nodes hosting the replicas.
   * <p>
   * A negative element indicates the corresponding active/replica is
   * currently unavailable.
   * <p>
   * CAVEAT: The length of this list is NOT guaranteed to equal (numReplicas + 1).
   * To reproduce the mismatch, use Couchbase Server 7.1.0 to create a bucket.
   * Then edit the bucket and set the requested number of replicas to more than
   * the cluster can support.
   */
  private final List<Integer> rawNodeIndexes;

  public PartitionTopology(@Nullable HostAndServicePorts active, List<HostAndServicePorts> availableReplicas, List<Integer> rawNodeIndexes) {
    this.active = Optional.ofNullable(active);
    this.availableReplicas = copyToUnmodifiableList(availableReplicas);
    this.rawNodeIndexes = copyToUnmodifiableList(rawNodeIndexes);
  }

  public static PartitionTopology parse(List<HostAndServicePorts> allNodes, List<Integer> partitionNodeIndexes) {
    // The zero-th element of partitionNodeIndexes is the index of the node hosting the active.
    // If the active is unavailable, the server either returns a negative value or omits the element.
    // Treat both of those cases the same way.
    int activeNodeIndex = partitionNodeIndexes.isEmpty() ? PARTITION_NOT_EXISTENT : partitionNodeIndexes.get(0);
    HostAndServicePorts activeNode = activeNodeIndex < 0 ? null : allNodes.get(activeNodeIndex);

    return new PartitionTopology(
      activeNode,
      findAvailableReplicas(allNodes, partitionNodeIndexes),
      partitionNodeIndexes
    );
  }

  private static List<HostAndServicePorts> findAvailableReplicas(List<HostAndServicePorts> allNodes, List<Integer> nodeIndexes) {
    return nodeIndexes.stream()
      .skip(1) // first is active partition instance, not what we're looking for
      .filter(nodeIndex -> nodeIndex >= 0) // negative index means unavailable
      .map(allNodes::get)
      .collect(toList());
  }

  public Optional<HostAndServicePorts> active() {
    return active;
  }

  public List<HostAndServicePorts> availableReplicas() {
    return availableReplicas;
  }

  public OptionalInt nodeIndexForActive() {
    return nodeIndex(0);
  }

  public OptionalInt nodeIndexForReplica(int replicaIndex) {
    return nodeIndex(replicaIndex + 1);
  }

  /**
   * @param activeOrReplica Zero means return the index of the node hosting the active partition.
   * 1 means return the index of the node hosting the first replica, and so on.
   * @return Index of the node hosting the requested instance of the partition,
   * or an empty optional if the requested instance is not currently available.
   * @throws IllegalArgumentException if activeOrReplica is less than zero
   */
  private OptionalInt nodeIndex(int activeOrReplica) {
    if (activeOrReplica < 0) {
      throw new IllegalArgumentException("activeOrReplica must be non-negative, but got " + activeOrReplica);
    }
    // Manual bounds checking because asking for an unavailable replica is a normal case,
    // and throwing IndexOutOfBoundsException is relatively expensive.
    if (activeOrReplica >= rawNodeIndexes.size()) {
      return OptionalInt.empty();
    }

    int index = rawNodeIndexes.get(activeOrReplica);
    return index < 0 ? OptionalInt.empty() : OptionalInt.of(index);
  }

  List<Integer> rawNodeIndexes() {
    return rawNodeIndexes;
  }

  @Override
  public String toString() {
    // like rawNodeIndexes.toString(), but without spaces between items
    return rawNodeIndexes.stream()
      .map(Object::toString)
      .collect(joining(",", "[", "]"));
  }
}
