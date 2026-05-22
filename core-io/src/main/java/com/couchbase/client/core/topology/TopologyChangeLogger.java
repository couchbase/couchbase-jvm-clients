/*
 * Copyright 2026 Couchbase, Inc.
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
import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.config.GlobalConfig;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

import static com.couchbase.client.core.logging.RedactableArgument.redactSystem;
import static com.couchbase.client.core.logging.RedactableArgument.redactUser;
import static com.couchbase.client.core.util.CbCollections.setOf;

/**
 * Logs cluster and bucket configuration changes at INFO when they're meaningful
 * to a user, otherwise at DEBUG.
 * <p>
 * This intentionally uses the deprecated {@link GlobalConfig} and {@link BucketConfig} classes, as they map more directly
 * to the raw server configs than the *Topology classes.
 */
@Stability.Internal
public class TopologyChangeLogger {
  private static final Logger log = LoggerFactory.getLogger(TopologyChangeLogger.class);

  private enum GlobalChange {
    PORT_INFOS,
    CLUSTER_CAPABILITIES,
    CLUSTER_IDENT,
  }

  private enum BucketChange {
    NODES,
    BUCKET_CAPABILITIES,
    CLUSTER_CAPABILITIES,
    LOCATOR,
    TYPE,
    TAINTED,
    FAST_FORWARD_MAP,
    NUMBER_OF_REPLICAS,
    NUMBER_OF_PARTITIONS,
    PARTITION_MAP,
  }

  private TopologyChangeLogger() {
  }

  public static void logChange(@Nullable GlobalConfig previous, GlobalConfig applied) {
    try {
      String label = "global cluster topology";

      if (previous == null) {
        log.info("Saw {} for the first time, at revision {}: {}",
                label, applied.version(), redactSystem(applied));
        return;
      }

      Set<GlobalChange> changes = diff(previous, applied);
      boolean meaningful = !changes.isEmpty();
      String msg = "Change to {} (revision {} -> {}) {}: {}";
      if (meaningful) {
        log.info(msg, label, previous.version(), applied.version(), changes, redactSystem(applied));
      } else {
        log.debug(msg, label, previous.version(), applied.version(), changes, redactSystem(applied));
      }
    } catch (Throwable ignored) {
      // Silently discard.  Logging is non-essential.
    }
  }

  public static void logChange(@Nullable BucketConfig previous, BucketConfig applied) {
    try {
      String label = "topology for bucket [" + redactUser(applied.name()) + "]";

      if (previous == null) {
        log.info("Saw {} for the first time, at revision {}: {}",
                label, applied.version(), redactSystem(applied));
        return;
      }

      Set<BucketChange> changes = diff(previous, applied);
      // Config changes caused by partition map shuffling is very common during rebalances, and too expensive to
      // log at INFO.
      boolean meaningful = !changes.equals(setOf(BucketChange.PARTITION_MAP));
      String msg = "Change to {} (revision {} -> {}) {}: {}";
      if (meaningful) {
        log.info(msg, label, previous.version(), applied.version(), changes, redactSystem(applied));
      } else {
        log.debug(msg, label, previous.version(), applied.version(), changes, redactSystem(applied));
      }
    } catch (Throwable ignored) {
      // Silently discard.  Logging is non-essential.
    }
  }

  private static Set<GlobalChange> diff(GlobalConfig previous, GlobalConfig applied) {
    EnumSet<GlobalChange> changes = EnumSet.noneOf(GlobalChange.class);
    if (!Objects.equals(previous.portInfos(), applied.portInfos())) changes.add(GlobalChange.PORT_INFOS);
    if (!Objects.equals(previous.clusterCapabilities(), applied.clusterCapabilities()))
      changes.add(GlobalChange.CLUSTER_CAPABILITIES);
    if (!Objects.equals(previous.clusterIdent(), applied.clusterIdent())) changes.add(GlobalChange.CLUSTER_IDENT);
    return changes;
  }

  private static Set<BucketChange> diff(BucketConfig previous, BucketConfig applied) {
    EnumSet<BucketChange> changes = EnumSet.noneOf(BucketChange.class);
    if (!Objects.equals(previous.nodes(), applied.nodes())) changes.add(BucketChange.NODES);
    if (!Objects.equals(previous.bucketCapabilities(), applied.bucketCapabilities()))
      changes.add(BucketChange.BUCKET_CAPABILITIES);
    if (!Objects.equals(previous.clusterCapabilities(), applied.clusterCapabilities()))
      changes.add(BucketChange.CLUSTER_CAPABILITIES);
    if (!Objects.equals(previous.locator(), applied.locator())) changes.add(BucketChange.LOCATOR);
    if (!Objects.equals(previous.type(), applied.type())) changes.add(BucketChange.TYPE);
    if (previous.tainted() != applied.tainted()) changes.add(BucketChange.TAINTED);
    if (previous.hasFastForwardMap() != applied.hasFastForwardMap()) changes.add(BucketChange.FAST_FORWARD_MAP);

    if (previous instanceof CouchbaseBucketConfig && applied instanceof CouchbaseBucketConfig) {
      CouchbaseBucketConfig p = (CouchbaseBucketConfig) previous;
      CouchbaseBucketConfig a = (CouchbaseBucketConfig) applied;
      if (p.numberOfReplicas() != a.numberOfReplicas()) changes.add(BucketChange.NUMBER_OF_REPLICAS);
      if (p.numberOfPartitions() != a.numberOfPartitions()) changes.add(BucketChange.NUMBER_OF_PARTITIONS);
      if (!partitionsToString(p).equals(partitionsToString(a))) changes.add(BucketChange.PARTITION_MAP);
    }

    return changes;
  }

  private static String partitionsToString(CouchbaseBucketConfig config) {
    StringBuilder sb = new StringBuilder();
    int n = config.numberOfPartitions();
    for (int i = 0; i < n; i++) {
      sb.append(i).append(':').append(config.nodeIndexForActive(i, false));
      for (int r = 0; r < config.numberOfReplicas(); r++) {
        sb.append(',').append(config.nodeIndexForReplica(i, r, false));
      }
      sb.append(';');
    }
    return sb.toString();
  }
}
