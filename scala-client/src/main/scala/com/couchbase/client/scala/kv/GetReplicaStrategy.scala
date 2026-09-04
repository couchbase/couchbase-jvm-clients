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
package com.couchbase.client.scala.kv

import com.couchbase.client.core.api.kv.CoreGetReplicaStrategy
import com.couchbase.client.core.service.kv.CoreGetReplicaStrategyFromIndex

/** Specifies a strategy for replica reads.
  */
sealed trait GetReplicaStrategy {
  private[scala] def toCore: CoreGetReplicaStrategy
}

object GetReplicaStrategy {

  /** Reads the document from a specific replica, identified by `index`.
    *
    * This is intended to be a low-level replica API that requires some understanding of how Couchbase maintains replicas:
    * Couchbase partitions data into vbuckets, with each vbucket having one active, and one or more replicas distributed
    * across the nodes.  The cluster topology contains what is known as the vbucket map, which will include for each
    * vbucket what is known as the replica chain.  This is an array of nodes identifying where those replicas exist.
    *
    * For example, if vbucket 106's replica chain is [3, 1] and its active is 6, then this vbucket has its active
    * currently on node 6, and two replicas currently on nodes 3 and 1.  ReplicaIndex.Second will access node 1 in this case.
    *
    * If ReplicaIndex.Third was specified, then as the bucket has only two replicas configured
    * [[com.couchbase.client.core.error.ReplicaIndexOutOfBoundsException]] will be raised.
    * Unless `options.wrap` is also specified, in which case the SDK will wrap around the replica chain, and here access node 3.
    *
    * Additionally users can find the configured replica count for the bucket using logic similar to:
    * `val numReplicas = cluster.buckets.getBucket("travel-sample").numReplicas`
    *
    * The vbucket map is expected to be stable when the cluster is, but under situations such as rebalances and
    * failovers, the vbucket map can and will change.  This can happen both in-between calls to `getReplica`, and
    * in-between SDK retries of the same operation.
    *
    * The replica chain can also contain -1 values, most commonly during transient failover scenarios.  This will;
    * result in those replica indexes raising [[com.couchbase.client.core.error.ReplicaIndexCurrentlyUnavailableException]].
    * Unless `options.wrap` is also specified, in which case the SDK will skip over -1 values and access the next
    * available replica.  If all replicas have -1 then `ReplicaIndexCurrentlyUnavailableException` will still be raised.
    *
    * The standard timeout of 2.5 seconds may not be suitable for some high availability replica use-cases.  It can be
    * configured using [[GetReplicaOptions]].
    *
    * This strategy never reads from the active; only replicas.  [[Collection.get]] should be used for reading the active
    * as usual.
    *
    * This strategy will raise [[com.couchbase.client.core.error.DocumentNotFoundOnReplicaException]] if the replica
    * reports that the document does not exist.  Note that this does not mean the document does not exist on the active
    * or other replicas, due to eventual consistency.
    * This strategy will never raise [[com.couchbase.client.core.error.DocumentNotFoundException]] or
    * [[com.couchbase.client.core.error.DocumentUnretrievableException]].
    *
    * @param index   the replica to read from
    * @param options see [[GetReplicaStrategyFromIndexOptions]]
    */
  def fromIndex(
      index: ReplicaIndex,
      options: GetReplicaStrategyFromIndexOptions = GetReplicaStrategyFromIndexOptions()
  ): GetReplicaStrategy = FromIndex(index, options.wrap)

  private[scala] case class FromIndex(index: ReplicaIndex, wrap: Boolean)
      extends GetReplicaStrategy {
    override private[scala] def toCore: CoreGetReplicaStrategy =
      new CoreGetReplicaStrategyFromIndex(index.toCore, wrap)
  }
}
