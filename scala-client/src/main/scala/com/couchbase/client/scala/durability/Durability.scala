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

package com.couchbase.client.scala.durability

import java.util.Optional

import com.couchbase.client.core.annotation.SinceCouchbase
import com.couchbase.client.core.msg.kv.{DurabilityLevel => CoreLevel}
import com.couchbase.client.core.service.kv.Observe
import com.couchbase.client.core.service.kv.Observe.{ObservePersistTo, ObserveReplicateTo}

/** Writes in Couchbase are written to a single node, and from there the Couchbase Server will
  * take care of sending that mutation to any configured replicas.  This option provides
  * some control over ensuring the success of the mutation's replication.
  *
  * This provides flexibility to the application to decide, per-operation, whether it wants to prioritise speed or
  * safety.
  */
sealed trait Durability {

  private[scala] def toDurabilityLevel: Optional[CoreLevel] = {
    this match {
      case Durability.Majority                   => Optional.of(CoreLevel.MAJORITY)
      case Durability.PersistToMajority          => Optional.of(CoreLevel.PERSIST_TO_MAJORITY)
      case Durability.MajorityAndPersistToActive =>
        Optional.of(CoreLevel.MAJORITY_AND_PERSIST_TO_ACTIVE)
      case _ => Optional.empty()
    }
  }
}

object Durability {

  /** The SDK will return as soon as the first single node has the mutation in memory (but not necessarily persisted
    * to disk).
    */
  case object Disabled extends Durability

  /** The SDK will do a simple polling loop to wait for the mutation to be available on a number of replicas.
    *
    * Available for all Couchbase Server versions.  If using Couchbase Server 6.5, then prefer using the other
    * Durability options.
    *
    * @param replicateTo the number of replicas to wait for on which the mutation will be available in-memory
    * @param persistTo   the number of replicas to wait for on which the mutation has been written to storage
    */
  case class ClientVerified(
      replicateTo: ReplicateTo.Value,
      persistTo: PersistTo.Value = PersistTo.None
  ) extends Durability

  /** The server will ensure that the change is available in memory on the majority of configured replicas, before
    * returning success to the SDK.
    *
    * Only available in Couchbase Server 6.5 and above.
    */
  @SinceCouchbase("6.5")
  case object Majority extends Durability

  /** The server will ensure that the change is available in memory on the majority of
    * configured replicas, plus persisted to disk on the active node, before returning success
    * to the SDK.
    *
    * Only available in Couchbase Server 6.5 and above.
    */
  @SinceCouchbase("6.5")
  case object MajorityAndPersistToActive extends Durability

  /** The server will ensure that the change is both available in memory and persisted to disk
    * on the majority of configured replicas, before returning success to the SDK.
    *
    * Only available in Couchbase Server 6.5 and above.
    */
  @SinceCouchbase("6.5")
  case object PersistToMajority extends Durability

}

/** The number of replicas to wait for on which the mutation will be available in-memory */
object ReplicateTo extends Enumeration {
  val None, One, Two, Three = Value

  private[scala] def asCore(v: ReplicateTo.Value): Observe.ObserveReplicateTo = v match {
    case None  => ObserveReplicateTo.NONE
    case One   => ObserveReplicateTo.ONE
    case Two   => ObserveReplicateTo.TWO
    case Three => ObserveReplicateTo.THREE
  }

}

/** The number of replicas to wait for on which the mutation has been written to storage */
object PersistTo extends Enumeration {
  val None, Active, One, Two, Three, Four = Value

  private[scala] def asCore(v: PersistTo.Value): Observe.ObservePersistTo = v match {
    case None   => ObservePersistTo.NONE
    case Active => ObservePersistTo.ACTIVE
    case One    => ObservePersistTo.ONE
    case Two    => ObservePersistTo.TWO
    case Three  => ObservePersistTo.THREE
    case Four   => ObservePersistTo.FOUR
  }
}
