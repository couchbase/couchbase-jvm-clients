/*
 * Copyright (c) 2018 Couchbase, Inc.
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

import com.couchbase.client.core.msg.kv.{DurabilityLevel => CoreLevel}
import com.couchbase.client.core.service.kv.Observe
import com.couchbase.client.core.service.kv.Observe.{ObservePersistTo, ObserveReplicateTo}

object ReplicateTo extends Enumeration {
  val None, One, Two, Three = Value

  def asCore(v: ReplicateTo.Value): Observe.ObserveReplicateTo = v match {
    case None => ObserveReplicateTo.NONE
    case One => ObserveReplicateTo.ONE
    case Two => ObserveReplicateTo.TWO
    case Three => ObserveReplicateTo.THREE
  }

}

object PersistTo extends Enumeration {
  val None, Active, One, Two, Three, Four = Value

  def asCore(v: PersistTo.Value): Observe.ObservePersistTo = v match {
    case None => ObservePersistTo.NONE
    case Active => ObservePersistTo.ACTIVE
    case One => ObservePersistTo.ONE
    case Two => ObservePersistTo.TWO
    case Three => ObservePersistTo.THREE
    case Four => ObservePersistTo.FOUR
  }
}

@Deprecated
object DurabilityLevel extends Enumeration {
  val None, Majority, MajorityAndPersistActive, PersistToMajority = Value
}

sealed trait Durability {

  def toDurabilityLevel: Optional[CoreLevel] = {
    this match {
      case Majority => Optional.of(CoreLevel.MAJORITY)
      case PersistToMajority => Optional.of(CoreLevel.PERSIST_TO_MAJORITY)
      case MajorityAndPersistOnMaster => Optional.of(CoreLevel.MAJORITY_AND_PERSIST_ON_MASTER)
      case _ => Optional.empty()
    }
  }
}
case object Disabled extends Durability
case class ClientVerified(replicateTo: ReplicateTo.Value, persistTo: PersistTo.Value = PersistTo.None) extends Durability
case object Majority extends Durability
case object MajorityAndPersistOnMaster extends Durability
case object PersistToMajority extends Durability