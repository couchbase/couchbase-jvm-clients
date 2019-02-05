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

object ReplicateTo extends Enumeration {
  val None, One, Two, Three = Value
}

object PersistTo extends Enumeration {
  val None, One, Two, Three = Value
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
case class ClientVerified(replicateTo: ReplicateTo.Value, persistTo: PersistTo.Value) extends Durability
case object Majority extends Durability
case object MajorityAndPersistOnMaster extends Durability
case object PersistToMajority extends Durability