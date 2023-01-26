/*
 * Copyright (c) 2020 Couchbase, Inc.
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
package com.couchbase.client.scala.util
import com.couchbase.client.core.Core
import com.couchbase.client.scala.durability.Durability.{ClientVerified, MajorityAndPersistToActive, PersistToMajority}
import com.couchbase.client.scala.durability.{Durability, PersistTo}
import com.couchbase.client.scala.env.{ClusterEnvironment, CoreEnvironment}
import com.couchbase.client.scala.util.DurationConversions.javaDurationToScala

import scala.concurrent.duration.Duration

private[scala] object TimeoutUtil {
  def kvTimeout(env: ClusterEnvironment)(durability: Durability): Duration = {
    val isPersistLevel = durability match {
      case MajorityAndPersistToActive | PersistToMajority => true
      case v: ClientVerified                              => v.persistTo != PersistTo.None
      case _                                              => false
    }
    if (isPersistLevel) {
      javaDurationToScala(env.timeoutConfig.kvDurableTimeout())
    } else {
      javaDurationToScala(env.timeoutConfig.kvTimeout())
    }
  }

  def kvTimeout(core: Core, durability: Durability): Duration = {
    val isPersistLevel = durability match {
      case MajorityAndPersistToActive | PersistToMajority => true
      case v: ClientVerified => v.persistTo != PersistTo.None
      case _ => false
    }
    if (isPersistLevel) {
      javaDurationToScala(core.context.environment.timeoutConfig.kvDurableTimeout())
    } else {
      javaDurationToScala(core.context.environment.timeoutConfig.kvTimeout())
    }
  }
}
