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

package com.couchbase.client.scala.api

import scala.concurrent.duration.{FiniteDuration, _}

case class MutateInOptions(timeout: FiniteDuration = null,
                           expiration: FiniteDuration = 0.seconds,
                           replicateTo: ReplicateTo.Value = ReplicateTo.None,
                           persistTo: PersistTo.Value = PersistTo.None) {
  def timeout(timeout: FiniteDuration): MutateInOptions = copy(timeout = timeout)

  def expiration(expiration: FiniteDuration): MutateInOptions = copy(expiration = expiration)

  def replicateTo(replicateTo: ReplicateTo.Value): MutateInOptions = copy(replicateTo = replicateTo)

  def persistTo(persistTo: PersistTo.Value): MutateInOptions = copy(persistTo = persistTo)
}

object MutateInOptions {
  def apply(): MutateInOptions = new MutateInOptions()
}




