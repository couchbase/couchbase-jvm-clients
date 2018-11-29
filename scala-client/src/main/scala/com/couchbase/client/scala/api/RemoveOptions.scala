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

import scala.concurrent.duration.FiniteDuration

case class DurabilityClient(replicateTo: ReplicateTo.Value = ReplicateTo.None,
                            persistTo: PersistTo.Value = PersistTo.None)

case class DurabilityServer(value: Durability.Value)

case class RemoveOptions(timeout: FiniteDuration = null,
                         durabilityClient: Option[DurabilityClient] = None,
                         durabilityServer: Option[DurabilityServer] = None) {
  def timeout(timeout: FiniteDuration): RemoveOptions = copy(timeout = timeout)

  def durabilityClient(replicateTo: ReplicateTo.Value, persistTo: PersistTo.Value): RemoveOptions = {
    copy(durabilityClient = Some(DurabilityClient(replicateTo, persistTo)), durabilityServer = None)
  }

  def durabilityServer(value: Durability.Value): RemoveOptions = {
    copy(durabilityServer = Some(DurabilityServer(value)), durabilityClient = None)
  }
}

object RemoveOptions {
  def apply(): RemoveOptions = new RemoveOptions()
}

