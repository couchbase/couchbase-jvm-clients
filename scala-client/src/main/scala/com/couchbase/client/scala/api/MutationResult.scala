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

package com.couchbase.client.scala.api

import com.couchbase.client.core.msg.kv.MutationToken

private[scala] trait HasDurabilityTokens {
  val cas: Long
  val mutationToken: Option[MutationToken]
}

/** Contains the result of a successful mutation.
  *
  * @param cas           each Couchbase document has a CAS value, which is increased (not necessarily monotonically)
  *                      on each
  *                      successful mutation.  This is the updated post-mutation value.
  * @param mutationToken if the [[com.couchbase.client.scala.env.ClusterEnvironment]]'s `ioConfig()
  *                      .mutationTokensEnabled()` field is true (which is recommended), this will contain a
  *                      `MutationToken` providing additional context on the mutation.
  * @author Graham Pople
  * @since 1.0.0
  */
case class MutationResult(cas: Long, mutationToken: Option[MutationToken]) extends HasDurabilityTokens

case class CounterResult(cas: Long,
                         mutationToken: Option[MutationToken],
                         content: Long) extends HasDurabilityTokens
