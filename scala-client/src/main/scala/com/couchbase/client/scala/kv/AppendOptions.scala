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
package com.couchbase.client.scala.kv

import com.couchbase.client.core.cnc.RequestSpan
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.durability.Durability
import com.couchbase.client.scala.durability.Durability.Disabled

import scala.concurrent.duration.Duration

/** Provides control over how a binary append operation is performed.
  */
case class AppendOptions(
    private[scala] val cas: Long = 0,
    private[scala] val durability: Durability = Disabled,
    private[scala] val timeout: Duration = Duration.MinusInf,
    private[scala] val parentSpan: Option[RequestSpan] = None,
    private[scala] val retryStrategy: Option[RetryStrategy] = None
) {

  /** Couchbase documents all have a CAS (Compare-And-Set) field, a simple integer that allows
    * optimistic concurrency - e.g. it can detect if another agent has modified a document
    * in-between this agent getting and modifying the document.
    *
    * The default is 0, which disables CAS checking.    *
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def cas(value: Long): AppendOptions = {
    copy(cas = value)
  }

  /** Changes the durability setting used for this operation.
    *
    * Writes in Couchbase are written to a single node, and from there the Couchbase Server will
    * take care of sending that mutation to any configured replicas.  This parameter provides
    * some control over ensuring the success of the mutation's replication.  See
    * [[com.couchbase.client.scala.durability.Durability]].
    *
    * If not specified, it defaults to [[com.couchbase.client.scala.durability.Durability.Disabled]].
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def durability(value: Durability): AppendOptions = {
    copy(durability = value)
  }

  /** Changes the timeout setting used for this operation.
    *
    * When the operation will timeout.  This will default to `timeoutConfig().kvTimeout()` in the
    * [[com.couchbase.client.scala.env.ClusterEnvironment]].
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def timeout(value: Duration): AppendOptions = {
    copy(timeout = value)
  }

  /** Changes the parent span setting used for this operation.
    *
    * This allows tracing requests through a full distributed system.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def parentSpan(value: RequestSpan): AppendOptions = {
    copy(parentSpan = Some(value))
  }

  /** Changes the parent span setting used for this operation.
    *
    * This allows tracing requests through a full distributed system.
    *
    * This Option-overload is provided as a convenience to help with chaining.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def parentSpan(value: Option[RequestSpan]): AppendOptions = {
    copy(parentSpan = value)
  }

  /** Provides some control over how the SDK handles failures.  Will default to `retryStrategy()`
    * in the provided [[com.couchbase.client.scala.env.ClusterEnvironment]], which by default is
    * `BestEffortRetryStrategy`; this will automatically retry some operations (e.g. non-mutating ones, or mutating
    * operations that have unambiguously failed before they mutated state) until the chosen timeout.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def retryStrategy(value: RetryStrategy): AppendOptions = {
    copy(retryStrategy = Some(value))
  }
}

object AppendOptions {
  private[scala] val Default: AppendOptions = AppendOptions()
}
