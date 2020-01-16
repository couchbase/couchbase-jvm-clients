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

import com.couchbase.client.core.annotation.Stability.Volatile
import com.couchbase.client.core.cnc.RequestSpan
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.codec.Transcoder
import com.couchbase.client.scala.durability.Durability
import com.couchbase.client.scala.durability.Durability.Disabled

import concurrent.duration._
import scala.concurrent.duration.Duration

/** Provides control over how an insert operation is performed.
  */
case class InsertOptions(
    private[scala] val durability: Durability = Disabled,
    private[scala] val timeout: Duration = Duration.MinusInf,
    private[scala] val parentSpan: Option[RequestSpan] = None,
    private[scala] val retryStrategy: Option[RetryStrategy] = None,
    private[scala] val transcoder: Option[Transcoder] = None,
    private[scala] val expiry: Duration = 0.seconds
) {

  /** Changes the durability setting used for this operation.
    *
    * Writes in Couchbase are written to a single node, and from there the Couchbase Server will
    * take care of sending that mutation to any configured replicas.  This parameter provides
    * some control over ensuring the success of the mutation's replication.  See
    * [[com.couchbase.client.scala.durability.Durability]].
    *
    * If not specified, it defaults to [[Durability.Disabled]].
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def durability(value: Durability): InsertOptions = {
    copy(durability = value)
  }

  /** Changes the timeout setting used for this operation.
    *
    * When the operation will timeout.  This will default to `timeoutConfig().kvTimeout()` in the
    * [[com.couchbase.client.scala.env.ClusterEnvironment]].
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def timeout(value: Duration): InsertOptions = {
    copy(timeout = value)
  }

  /** Changes the parent span setting used for this operation.
    *
    * This allows tracing requests through a full distributed system.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  @Volatile
  def parentSpan(value: RequestSpan): InsertOptions = {
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
  @Volatile
  def parentSpan(value: Option[RequestSpan]): InsertOptions = {
    copy(parentSpan = value)
  }

  /** Changes the expiry setting used for this operation.
    *
    * Couchbase documents optionally can have an expiration field set, e.g. when they will
    * automatically expire and be removed.  On mutations if this is left at the default (0), then any expiry
    * will be removed and the document will never expire.  If the application wants to preserve
    * expiration then they should use the `withExpiration` parameter on any gets, and provide
    * the returned expiration parameter to any mutations.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def expiry(value: Duration): InsertOptions = {
    copy(expiry = value)
  }

  /** Changes the expiry setting used for this operation.
    *
    * Couchbase documents optionally can have an expiration field set, e.g. when they will
    * automatically expire and be removed.  On mutations if this is left at the default (0), then any expiry
    * will be removed and the document will never expire.  If the application wants to preserve
    * expiration then they should use the `withExpiration` parameter on any gets, and provide
    * the returned expiration parameter to any mutations.
    *
    * This Option-overload is provided as a convenience to help with chaining.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def expiry(value: Option[Duration]): InsertOptions = {
    value match {
      case Some(x) => copy(expiry = x)
      case _       => this
    }
  }

  /** Changes the transcoder used for this operation.
    *
    * The transcoder provides control over how JSON is converted and stored on the Couchbase Server.
    * See [[Transcoder]] for more detail.
    *
    * If not specified it will default to to `transcoder()` in the
    * [[com.couchbase.client.scala.env.ClusterEnvironment]].
    *
    * This Option-overload is provided as a convenience to help with chaining.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def transcoder(value: Transcoder): InsertOptions = {
    copy(transcoder = Some(value))
  }

  /** Provides some control over how the SDK handles failures.  Will default to `retryStrategy()`
    * in the provided [[com.couchbase.client.scala.env.ClusterEnvironment]], which by default is
    * `BestEffortRetryStrategy`; this will automatically retry some operations (e.g. non-mutating ones, or mutating
    * operations that have unambiguously failed before they mutated state) until the chosen timeout.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def retryStrategy(value: RetryStrategy): InsertOptions = {
    copy(retryStrategy = Some(value))
  }
}
