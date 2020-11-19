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

import java.time.Instant

import com.couchbase.client.core.annotation.Stability.{Uncommitted, Volatile}
import com.couchbase.client.core.cnc.RequestSpan
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.codec.Transcoder
import com.couchbase.client.scala.durability.Durability
import com.couchbase.client.scala.durability.Durability.Disabled

import scala.concurrent.duration.{Duration, _}

/** Provides control over how an decrement operation is performed.
  */
case class DecrementOptions(
    private[scala] val initial: Option[Long] = None,
    private[scala] val durability: Durability = Disabled,
    private[scala] val timeout: Duration = Duration.MinusInf,
    private[scala] val parentSpan: Option[RequestSpan] = None,
    private[scala] val retryStrategy: Option[RetryStrategy] = None,
    // null is not very Scala, but is required for backwards-compatibility
    private[scala] val expiry: Duration = null,
    private[scala] val expiryTime: Option[Instant] = None
) {

  /** The amount to initialise the document too, if it does not exist.  If this is
    * not set, and the document does not exist, Failure(DocumentDoesNotExistException) will be
    * returned
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def initial(value: Long): DecrementOptions = {
    copy(initial = Some(value))
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
  def durability(value: Durability): DecrementOptions = {
    copy(durability = value)
  }

  /** Changes the timeout setting used for this operation.
    *
    * When the operation will timeout.  This will default to `timeoutConfig().kvTimeout()` in the
    * [[com.couchbase.client.scala.env.ClusterEnvironment]].
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def timeout(value: Duration): DecrementOptions = {
    copy(timeout = value)
  }

  /** Changes the parent span setting used for this operation.
    *
    * This allows tracing requests through a full distributed system.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  @Volatile
  def parentSpan(value: RequestSpan): DecrementOptions = {
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
  def parentSpan(value: Option[RequestSpan]): DecrementOptions = {
    copy(parentSpan = value)
  }

  /** Provides some control over how the SDK handles failures.  Will default to `retryStrategy()`
    * in the provided [[com.couchbase.client.scala.env.ClusterEnvironment]], which by default is
    * `BestEffortRetryStrategy`; this will automatically retry some operations (e.g. non-mutating ones, or mutating
    * operations that have unambiguously failed before they mutated state) until the chosen timeout.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def retryStrategy(value: RetryStrategy): DecrementOptions = {
    copy(retryStrategy = Some(value))
  }

  /** Changes the expiry setting used for this operation.
    *
    * This overload should be used for any expiration times < 30 days.  If over that, use the overload that takes an
    * `Instant` instead.
    *
    * Couchbase documents optionally can have an expiration field set, e.g. when they will
    * automatically expire and be removed.  On mutations if this is left at the default (null), then any expiry
    * will be removed and the document will never expire.  If the application wants to preserve
    * expiration then they should use the `withExpiration` parameter on any gets, and provide
    * the returned expiration parameter to any mutations.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def expiry(value: Duration): DecrementOptions = {
    copy(expiry = value)
  }

  /** Changes the expiry setting used for this operation.
    *
    * This overload should be used for any expiration times < 30 days.  If over that, use the overload that takes an
    * `Instant` instead.
    *
    * Couchbase documents optionally can have an expiration field set, e.g. when they will
    * automatically expire and be removed.  On mutations if this is left at the default (null), then any expiry
    * will be removed and the document will never expire.  If the application wants to preserve
    * expiration then they should use the `withExpiration` parameter on any gets, and provide
    * the returned expiration parameter to any mutations.
    *
    * This Option-overload is provided as a convenience to help with chaining.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def expiry(value: Option[Duration]): DecrementOptions = {
    value match {
      case Some(x) => copy(expiry = x)
      case _       => this
    }
  }

  /** Changes the expiry setting used for this operation.
    *
    * This overload should be used for any expiration times >= 30 days.  If below that, use the overload that takes a
    * `Duration` instead.
    *
    * Couchbase documents optionally can have an expiration field set, e.g. when they will
    * automatically expire and be removed.  On mutations if this is left at the default (0), then any expiry
    * will be removed and the document will never expire.  If the application wants to preserve
    * expiration then they should use the `withExpiration` parameter on any gets, and provide
    * the returned expiration parameter to any mutations.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def expiry(value: Instant): DecrementOptions = {
    copy(expiryTime = Some(value))
  }
}
