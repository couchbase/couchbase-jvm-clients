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
import com.couchbase.client.scala.codec.Transcoder
import com.couchbase.client.scala.json

import scala.concurrent.duration.Duration

/** Provides control over how a get operation is performed.
  */
case class GetOptions(
    private[scala] val withExpiry: Boolean = false,
    private[scala] val project: Seq[String] = GetOptions.EmptyProject,
    private[scala] val timeout: Duration = Duration.MinusInf,
    private[scala] val parentSpan: Option[RequestSpan] = None,
    private[scala] val retryStrategy: Option[RetryStrategy] = None,
    private[scala] val transcoder: Option[Transcoder] = None
) {

  /** Couchbase documents optionally can have an expiration field set, e.g. when they will
    * automatically expire.  For efficiency reasons, by default the value of this expiration
    * field is not fetched upon getting a document.  If expiry is being used, then set this
    * field to true to ensure the expiration is fetched.  This will not only make it available
    * in the returned result, but also ensure that the expiry is available to use when mutating
    * the document, to avoid accidentally resetting the expiry to the default of 0.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def withExpiry(value: Boolean): GetOptions = {
    copy(withExpiry = value)
  }

  /** Projection is an advanced feature allowing one or more fields to be fetched from a JSON
    * document, and the results combined into a `JsonObject` result.
    *
    * It combines the efficiency of a Sub-Document fetch, in that only specific fields need to be retrieved, with
    * the ease-of-handling of a regular fetch, in that the results can be handled as one JSON.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def project(value: Seq[String]): GetOptions = {
    copy(project = value)
  }

  /** Changes the timeout setting used for this operation.
    *
    * When the operation will timeout.  This will default to `timeoutConfig().kvTimeout()` in the
    * [[com.couchbase.client.scala.env.ClusterEnvironment]].
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def timeout(value: Duration): GetOptions = {
    copy(timeout = value)
  }

  /** Changes the parent span setting used for this operation.
    *
    * This allows tracing requests through a full distributed system.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def parentSpan(value: RequestSpan): GetOptions = {
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
  def parentSpan(value: Option[RequestSpan]): GetOptions = {
    copy(parentSpan = value)
  }

  /** Changes the transcoder used for this operation.
    *
    * The transcoder provides control over how JSON is converted and stored on the Couchbase Server.
    *
    *
    * If not specified it will default to to `transcoder()` in the
    * [[com.couchbase.client.scala.env.ClusterEnvironment]].
    *
    * This Option-overload is provided as a convenience to help with chaining.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def transcoder(value: Transcoder): GetOptions = {
    copy(transcoder = Some(value))
  }

  /** Provides some control over how the SDK handles failures.  Will default to `retryStrategy()`
    * in the provided [[com.couchbase.client.scala.env.ClusterEnvironment]], which by default is
    * `BestEffortRetryStrategy`; this will automatically retry some operations (e.g. non-mutating ones, or mutating
    * operations that have unambiguously failed before they mutated state) until the chosen timeout.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def retryStrategy(value: RetryStrategy): GetOptions = {
    copy(retryStrategy = Some(value))
  }
}

object GetOptions {
  private val EmptyProject = Seq.empty[String]
}
