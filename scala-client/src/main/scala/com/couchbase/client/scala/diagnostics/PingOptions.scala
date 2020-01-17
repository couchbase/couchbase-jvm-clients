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
package com.couchbase.client.scala.diagnostics
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.core.service.ServiceType

import scala.concurrent.duration.Duration

case class PingOptions(serviceTypes: Set[ServiceType] = PingOptions.AllServiceTypes,
                       reportId: Option[String] = None,
                       retryStrategy: Option[RetryStrategy] = None,
                       timeout: Option[Duration] = None) {

  /** Changes the timeout setting used for this operation.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def timeout(value: Duration): PingOptions = {
    copy(timeout = Some(value))
  }

  /** Provides some control over how the SDK handles failures.  Will default to `retryStrategy()`
    * in the provided [[com.couchbase.client.scala.env.ClusterEnvironment]], which by default is
    * `BestEffortRetryStrategy`; this will automatically retry some operations (e.g. non-mutating ones, or mutating
    * operations that have unambiguously failed before they mutated state) until the chosen timeout.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def retryStrategy(value: RetryStrategy): PingOptions = {
    copy(retryStrategy = Some(value))
  }

  /** Sets a report ID that will be returned in the [[com.couchbase.client.core.diagnostics.PingResult]].
    *
    * It defaults to a random string
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def reportId(value: String): PingOptions = {
    copy(reportId = Some(value))
  }

  /** Changes the services that will be pinged.
    *
    * The default is an empty set, which will cause all services to be pinged.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def serviceTypes(value: Set[ServiceType]): PingOptions = {
    copy(serviceTypes = value)
  }
}

object PingOptions {
  private[scala] val AllServiceTypes = Set.empty[ServiceType]
}
