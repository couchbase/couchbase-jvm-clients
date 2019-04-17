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
package com.couchbase.client.scala.env

import com.couchbase.client.core
import com.couchbase.client.core.endpoint
import com.couchbase.client.scala.util.DurationConversions._

import scala.concurrent.duration.Duration

/** Allows configuring a [[com.couchbase.client.core.endpoint.CircuitBreaker]].
  *
  * @since 1.0.0
  */
case class CircuitBreakerConfig(private[scala] val enabled: Boolean =
                                core.endpoint.CircuitBreakerConfig.DEFAULT_ENABLED,
                                private[scala] val volumeThreshold: Int =
                                core.endpoint.CircuitBreakerConfig.DEFAULT_VOLUME_THRESHOLD,
                                private[scala] val errorThresholdPercentage: Int =
                                core.endpoint.CircuitBreakerConfig.DEFAULT_ERROR_THRESHOLD_PERCENTAGE,
                                private[scala] val sleepWindow: Duration =
                                core.endpoint.CircuitBreakerConfig.DEFAULT_SLEEP_WINDOW,
                                private[scala] val rollingWindow: Duration =
                                core.endpoint.CircuitBreakerConfig.DEFAULT_ROLLING_WINDOW) {

  private[scala] def toCore: endpoint.CircuitBreakerConfig.Builder = {
    val builder = endpoint.CircuitBreakerConfig.builder()

    builder.enabled(enabled)
    builder.volumeThreshold(volumeThreshold)
    builder.errorThresholdPercentage(errorThresholdPercentage)
    builder.sleepWindow(sleepWindow)
    builder.rollingWindow(rollingWindow)
  }

  /** Enables or disables this circuit breaker.
    *
    * <p>If this property is set to false, then all other properties are not looked at.</p>
    *
    * @param enabled if true enables it, if false disables it.
    *
    * @return this for chaining purposes.
    */
  def enabled(enabled: Boolean): CircuitBreakerConfig = {
    copy(enabled = enabled)
  }

  /** The volume threshold defines how many operations need to be in the window at least so that
    * the threshold percentage can be meaningfully calculated.
    *
    * The default is 20.
    *
    * @param volumeThreshold the volume threshold in the interval.
    *
    * @return this for chaining purposes.
    */
  def volumeThreshold(volumeThreshold: Int): CircuitBreakerConfig = {
    copy(volumeThreshold = volumeThreshold)
  }

  /** The percentage of operations that need to fail in a window until the circuit is opened.
    *
    * The default is 50.
    *
    * @param errorThresholdPercentage the percent of ops that need to fail.
    *
    * @return this for chaining purposes.
    */
  def errorThresholdPercentage(errorThresholdPercentage: Int): CircuitBreakerConfig = {
    copy(errorThresholdPercentage = errorThresholdPercentage)
  }

  /** The sleep window that is waited from when the circuit opens to when the canary is tried.
    *
    * The default is 5 seconds.
    *
    * @param sleepWindow the sleep window as a duration.
    *
    * @return this for chaining purposes.
    */
  def sleepWindow(sleepWindow: Duration): CircuitBreakerConfig = {
    copy(sleepWindow = sleepWindow)
  }

  /** How long the window is in which the number of failed ops are tracked in a rolling fashion.
    *
    * The default is 1 minute.
    *
    * @param rollingWindow the rolling window duration.
    *
    * @return this for chaining purposes.
    */
  def rollingWindow(rollingWindow: Duration): CircuitBreakerConfig = {
    copy(rollingWindow = rollingWindow)
  }
}
