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
import com.couchbase.client.core.env.{SaslMechanism}

import scala.concurrent.duration.Duration
import scala.collection.JavaConverters._
import com.couchbase.client.scala.util.DurationConversions._

case class IoConfig(private[scala] val mutationTokensEnabled: Boolean = false,
                    private[scala] val allowedSaslMechanisms: Option[Set[SaslMechanism]] = None,
                    private[scala] val configPollInterval: Option[Duration] = None,
                    private[scala] val kvCircuitBreakerConfig: Option[CircuitBreakerConfig] = None,
                    private[scala] val queryCircuitBreakerConfig: Option[CircuitBreakerConfig] = None,
                    private[scala] val viewCircuitBreakerConfig: Option[CircuitBreakerConfig] = None,
                    private[scala] val searchCircuitBreakerConfig: Option[CircuitBreakerConfig] = None,
                    private[scala] val analyticsCircuitBreakerConfig: Option[CircuitBreakerConfig] = None,
                    private[scala] val managerCircuitBreakerConfig: Option[CircuitBreakerConfig] = None) {

  private[scala] def toCore: core.env.IoConfig.Builder = {
    val builder = core.env.IoConfig.builder()

    builder.mutationTokensEnabled(mutationTokensEnabled)
    allowedSaslMechanisms.foreach(v => builder.allowedSaslMechanisms(v.asJava))
    configPollInterval.foreach(v => builder.configPollInterval(v))
    kvCircuitBreakerConfig.foreach(v => builder.kvCircuitBreakerConfig(v.toCore))
    queryCircuitBreakerConfig.foreach(v => builder.queryCircuitBreakerConfig(v.toCore))
    viewCircuitBreakerConfig.foreach(v => builder.viewCircuitBreakerConfig(v.toCore))
    searchCircuitBreakerConfig.foreach(v => builder.searchCircuitBreakerConfig(v.toCore))
    analyticsCircuitBreakerConfig.foreach(v => builder.analyticsCircuitBreakerConfig(v.toCore))
    managerCircuitBreakerConfig.foreach(v => builder.managerCircuitBreakerConfig(v.toCore))

    builder
  }


  /** Configures whether mutation tokens will be returned from the server for all mutation operations.
    *
    * @return this, for chaining
    */
  def mutationTokensEnabled(value: Boolean): IoConfig = {
    copy(mutationTokensEnabled = value)
  }

  /** Configures which SASL mechanisms can be used.
    *
    * @return this, for chaining
    */
  def allowedSaslMechanisms(value: Set[SaslMechanism]): IoConfig = {
    copy(allowedSaslMechanisms = Some(value))
  }

  def configPollInterval(value: Duration): IoConfig = {
    copy(configPollInterval = Some(value))
  }

  /** Configures a [[com.couchbase.client.core.endpoint.CircuitBreaker]] to use for key-value operations.
    *
    * @return this, for chaining
    */
  def kvCircuitBreakerConfig(value: CircuitBreakerConfig): IoConfig = {
    copy(kvCircuitBreakerConfig = Some(value))
  }

  /** Configures a [[com.couchbase.client.core.endpoint.CircuitBreaker]] to use for queryoperations.
    *
    * @return this, for chaining
    */
  def queryCircuitBreakerConfig(value: CircuitBreakerConfig): IoConfig = {
    copy(queryCircuitBreakerConfig = Some(value))
  }

  /** Configures a [[com.couchbase.client.core.endpoint.CircuitBreaker]] to use for view operations.
    *
    * @return this, for chaining
    */
  def viewCircuitBreakerConfig(value: CircuitBreakerConfig): IoConfig = {
    copy(viewCircuitBreakerConfig = Some(value))
  }

  /** Configures a [[com.couchbase.client.core.endpoint.CircuitBreaker]] to use for search operations.
    *
    * @return this, for chaining
    */
  def searchCircuitBreakerConfig(value: CircuitBreakerConfig): IoConfig = {
    copy(searchCircuitBreakerConfig = Some(value))
  }

  /** Configures a [[com.couchbase.client.core.endpoint.CircuitBreaker]] to use for analytics operations.
    *
    * @return this, for chaining
    */
  def analyticsCircuitBreakerConfig(value: CircuitBreakerConfig): IoConfig = {
    copy(analyticsCircuitBreakerConfig = Some(value))
  }

  /** Configures a [[com.couchbase.client.core.endpoint.CircuitBreaker]] to use for management operations.
    *
    * @return this, for chaining
    */
  def managerCircuitBreakerConfig(value: CircuitBreakerConfig): IoConfig = {
    copy(managerCircuitBreakerConfig = Some(value))
  }

}
