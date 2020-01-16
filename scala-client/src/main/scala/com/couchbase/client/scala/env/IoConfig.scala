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
import com.couchbase.client.core.env.NetworkResolution
import com.couchbase.client.core.service.ServiceType
import com.couchbase.client.scala.util.DurationConversions._

import scala.concurrent.duration.Duration

case class IoConfig(
    private[scala] val mutationTokensEnabled: Boolean = true,
    private[scala] val dnsSrvEnabled: Option[Boolean] = None,
    private[scala] val configPollInterval: Option[Duration] = None,
    private[scala] val kvCircuitBreakerConfig: Option[CircuitBreakerConfig] = None,
    private[scala] val queryCircuitBreakerConfig: Option[CircuitBreakerConfig] = None,
    private[scala] val viewCircuitBreakerConfig: Option[CircuitBreakerConfig] = None,
    private[scala] val searchCircuitBreakerConfig: Option[CircuitBreakerConfig] = None,
    private[scala] val analyticsCircuitBreakerConfig: Option[CircuitBreakerConfig] = None,
    private[scala] val managerCircuitBreakerConfig: Option[CircuitBreakerConfig] = None,
    private[scala] val captureTraffic: Option[Set[ServiceType]] = None,
    private[scala] val networkResolution: Option[NetworkResolution] = None,
    private[scala] val tcpKeepAlivesEnabled: Option[Boolean] = None,
    private[scala] val tcpKeepAliveTime: Option[Duration] = None,
    private[scala] val numKvConnections: Option[Int] = None,
    private[scala] val maxHttpConnections: Option[Int] = None,
    private[scala] val idleHttpConnectionTimeout: Option[Duration] = None,
    private[scala] val configIdleRedialTimeout: Option[Duration] = None
) {

  private[scala] def toCore: core.env.IoConfig.Builder = {
    val builder = core.env.IoConfig.builder()

    builder.enableMutationTokens(mutationTokensEnabled)
    dnsSrvEnabled.foreach(v => builder.enableDnsSrv(v))
    configPollInterval.foreach(v => builder.configPollInterval(v))
    kvCircuitBreakerConfig.foreach(v => builder.kvCircuitBreakerConfig(v.toCore))
    queryCircuitBreakerConfig.foreach(v => builder.queryCircuitBreakerConfig(v.toCore))
    viewCircuitBreakerConfig.foreach(v => builder.viewCircuitBreakerConfig(v.toCore))
    searchCircuitBreakerConfig.foreach(v => builder.searchCircuitBreakerConfig(v.toCore))
    analyticsCircuitBreakerConfig.foreach(v => builder.analyticsCircuitBreakerConfig(v.toCore))
    managerCircuitBreakerConfig.foreach(v => builder.managerCircuitBreakerConfig(v.toCore))
    captureTraffic.foreach(v => builder.captureTraffic(v.toSeq: _*))
    networkResolution.foreach(v => builder.networkResolution(v))
    tcpKeepAlivesEnabled.foreach(v => builder.enableTcpKeepAlives(v))
    tcpKeepAliveTime.foreach(v => builder.tcpKeepAliveTime(v))
    numKvConnections.foreach(v => builder.numKvConnections(v))
    maxHttpConnections.foreach(v => builder.maxHttpConnections(v))
    idleHttpConnectionTimeout.foreach(v => builder.idleHttpConnectionTimeout(v))
    configIdleRedialTimeout.foreach(v => builder.configIdleRedialTimeout(v))

    builder
  }

  /** Configures whether mutation tokens will be returned from the server for all mutation operations.
    *
    * @return this, for chaining
    */
  def mutationTokensEnabled(value: Boolean): IoConfig = {
    copy(mutationTokensEnabled = value)
  }

  /** Configures that DNS SRV should be used.
    *
    * @return this, for chaining
    */
  def enableDnsSrv(value: Boolean): IoConfig = {
    copy(dnsSrvEnabled = Some(value))
  }

  /** Configures whether network traffic should be captured on one or more services.
    *
    * @return this, for chaining
    */
  def captureTraffic(value: Set[ServiceType]): IoConfig = {
    copy(captureTraffic = Some(value))
  }

  /** Configures the network resolution setting to use.
    *
    * @return this, for chaining
    */
  def networkResolution(value: NetworkResolution): IoConfig = {
    copy(networkResolution = Some(value))
  }

  /** Configure whether TCP keep-alives will be sent.
    *
    * @return this, for chaining
    */
  def enableTcpKeepAlives(value: Boolean): IoConfig = {
    copy(tcpKeepAlivesEnabled = Some(value))
  }

  /** Configure the time between sending TCP keep-alives.
    *
    * @return this, for chaining
    */
  def tcpKeepAliveTime(value: Duration): IoConfig = {
    copy(tcpKeepAliveTime = Some(value))
  }

  /** Configure the number of connections to the KV service that will be created, per-node.
    *
    * @return this, for chaining
    */
  def numKvConnections(value: Int): IoConfig = {
    copy(numKvConnections = Some(value))
  }

  /** Configure the maximum number of HTTP connections to create.
    *
    * @return this, for chaining
    */
  def maxHttpConnections(value: Int): IoConfig = {
    copy(maxHttpConnections = Some(value))
  }

  def idleHttpConnectionTimeout(value: Duration): IoConfig = {
    copy(idleHttpConnectionTimeout = Some(value))
  }

  def configIdleRedialTimeout(value: Duration): IoConfig =
    copy(configIdleRedialTimeout = Some(value))

  /** Configures how frequently it will poll for new configs.
    *
    * @return this, for chaining
    */
  def configPollInterval(value: Duration): IoConfig = {
    copy(configPollInterval = Some(value))
  }

  /** Configures a `com.couchbase.client.core.endpoint.CircuitBreaker` to use for key-value operations.
    *
    * @return this, for chaining
    */
  def kvCircuitBreakerConfig(value: CircuitBreakerConfig): IoConfig = {
    copy(kvCircuitBreakerConfig = Some(value))
  }

  /** Configures a `com.couchbase.client.core.endpoint.CircuitBreaker` to use for query operations.
    *
    * @return this, for chaining
    */
  def queryCircuitBreakerConfig(value: CircuitBreakerConfig): IoConfig = {
    copy(queryCircuitBreakerConfig = Some(value))
  }

  /** Configures a `com.couchbase.client.core.endpoint.CircuitBreaker` to use for view operations.
    *
    * @return this, for chaining
    */
  def viewCircuitBreakerConfig(value: CircuitBreakerConfig): IoConfig = {
    copy(viewCircuitBreakerConfig = Some(value))
  }

  /** Configures a `com.couchbase.client.core.endpoint.CircuitBreaker` to use for search operations.
    *
    * @return this, for chaining
    */
  def searchCircuitBreakerConfig(value: CircuitBreakerConfig): IoConfig = {
    copy(searchCircuitBreakerConfig = Some(value))
  }

  /** Configures a `com.couchbase.client.core.endpoint.CircuitBreaker` to use for analytics operations.
    *
    * @return this, for chaining
    */
  def analyticsCircuitBreakerConfig(value: CircuitBreakerConfig): IoConfig = {
    copy(analyticsCircuitBreakerConfig = Some(value))
  }

  /** Configures a `com.couchbase.client.core.endpoint.CircuitBreaker` to use for management operations.
    *
    * @return this, for chaining
    */
  def managerCircuitBreakerConfig(value: CircuitBreakerConfig): IoConfig = {
    copy(managerCircuitBreakerConfig = Some(value))
  }

}
