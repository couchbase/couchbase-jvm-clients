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
import com.couchbase.client.core.service
import com.couchbase.client.core.service.AbstractPooledEndpointServiceConfig
import com.couchbase.client.scala.util.DurationConversions._

import scala.concurrent.duration.Duration

case class ServiceConfig(private[scala] val keyValueServiceConfig: Option[KeyValueServiceConfig] = None,
                          private[scala] val queryServiceConfig: Option[QueryServiceConfig] = None,
                          private[scala] val viewServiceConfig: Option[ViewServiceConfig] = None,
                          private[scala] val searchServiceConfig: Option[SearchServiceConfig] = None,
                          private[scala] val analyticsServiceConfig: Option[AnalyticsServiceConfig] = None) {

  private[scala] def toCore: core.env.ServiceConfig.Builder = {
    val builder = new core.env.ServiceConfig.Builder

    keyValueServiceConfig.foreach(v => builder.keyValueServiceConfig(v.toCore))
    queryServiceConfig.foreach(v => builder.queryServiceConfig(v.toCore))
    viewServiceConfig.foreach(v => builder.viewServiceConfig(v.toCore))
    searchServiceConfig.foreach(v => builder.searchServiceConfig(v.toCore))
    analyticsServiceConfig.foreach(v => builder.analyticsServiceConfig(v.toCore))

    builder
  }


  /** Sets the configuration for key-value operations.
    *
    * @return a copy of this, for chaining
    */
  def keyValueServiceConfig(config: KeyValueServiceConfig): ServiceConfig = {
    copy(keyValueServiceConfig = Some(config))
  }

  /** Sets the configuration for query operations.
    *
    * @return a copy of this, for chaining
    */
  def queryServiceConfig(config: QueryServiceConfig): ServiceConfig = {
    copy(queryServiceConfig = Some(config))
  }

  /** Sets the configuration for view operations.
    *
    * @return a copy of this, for chaining
    */
  def viewServiceConfig(config: ViewServiceConfig): ServiceConfig = {
    copy(viewServiceConfig = Some(config))
  }

  /** Sets the configuration for search operations.
    *
    * @return a copy of this, for chaining
    */
  def searchServiceConfig(config: SearchServiceConfig): ServiceConfig = {
    copy(searchServiceConfig = Some(config))
  }

  /** Sets the configuration for analytics operations.
    *
    * @return a copy of this, for chaining
    */
  def analyticsServiceConfig(config: AnalyticsServiceConfig): ServiceConfig = {
    copy(analyticsServiceConfig = Some(config))
  }
}

case class KeyValueServiceConfig(private[scala] val endpoints: Option[Int] = None) {

  private[scala] def toCore: service.KeyValueServiceConfig.Builder = {
    service.KeyValueServiceConfig.builder()
      .endpoints(endpoints.getOrElse(service.KeyValueServiceConfig.DEFAULT_ENDPOINTS))
  }

  def endpoints(value: Int): KeyValueServiceConfig = {
    copy(endpoints = Some(value))
  }
}

case class QueryServiceConfig(private[scala] val minEndpoints: Int = AbstractPooledEndpointServiceConfig.DEFAULT_MIN_ENDPOINTS,
                              private[scala] val maxEndpoints: Int = AbstractPooledEndpointServiceConfig.DEFAULT_MAX_ENDPOINTS,
                              private[scala] val idleTime: Duration = AbstractPooledEndpointServiceConfig.DEFAULT_IDLE_TIME) {

  private[scala] def toCore: service.QueryServiceConfig.Builder = {
    service.QueryServiceConfig.builder()
      .minEndpoints(minEndpoints).maxEndpoints(maxEndpoints).idleTime(idleTime)
  }

  def minEndpoints(value: Int): QueryServiceConfig = {
    copy(minEndpoints = value)
  }

  def maxEndpoints(value: Int): QueryServiceConfig = {
    copy(maxEndpoints = value)
  }

  def idleTime(value: Duration): QueryServiceConfig = {
    copy(idleTime = value)
  }
}

case class ViewServiceConfig(private[scala] val minEndpoints: Int = AbstractPooledEndpointServiceConfig.DEFAULT_MIN_ENDPOINTS,
                             private[scala] val maxEndpoints: Int = AbstractPooledEndpointServiceConfig.DEFAULT_MAX_ENDPOINTS,
                             private[scala] val idleTime: Duration = AbstractPooledEndpointServiceConfig.DEFAULT_IDLE_TIME) {

  private[scala] def toCore: service.ViewServiceConfig.Builder = {
    service.ViewServiceConfig.builder()
      .minEndpoints(minEndpoints).maxEndpoints(maxEndpoints).idleTime(idleTime)
  }

  def minEndpoints(value: Int): ViewServiceConfig = {
    copy(minEndpoints = value)
  }

  def maxEndpoints(value: Int): ViewServiceConfig = {
    copy(maxEndpoints = value)
  }

  def idleTime(value: Duration): ViewServiceConfig = {
    copy(idleTime = value)
  }
}

case class SearchServiceConfig(private[scala] val minEndpoints: Int = AbstractPooledEndpointServiceConfig.DEFAULT_MIN_ENDPOINTS,
                               private[scala] val maxEndpoints: Int = AbstractPooledEndpointServiceConfig.DEFAULT_MAX_ENDPOINTS,
                               private[scala] val idleTime: Duration = AbstractPooledEndpointServiceConfig.DEFAULT_IDLE_TIME) {

  private[scala] def toCore: service.SearchServiceConfig.Builder = {
    service.SearchServiceConfig.builder()
      .minEndpoints(minEndpoints).maxEndpoints(maxEndpoints).idleTime(idleTime)
  }

  def minEndpoints(value: Int): SearchServiceConfig = {
    copy(minEndpoints = value)
  }

  def maxEndpoints(value: Int): SearchServiceConfig = {
    copy(maxEndpoints = value)
  }

  def idleTime(value: Duration): SearchServiceConfig = {
    copy(idleTime = value)
  }
}

case class AnalyticsServiceConfig(private[scala] val minEndpoints: Int = AbstractPooledEndpointServiceConfig.DEFAULT_MIN_ENDPOINTS,
                                  private[scala] val maxEndpoints: Int = AbstractPooledEndpointServiceConfig.DEFAULT_MAX_ENDPOINTS,
                                  private[scala] val idleTime: Duration = AbstractPooledEndpointServiceConfig.DEFAULT_IDLE_TIME) {

  private[scala] def toCore: service.AnalyticsServiceConfig.Builder = {
    service.AnalyticsServiceConfig.builder()
      .minEndpoints(minEndpoints).maxEndpoints(maxEndpoints).idleTime(idleTime)
  }

  def minEndpoints(value: Int): AnalyticsServiceConfig = {
    copy(minEndpoints = value)
  }

  def maxEndpoints(value: Int): AnalyticsServiceConfig = {
    copy(maxEndpoints = value)
  }

  def idleTime(value: Duration): AnalyticsServiceConfig = {
    copy(idleTime = value)
  }
}

