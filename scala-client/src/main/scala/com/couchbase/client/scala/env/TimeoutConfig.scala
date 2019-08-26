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
import com.couchbase.client.scala.util.DurationConversions._

import scala.concurrent.duration.Duration

/** Configures all default timeouts.
  *
  * @param kvTimeout         the default timeout to use for key-value operations
  * @param managementTimeout the default timeout to use for management operations
  * @param queryTimeout      the default timeout to use for query operations
  * @param viewTimeout       the default timeout to use for view operations
  * @param searchTimeout     the default timeout to use for search operations
  * @param analyticsTimeout  the default timeout to use for analytics operations
  * @param connectTimeout    the default timeout to use for connection operations
  * @param disconnectTimeout the default timeout to use for disconnection operations
  */
case class TimeoutConfig(private[scala] val kvTimeout: Duration = core.env.TimeoutConfig.DEFAULT_KV_TIMEOUT,
                         private[scala] val managementTimeout: Duration = core.env.TimeoutConfig.DEFAULT_MANAGEMENT_TIMEOUT,
                         private[scala] val queryTimeout: Duration = core.env.TimeoutConfig.DEFAULT_QUERY_TIMEOUT,
                         private[scala] val viewTimeout: Duration = core.env.TimeoutConfig.DEFAULT_VIEW_TIMEOUT,
                         private[scala] val searchTimeout: Duration = core.env.TimeoutConfig.DEFAULT_SEARCH_TIMEOUT,
                         private[scala] val analyticsTimeout: Duration = core.env.TimeoutConfig.DEFAULT_ANALYTICS_TIMEOUT,
                         private[scala] val connectTimeout: Duration = core.env.TimeoutConfig.DEFAULT_CONNECT_TIMEOUT,
                         private[scala] val disconnectTimeout: Duration = core.env.TimeoutConfig.DEFAULT_DISCONNECT_TIMEOUT) {

  private[scala] def toCore: core.env.TimeoutConfig.Builder = {
    val builder = new core.env.TimeoutConfig.Builder

    builder.kvTimeout(kvTimeout)
    builder.managementTimeout(managementTimeout)
    builder.queryTimeout(queryTimeout)
    builder.viewTimeout(viewTimeout)
    builder.searchTimeout(searchTimeout)
    builder.analyticsTimeout(analyticsTimeout)
    builder.connectTimeout(connectTimeout)
    builder.disconnectTimeout(disconnectTimeout)

    builder
  }


  /** Sets the timeout to use for key-value operations.
    *
    * The default is 2.5 seconds.
    *
    * @return this, for chaining
    */
  def kvTimeout(value: Duration): TimeoutConfig = {
    copy(kvTimeout = value)
  }

  /** Sets the timeout to use for management operations.
    *
    * The default is 75 seconds.
    *
    * @return this, for chaining
    */
  def managementTimeout(value: Duration): TimeoutConfig = {
    copy(managementTimeout = value)
  }

  /** Sets the timeout to use for query operations.
    *
    * The default is 75 seconds.
    *
    * @return this, for chaining
    */
  def queryTimeout(value: Duration): TimeoutConfig = {
    copy(queryTimeout = value)
  }

  /** Sets the timeout to use for view operations.
    *
    * The default is 75 seconds.
    *
    * @return this, for chaining
    */
  def viewTimeout(value: Duration): TimeoutConfig = {
    copy(viewTimeout = value)
  }

  /** Sets the timeout to use for search operations.
    *
    * The default is 75 seconds.
    *
    * @return this, for chaining
    */
  def searchTimeout(value: Duration): TimeoutConfig = {
    copy(searchTimeout = value)
  }

  /** Sets the timeout to use for analytics operations.
    *
    * The default is 75 seconds.
    *
    * @return this, for chaining
    */
  def analyticsTimeout(value: Duration): TimeoutConfig = {
    copy(analyticsTimeout = value)
  }

  /** Sets the timeout to use for connection operations.
    *
    * The default is 10 seconds.
    *
    * @return this, for chaining
    */
  def connectTimeout(value: Duration): TimeoutConfig = {
    copy(connectTimeout = value)
  }

  /** Sets the timeout to use for disconnection operations.
    *
    * The default is 10 seconds.
    *
    * @return this, for chaining
    */
  def disconnectTimeout(value: Duration): TimeoutConfig = {
    copy(disconnectTimeout = value)
  }

}
