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
import com.couchbase.client.core.annotation.Stability
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
case class TimeoutConfig(
    private[scala] val kvTimeout: Option[Duration] = None,
    private[scala] val kvDurableTimeout: Option[Duration] = None,
    private[scala] val managementTimeout: Option[Duration] = None,
    private[scala] val queryTimeout: Option[Duration] = None,
    private[scala] val viewTimeout: Option[Duration] = None,
    private[scala] val searchTimeout: Option[Duration] = None,
    private[scala] val analyticsTimeout: Option[Duration] = None,
    private[scala] val connectTimeout: Option[Duration] = None,
    private[scala] val disconnectTimeout: Option[Duration] = None,
    private[scala] val kvScanTimeout: Option[Duration] = None
) {

  private[scala] def toCore: core.env.TimeoutConfig.Builder = {
    val builder = new core.env.TimeoutConfig.Builder

    kvTimeout.foreach(v => builder.kvTimeout(v))
    kvDurableTimeout.foreach(v => builder.kvDurableTimeout(v))
    kvScanTimeout.foreach(v => builder.kvScanTimeout(v))
    managementTimeout.foreach(v => builder.managementTimeout(v))
    queryTimeout.foreach(v => builder.queryTimeout(v))
    viewTimeout.foreach(v => builder.viewTimeout(v))
    searchTimeout.foreach(v => builder.searchTimeout(v))
    analyticsTimeout.foreach(v => builder.analyticsTimeout(v))
    connectTimeout.foreach(v => builder.connectTimeout(v))
    disconnectTimeout.foreach(v => builder.disconnectTimeout(v))

    builder
  }

  /** Sets the timeout to use for key-value operations.
    *
    * The default is 2.5 seconds.
    *
    * @return this, for chaining
    */
  def kvTimeout(value: Duration): TimeoutConfig = {
    copy(kvTimeout = Some(value))
  }

  /** Sets the timeout to use for persist-level key-value operations.
    *
    * This includes any with [[com.couchbase.client.scala.durability.Durability.MajorityAndPersistToActive]]
    * or [[com.couchbase.client.scala.durability.Durability.PersistToMajority]] or
    * [[com.couchbase.client.scala.durability.Durability.ClientVerified]] with `persistTo` set.
    *
    * The default is 10 seconds.
    *
    * @return this, for chaining
    */
  def kvDurableTimeout(value: Duration): TimeoutConfig = {
    copy(kvDurableTimeout = Some(value))
  }

  /** Sets the timeout to use for key-value range scan operations.
    *
    * The default is 75 seconds.
    *
    * @return this, for chaining
    */
  def kvScanTimeout(value: Duration): TimeoutConfig = {
    copy(kvScanTimeout = Some(value))
  }

  /** Sets the timeout to use for management operations.
    *
    * The default is 75 seconds.
    *
    * @return this, for chaining
    */
  def managementTimeout(value: Duration): TimeoutConfig = {
    copy(managementTimeout = Some(value))
  }

  /** Sets the timeout to use for query operations.
    *
    * The default is 75 seconds.
    *
    * @return this, for chaining
    */
  def queryTimeout(value: Duration): TimeoutConfig = {
    copy(queryTimeout = Some(value))
  }

  /** Sets the timeout to use for view operations.
    *
    * The default is 75 seconds.
    *
    * @return this, for chaining
    */
  def viewTimeout(value: Duration): TimeoutConfig = {
    copy(viewTimeout = Some(value))
  }

  /** Sets the timeout to use for search operations.
    *
    * The default is 75 seconds.
    *
    * @return this, for chaining
    */
  def searchTimeout(value: Duration): TimeoutConfig = {
    copy(searchTimeout = Some(value))
  }

  /** Sets the timeout to use for analytics operations.
    *
    * The default is 75 seconds.
    *
    * @return this, for chaining
    */
  def analyticsTimeout(value: Duration): TimeoutConfig = {
    copy(analyticsTimeout = Some(value))
  }

  /** Sets the timeout to use for connection operations.
    *
    * The default is 10 seconds.
    *
    * @return this, for chaining
    */
  def connectTimeout(value: Duration): TimeoutConfig = {
    copy(connectTimeout = Some(value))
  }

  /** Sets the timeout to use for disconnection operations.
    *
    * The default is 10 seconds.
    *
    * @return this, for chaining
    */
  def disconnectTimeout(value: Duration): TimeoutConfig = {
    copy(disconnectTimeout = Some(value))
  }

}
