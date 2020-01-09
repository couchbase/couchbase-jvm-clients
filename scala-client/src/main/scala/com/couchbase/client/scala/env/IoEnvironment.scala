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
import com.couchbase.client.core.deps.io.netty.channel.EventLoopGroup

/** Holds all IO-related configuration and state.
  *
  * @since 1.0.0
  */
case class IoEnvironment(
    private[scala] val managerEventLoopGroup: Option[EventLoopGroup] = None,
    private[scala] val kvEventLoopGroup: Option[EventLoopGroup] = None,
    private[scala] val queryEventLoopGroup: Option[EventLoopGroup] = None,
    private[scala] val analyticsEventLoopGroup: Option[EventLoopGroup] = None,
    private[scala] val searchEventLoopGroup: Option[EventLoopGroup] = None,
    private[scala] val viewEventLoopGroup: Option[EventLoopGroup] = None,
    private[scala] val nativeIoEnabled: Option[Boolean] = None,
    private[scala] val eventLoopThreadCount: Option[Int] = None
) {

  /** Sets the [[EventLoopGroup]] to be used for config traffic.
    *
    * @return this, for chaining
    */
  def managerEventLoopGroup(value: EventLoopGroup): IoEnvironment = {
    copy(managerEventLoopGroup = Some(value))
  }

  /** Sets the [[EventLoopGroup]] to be used for key/value traffic.
    *
    * @return this, for chaining
    */
  def kvEventLoopGroup(value: EventLoopGroup): IoEnvironment = {
    copy(kvEventLoopGroup = Some(value))
  }

  /** Sets the [[EventLoopGroup]] to be used for query traffic.
    *
    * @return this, for chaining
    */
  def queryEventLoopGroup(value: EventLoopGroup): IoEnvironment = {
    copy(queryEventLoopGroup = Some(value))
  }

  /** Sets the [[EventLoopGroup]] to be used for analytics traffic.
    *
    * @return this, for chaining
    */
  def analyticsEventLoopGroup(value: EventLoopGroup): IoEnvironment = {
    copy(analyticsEventLoopGroup = Some(value))
  }

  /** Sets the [[EventLoopGroup]] to be used for search traffic.
    *
    * @return this, for chaining
    */
  def searchEventLoopGroup(value: EventLoopGroup): IoEnvironment = {
    copy(searchEventLoopGroup = Some(value))
  }

  /** Sets the [[EventLoopGroup]] to be used for view traffic.
    *
    * @return this, for chaining
    */
  def viewEventLoopGroup(value: EventLoopGroup): IoEnvironment = {
    copy(viewEventLoopGroup = Some(value))
  }

  /** If set to false (enabled by default) will force using the JVM NIO based IO transport.
    *
    * Usually the native transports used (epoll on linux and kqueue on OSX) are going to be faster and more efficient
    * than the generic NIO one. We recommend to only set this to false if you experience issues with the native
    * transports or instructed by couchbase support to do so for troubleshooting reasons.
    *
    * @param nativeIoEnabled if native IO should be enabled or disabled.
    *
    * @return this, for chaining
    */
  def enableNativeIo(nativeIoEnabled: Boolean): IoEnvironment = {
    copy(nativeIoEnabled = Some(nativeIoEnabled))
  }

  /**
    * Overrides the number of threads used per event loop.
    *
    * If not manually overridden, a fair thread count is calculated.
    *
    * Note that the count provided will only be used by event loops that the SDK creates. If you configure a custom
    * event loop (i.e. through [[.kvEventLoopGroup]]  you are responsible for sizing it
    * appropriately on your own.
    *
    * @param eventLoopThreadCount the number of event loops to use per pool.
    *
    * @return this, for chaining
    */
  def eventLoopThreadCount(eventLoopThreadCount: Int): IoEnvironment = {
    copy(eventLoopThreadCount = Some(eventLoopThreadCount))
  }


  private[scala] def toCore = {
    val builder = core.env.IoEnvironment.builder()

    managerEventLoopGroup.foreach(v => builder.managerEventLoopGroup(v))
    kvEventLoopGroup.foreach(v => builder.kvEventLoopGroup(v))
    queryEventLoopGroup.foreach(v => builder.queryEventLoopGroup(v))
    analyticsEventLoopGroup.foreach(v => builder.analyticsEventLoopGroup(v))
    searchEventLoopGroup.foreach(v => builder.searchEventLoopGroup(v))
    viewEventLoopGroup.foreach(v => builder.viewEventLoopGroup(v))
    nativeIoEnabled.foreach(v => builder.enableNativeIo(v))
    eventLoopThreadCount.foreach(v => builder.eventLoopThreadCount(v))

    builder
  }
}
