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

/** Holds all IO-related configuration and state.
  *
  * @since 1.0.0
  */
case class IoEnvironment(
    private[scala] val nativeIoEnabled: Option[Boolean] = None,
    private[scala] val eventLoopThreadCount: Option[Int] = None
) {

  /** If set to false (enabled by default) will disable I/O features that require native libraries,
    * like setting TCP_USER_TIMEOUT.
    *
    * @param nativeIoEnabled if native IO should be enabled or disabled.
    *
    * @return this, for chaining
    */
  def enableNativeIo(nativeIoEnabled: Boolean): IoEnvironment = {
    copy(nativeIoEnabled = Some(nativeIoEnabled))
  }

  /** Overrides the number of threads used per event loop.
    *
    * If not manually overridden, a fair thread count is calculated.
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

    nativeIoEnabled.foreach(v => builder.enableNativeIo(v))
    eventLoopThreadCount.foreach(v => builder.eventLoopThreadCount(v))

    builder
  }
}
