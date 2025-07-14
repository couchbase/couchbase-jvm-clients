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

/** Allows configuring and customizing the compression configuration.
  *
  * @param enabled  If compression is enabled or not.
  * @param minSize  The minimum size when compression should be performed.
  * @param minRatio The minimum ratio of when a compressed doc should be sent.
  *
  * @since 1.0.0
  */
case class CompressionConfig(
    private[scala] val enabled: Boolean = core.env.CompressionConfig.DEFAULT_ENABLED,
    private[scala] val minSize: Int = core.env.CompressionConfig.DEFAULT_MIN_SIZE,
    private[scala] val minRatio: Double = core.env.CompressionConfig.DEFAULT_MIN_RATIO
) {

  private[scala] def toCore: core.env.CompressionConfig.Builder = {
    val builder = new core.env.CompressionConfig.Builder

    builder.enable(enabled)
    builder.minSize(minSize)
    builder.minRatio(minRatio)

    builder
  }

  /** If set to false, disabled compression.
    *
    * @param enabled true to enable, false otherwise.
    *
    * @return this for chaining purposes.
    */
  def enable(enabled: Boolean): CompressionConfig = {
    copy(enabled = enabled)
  }

  /** The minimum size after which compression is performed.
    *
    * The default is 32 bytes.
    *
    * @param minSize minimum size in bytes.
    *
    * @return this for chaining purposes.
    */
  def minSize(minSize: Int): CompressionConfig = {
    copy(minSize = minSize)
  }

  /** The minimum ratio after which a compressed doc is sent compressed
    * versus the uncompressed version is sent for efficiency.
    *
    * The default is 0.83.
    *
    * @param minRatio the minimum ratio.
    *
    * @return this for chaining purposes.
    */
  def minRatio(minRatio: Double) = {
    copy(minRatio = minRatio)
  }

}
