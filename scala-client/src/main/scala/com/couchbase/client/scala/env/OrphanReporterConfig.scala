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
package com.couchbase.client.scala.env
import scala.concurrent.duration.Duration
import com.couchbase.client.scala.util.DurationConversions._

case class OrphanReporterConfig(
    private[scala] val emitInterval: Option[Duration] = None,
    private[scala] val sampleSize: Option[Int] = None,
    private[scala] val queueLength: Option[Int] = None
) {

  /** Customize the emit interval.
    *
    * @return this, for chaining
    */
  def emitInterval(value: Duration): OrphanReporterConfig = {
    copy(emitInterval = Some(value))
  }

  /** Configure the queue size for the individual span queues
    * used to track the spans over threshold.
    *
    * @return this, for chaining
    */
  def queueLength(value: Int): OrphanReporterConfig = {
    copy(queueLength = Some(value))
  }

  /** Customize the sample size per service.
    *
    * @return this, for chaining
    */
  def sampleSize(value: Int): OrphanReporterConfig = {
    copy(sampleSize = Some(value))
  }

  private[scala] def toCore = {
    val builder = com.couchbase.client.core.env.OrphanReporterConfig.builder()

    emitInterval.foreach(v => builder.emitInterval(v))
    queueLength.foreach(v => builder.queueLength(v))
    sampleSize.foreach(v => builder.sampleSize(v))

    builder
  }
}
