package com.couchbase.client.scala.env

import com.couchbase.client.core
import com.couchbase.client.scala.util.DurationConversions._

import scala.concurrent.duration.Duration

/** Allows customizing the `AggregatingMeter`.
  */
case class AggregatingMeterConfig(
    private[scala] val emitInterval: Duration =
      core.env.AggregatingMeterConfig.Defaults.DEFAULT_EMIT_INTERVAL,
    private[scala] val enabled: Boolean = core.env.AggregatingMeterConfig.Defaults.DEFAULT_ENABLED
) {

  /** Customize the emit interval.
    *
    * Default is 600s.
    *
    * @return this, for chaining
    */
  def emitInterval(value: Duration): AggregatingMeterConfig = {
    copy(emitInterval = value)
  }

  /** Customize the emit interval.
    *
    * @return this, for chaining
    */
  def enabled(value: Boolean): AggregatingMeterConfig = {
    copy(enabled = value)
  }

  private[scala] def toCore = {
    val builder = com.couchbase.client.core.env.AggregatingMeterConfig.builder()

    builder.emitInterval(emitInterval)
    builder.enabled(enabled)

    builder
  }

}
