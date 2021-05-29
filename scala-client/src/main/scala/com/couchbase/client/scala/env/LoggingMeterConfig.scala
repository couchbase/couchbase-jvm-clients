package com.couchbase.client.scala.env

import com.couchbase.client.core
import com.couchbase.client.scala.util.DurationConversions._

import scala.concurrent.duration.Duration

/** Allows customizing the `LoggingMeter`.
  */
case class LoggingMeterConfig(
    private[scala] val emitInterval: Duration =
      core.env.LoggingMeterConfig.Defaults.DEFAULT_EMIT_INTERVAL,
    private[scala] val enabled: Boolean = core.env.LoggingMeterConfig.Defaults.DEFAULT_ENABLED
) {

  /** Customize the emit interval.
    *
    * Default is 600s.
    *
    * @return this, for chaining
    */
  def emitInterval(value: Duration): LoggingMeterConfig = {
    copy(emitInterval = value)
  }

  /** Customize the emit interval.
    *
    * @return this, for chaining
    */
  def enabled(value: Boolean): LoggingMeterConfig = {
    copy(enabled = value)
  }

  private[scala] def toCore = {
    val builder = com.couchbase.client.core.env.LoggingMeterConfig.builder()

    builder.emitInterval(emitInterval)
    builder.enabled(enabled)

    builder
  }

}
