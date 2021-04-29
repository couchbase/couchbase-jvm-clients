package com.couchbase.client.scala.env

import com.couchbase.client.core
import com.couchbase.client.scala.util.DurationConversions._

import scala.concurrent.duration.Duration

/** Allows customising the threshold request tracer.
  */
case class ThresholdRequestTracerConfig(
    private[scala] val emitInterval: Duration =
      core.env.ThresholdRequestTracerConfig.Defaults.DEFAULT_EMIT_INTERVAL,
    private[scala] val queueLength: Int =
      core.env.ThresholdRequestTracerConfig.Defaults.DEFAULT_QUEUE_LENGTH,
    private[scala] val sampleSize: Int =
      core.env.ThresholdRequestTracerConfig.Defaults.DEFAULT_SAMPLE_SIZE,
    private[scala] val kvThreshold: Duration =
      core.env.ThresholdRequestTracerConfig.Defaults.DEFAULT_KV_THRESHOLD,
    private[scala] val queryThreshold: Duration =
      core.env.ThresholdRequestTracerConfig.Defaults.DEFAULT_QUERY_THRESHOLD,
    private[scala] val viewThreshold: Duration =
      core.env.ThresholdRequestTracerConfig.Defaults.DEFAULT_VIEW_THRESHOLD,
    private[scala] val searchThreshold: Duration =
      core.env.ThresholdRequestTracerConfig.Defaults.DEFAULT_SEARCH_THRESHOLD,
    private[scala] val analyticsThreshold: Duration =
      core.env.ThresholdRequestTracerConfig.Defaults.DEFAULT_ANALYTICS_THRESHOLD
) {

  /** Customize the emit interval.
    *
    * Default is 600s.
    *
    * @return this, for chaining
    */
  def emitInterval(value: Duration): ThresholdRequestTracerConfig = {
    copy(emitInterval = value)
  }

  /** Customize the queue size for the individual span queues used to track the spans over threshold.
    *
    * @return this, for chaining
    */
  def queueLength(value: Int): ThresholdRequestTracerConfig = {
    copy(queueLength = value)
  }

  /** Customize the sample size.
    *
    * @return this, for chaining
    */
  def sampleSize(value: Int): ThresholdRequestTracerConfig = {
    copy(sampleSize = value)
  }

  /** Customize the threshold for KV operations.
    *
    * Default is 500ms.
    *
    * @return this, for chaining
    */
  def kvThreshold(value: Duration): ThresholdRequestTracerConfig = {
    copy(kvThreshold = value)
  }

  /** Customize the threshold for query (N1QL) operations.
    *
    * Default is 1s.
    *
    * @return this, for chaining
    */
  def queryThreshold(value: Duration): ThresholdRequestTracerConfig = {
    copy(queryThreshold = value)
  }

  /** Customize the threshold for view operations.
    *
    * Default is 1s.
    *
    * @return this, for chaining
    */
  def viewThreshold(value: Duration): ThresholdRequestTracerConfig = {
    copy(viewThreshold = value)
  }

  /** Customize the threshold for search (FTS) operations.
    *
    * Default is 1s.
    *
    * @return this, for chaining
    */
  def searchThreshold(value: Duration): ThresholdRequestTracerConfig = {
    copy(searchThreshold = value)
  }

  /** Customize the threshold for analytics operations.
    *
    * Default is 1s.
    *
    * @return this, for chaining
    */
  def analyticsThreshold(value: Duration): ThresholdRequestTracerConfig = {
    copy(analyticsThreshold = value)
  }

  private[scala] def toCore = {
    val builder = com.couchbase.client.core.env.ThresholdRequestTracerConfig.builder()

    builder.emitInterval(emitInterval)
    builder.queueLength(queueLength)
    builder.sampleSize(sampleSize)
    builder.kvThreshold(kvThreshold)
    builder.queryThreshold(queryThreshold)
    builder.viewThreshold(viewThreshold)
    builder.searchThreshold(searchThreshold)
    builder.analyticsThreshold(analyticsThreshold)

    builder
  }

}
