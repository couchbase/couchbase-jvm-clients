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

package com.couchbase.client.scala.analytics

import java.util.UUID

import com.couchbase.client.core.annotation.Stability.Volatile
import com.couchbase.client.core.cnc.RequestSpan
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.json.{JsonArray, JsonObject}

import scala.concurrent.duration.Duration

/** Customize the execution of an analytics query.
  *
  * @author Graham Pople
  * @since 1.0.0
  */
case class AnalyticsOptions(
    private[scala] val parameters: Option[AnalyticsParameters] = None,
    private[scala] val clientContextId: Option[String] = None,
    private[scala] val retryStrategy: Option[RetryStrategy] = None,
    private[scala] val timeout: Option[Duration] = None,
    private[scala] val priority: Boolean = false,
    private[scala] val readonly: Option[Boolean] = None,
    private[scala] val scanConsistency: Option[AnalyticsScanConsistency] = None,
    private[scala] val parentSpan: Option[RequestSpan] = None,
    private[scala] val raw: Option[Map[String, Any]] = None
) {

  /** Sets the parent `RequestSpan`.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  @Volatile
  def parentSpan(value: RequestSpan): AnalyticsOptions = {
    copy(parentSpan = Some(value))
  }

  /** Provides named or positional parameters, for queries parameterised that way.
    *
    * See [[AnalyticsParameters]] for details.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def parameters(values: AnalyticsParameters): AnalyticsOptions = {
    copy(
      parameters = Some(values)
    )
  }

  /** Adds a client context ID to the request, that will be sent back in the response, allowing clients
    * to meaningfully trace requests/responses when many are exchanged.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def clientContextId(contextId: String): AnalyticsOptions = {
    copy(clientContextId = Option(contextId))
  }

  /** Sets a maximum timeout for processing.
    *
    * @param timeout the duration of the timeout.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def timeout(timeout: Duration): AnalyticsOptions = {
    copy(timeout = Option(timeout))
  }

  /** Sets the `RetryStrategy` to use.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def retryStrategy(retryStrategy: RetryStrategy): AnalyticsOptions = {
    copy(retryStrategy = Option(retryStrategy))
  }

  /** Specify that this is a high-priority request.  The default is false.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def priority(value: Boolean): AnalyticsOptions = {
    copy(priority = value)
  }

  /** Specify whether this is a readonly request, e.g. one that performs no mutations.
    *
    * The default is false.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def readonly(readonly: Boolean): AnalyticsOptions = copy(readonly = Option(readonly))

  /** Allows providing custom JSON key/value pairs for advanced usage.
    *
    * If available, it is recommended to use the methods on this object to customize the query. This method should
    * only be used if no such setter can be found (i.e. if an undocumented property should be set or you are using
    * an older client and a new server-configuration property has been added to the cluster).
    *
    * Note that the values will be passed through a JSON encoder, so do not provide already encoded JSON as the value. If
    * you want to pass objects or arrays, you can use `JsonObject` and `JsonArray` respectively.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def raw(raw: Map[String, Any]): AnalyticsOptions = {
    copy(raw = Some(raw))
  }

  /** Scan consistency for the query
    *
    * @param scanConsistency the index scan consistency to be used; see [[AnalyticsScanConsistency]] for details
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def scanConsistency(scanConsistency: AnalyticsScanConsistency): AnalyticsOptions =
    copy(scanConsistency = Some(scanConsistency))

  private[scala] def encode() = {
    val out = JsonObject.create

    parameters match {
      case Some(AnalyticsParameters.Named(named)) =>
        for { (k, v) <- named } {
          out.put("$" + k, v)
        }
      case Some(AnalyticsParameters.Positional(values)) =>
        val arr = JsonArray.create
        values foreach arr.add
        out.put("args", arr)

      case _ =>
    }

    clientContextId
      .getOrElse(UUID.randomUUID.toString)
      .foreach(v => out.put("client_context_id", v))

    readonly.foreach(v => out.put("readonly", v))

    scanConsistency match {
      case Some(sc) =>
        val toPut: String = sc match {
          case AnalyticsScanConsistency.NotBounded  => "not_bounded"
          case AnalyticsScanConsistency.RequestPlus => "request_plus"
        }
        out.put("scan_consistency", toPut)
      case _ =>
    }

    raw.foreach(_.foreach(x => out.put(x._1, x._2)))

    out
  }
}
