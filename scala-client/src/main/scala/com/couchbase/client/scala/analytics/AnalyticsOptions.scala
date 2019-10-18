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

import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.json.{JsonArray, JsonObject}

import scala.collection.GenMap
import scala.concurrent.duration.Duration


/** Customize the execution of an analytics query.
  *
  * @author Graham Pople
  * @since 1.0.0
  */
case class AnalyticsOptions(private[scala] val namedParameters: Option[GenMap[String,Any]] = None,
                        private[scala] val positionalParameters: Option[Seq[Any]] = None,
                        private[scala] val clientContextId: Option[String] = None,
                        private[scala] val retryStrategy: Option[RetryStrategy] = None,
                        private[scala] val serverSideTimeout: Option[Duration] = None,
                        private[scala] val timeout: Option[Duration] = None,
                        private[scala] val priority: Boolean = false,
                        private[scala] val readonly: Option[Boolean] = None,
                        private[scala] val scanConsistency: Option[AnalyticsScanConsistency] = None
                       ) {
  /** Provides named parameters for queries parameterised that way.
    *
    * Overrides any previously-supplied named parameters.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def parameters(values: Map[String,Any]): AnalyticsOptions = {
    copy(namedParameters = Option(values), positionalParameters = None)
  }

  /** Provides named parameters for queries parameterised that way.
    *
    * Overrides any previously-supplied named parameters.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def parameters(values: JsonObject): AnalyticsOptions = {
    copy(namedParameters = Option(values.toMap), positionalParameters = None)
  }

  /** Provides positional parameters for queries parameterised that way.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def parameters(values: Seq[Any]): AnalyticsOptions = {
    copy(positionalParameters = Option(values), namedParameters = None)
  }

  /** Provides positional parameters for queries parameterised that way.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def parameters(values: JsonArray): AnalyticsOptions = {
    copy(positionalParameters = Option(values.toSeq), namedParameters = None)
  }

  /** Adds a client context ID to the request, that will be sent back in the response, allowing clients
    * to meaningfully trace requests/responses when many are exchanged.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def clientContextId(contextId: String): AnalyticsOptions = copy(clientContextId = Option(contextId))

  /** Sets a maximum timeout for processing on the server side.
    *
    * @param serverSideTimeout the duration of the timeout.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def serverSideTimeout(serverSideTimeout: Duration): AnalyticsOptions = copy(serverSideTimeout = Option(serverSideTimeout))

  def timeout(timeout: Duration): AnalyticsOptions = {
    copy(timeout = Option(timeout))
  }

  /** Specify that this is a high-priority request.  The default is false.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def priority(value: Boolean): AnalyticsOptions = {
    copy(priority = value)
  }

  def readonly(readonly: Boolean): AnalyticsOptions = copy(readonly = Option(readonly))

  private[scala] def durationToN1qlFormat(duration: Duration) = {
    if (duration.toSeconds > 0) duration.toSeconds + "s"
    else duration.toNanos + "ns"
  }

  private[scala] def encode() = {
    val out = JsonObject.create

    namedParameters.foreach(p => {
      p.foreach(k => {
        out.put('$' + k._1, k._2)
      })
    })
    positionalParameters.foreach(p => {
      val arr = JsonArray.create
      p.foreach(k => {
        arr.add(k)
      })
      out.put("args", arr)
    })
    serverSideTimeout.foreach(v => out.put("timeout", durationToN1qlFormat(v)))

    clientContextId
      .getOrElse(UUID.randomUUID.toString)
      .foreach(v => out.put("client_context_id", v))

    readonly.foreach(v => out.put("readonly", v))

    scanConsistency match {
      case Some(sc) =>
        val toPut: String = sc match {
          case AnalyticsScanConsistency.NotBounded => "not_bounded"
          case AnalyticsScanConsistency.RequestPlus => "request_plus"
        }
        out.put("scan_consistency", toPut)
      case _ =>
    }

    out
  }
}





