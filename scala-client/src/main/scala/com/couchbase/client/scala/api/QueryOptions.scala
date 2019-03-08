/*
 * Copyright (c) 2018 Couchbase, Inc.
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

package com.couchbase.client.scala.api

import java.util.Objects

import com.couchbase.client.core.env.{Credentials, RoleBasedCredentials}
import com.couchbase.client.core.msg.kv.MutationToken
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.json.{JsonArray, JsonObject}

import scala.collection.{GenMap, mutable}
import scala.concurrent.duration.Duration

object N1qlProfile extends Enumeration {
  val Off, Phases, Timings = Value
}

sealed trait ScanConsistency {
  private[scala] def encoded: String
}
case class NotBounded() extends ScanConsistency {
  private[scala] def encoded = "not_bounded"
}
//case class AtPlus(consistentWith: List[MutationToken], scanWait: Option[Duration] = None) extends ScanConsistency
case class RequestPlus(scanWait: Option[Duration] = None) extends ScanConsistency {
  private[scala] def encoded = "request_plus"
}
//case class StatementPlus(scanWait: Option[Duration] = None) extends ScanConsistency

case class QueryOptions(private[scala] val namedParameters: Option[Map[String,Any]] = None,
                        private[scala] val positionalParameters: Option[List[Any]] = None,
                        private[scala] val clientContextId: Option[String] = None,
                        private[scala] val credentials: Option[Map[String,String]] = None,
                        private[scala] val maxParallelism: Option[Int] = None,
                        private[scala] val disableMetrics: Option[Boolean] = None,
                        private[scala] val pipelineBatch: Option[Int] = None,
                        private[scala] val pipelineCap: Option[Int] = None,
                        private[scala] val profile: Option[N1qlProfile.Value] = None,
                        private[scala] val readonly: Option[Boolean] = None,
                        private[scala] val retryStrategy: Option[RetryStrategy] = None,
                        private[scala] val scanCap: Option[Int] = None,
                        private[scala] val scanConsistency: Option[ScanConsistency] = None,
                        // TODO support
                        //                        consistentWith: Option[List[MutationToken]]
                        private[scala] val serverSideTimeout: Option[Duration] = None,
                        private[scala] val timeout: Option[Duration] = None
                       ) {
  def namedParameter(name: String, value: Any): QueryOptions = {
    copy(namedParameters = Some(namedParameters.getOrElse(Map()) + (name -> value)))
  }

  def namedParameters(values: (String, Any)*): QueryOptions = {
    copy(namedParameters = Option(values.toMap))
  }

  def namedParameters(values: Map[String,Any]): QueryOptions = {
    copy(namedParameters = Option(values))
  }

  def positionalParameters(values: Any*): QueryOptions = {
    copy(positionalParameters = Option(values.toList))
  }

  def clientContextId(contextId: String): QueryOptions = {
    copy(clientContextId = Option(contextId))
  }

  def credentials(login: String, password: String): QueryOptions = {
    copy(credentials = Option(Map(login -> password)))
  }

  def maxParallelism(maxParellism: Int): QueryOptions = {
    copy(maxParallelism = Option(maxParellism))
  }

  def pipelineCap(cap: Int): QueryOptions = {
    copy(pipelineCap = Some(cap))
  }

  def pipelineBatch(batch: Int): QueryOptions = {
    copy(pipelineBatch = Some(batch))
  }

  def disableMetrics(disableMetrics: Boolean): QueryOptions = {
    copy(disableMetrics = Option(disableMetrics))
  }

  def profile(profile: N1qlProfile.Value): QueryOptions = {
    copy(profile = Option(profile))
  }

  def readonly(readonly: Boolean): QueryOptions= {
    copy(readonly = Option(readonly))
  }

  def scanCap(scanCap: Int): QueryOptions = {
    copy(scanCap = Option(scanCap))
  }

  def scanConsistency(scanConsistency: ScanConsistency): QueryOptions = {
    copy(scanConsistency = Some(scanConsistency))
  }

//  def consistentWith(consistentWith: List[MutationToken]): QueryOptions = {
//    copy(consistentWith = Option(consistentWith))
//  }

  def serverSideTimeout(serverSideTimeout: Duration): QueryOptions = {
    copy(serverSideTimeout = Option(serverSideTimeout))
  }

  def timeout(timeout: Duration): QueryOptions = {
    copy(timeout = Option(timeout))
  }

  private[scala] def durationToN1qlFormat(duration: Duration) = {
    if (duration.toSeconds > 0) duration.toSeconds + "s"
    else duration.toNanos + "ns"
  }

  private[scala] def encode() = {
    val out = JsonObject.create

    credentials.foreach(creds => {
      val credsArr = JsonArray.create

      creds.foreach(k => {
        val c = JsonObject("user" -> k._1, "pass" -> k._2)
        credsArr.add(c)
      })

      if (credsArr.nonEmpty) {
        out.put("creds", creds)
      }
    })

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
    scanConsistency.foreach(v => out.put("scan_consistency", v.encoded))
    profile.foreach(v => out.put("profile", v.toString.toLowerCase))
    serverSideTimeout.foreach(v => out.put("timeout", durationToN1qlFormat(v)))

    // TODO
//    if (scanWait != null && ScanConsistency.REQUEST_PLUS eq scanConsistency) queryJson.put("scan_wait", scanWait)

    clientContextId.foreach(v => out.put("client_context_id", v))
    maxParallelism.foreach(v => out.put("max_parallelism", v.toString))
    pipelineCap.foreach(v => out.put("pipeline_cap", v.toString))
    pipelineBatch.foreach(v => out.put("pipeline_batch", v.toString))
    scanCap.foreach(v => out.put("scan_cap", v.toString))
    disableMetrics.foreach(v => out.put("metrics", v))
    readonly.foreach(v => out.put("readonly", v))

    val autoPrepare = System.getProperty("com.couchbase.client.query.autoprepared", "false").toBoolean
    if (autoPrepare) out.put("auto_prepare", "true")

    out
  }
}


object QueryOptions {
  def apply() = new QueryOptions()
}