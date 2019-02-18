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
import ujson.Value

import scala.collection.{GenMap, mutable}
import scala.concurrent.duration.Duration

object N1qlProfile extends Enumeration {
  val Off, Phases, Timing = Value
}

sealed trait ScanConsistency
case class NotBounded() extends ScanConsistency
case class AtPlus(consistentWith: List[MutationToken], scanWait: Option[Duration] = None) extends ScanConsistency
case class RequestPlus(scanWait: Option[Duration] = None) extends ScanConsistency
case class StatementPlus(scanWait: Option[Duration] = None) extends ScanConsistency

case class QueryOptions(private[scala] val namedParameters: Option[Map[String,Any]] = None,
                        private[scala] val positionalParameters: Option[List[Any]] = None,
                        private[scala] val contextId: Option[String] = None,
                        private[scala] val credentials: Option[List[Credentials]] = None,
                        private[scala] val maxParallelism: Option[Int] = None,
                        private[scala] val disableMetrics: Option[Boolean] = None,
                        private[scala] val pipelineBatch: Option[Int] = None,
                        private[scala] val pipelineCap: Option[Int] = None,
                        private[scala] val profile: Option[N1qlProfile.Value] = None,
                        private[scala] val readonly: Option[Boolean] = None,
                        private[scala] val retryStrategy: Option[RetryStrategy] = None,
                        private[scala] val scanCap: Option[Int] = None,
                        private[scala] val scanConsistency: Option[ScanConsistency] = None,
                       // TODO BLOCKED support
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

  def contextId(contextId: String): QueryOptions = {
    copy(contextId = Option(contextId))
  }

  def credentials(credentials: List[Credentials]): QueryOptions = {
    copy(credentials = Option(credentials))
  }

  def credentials(login: String, password: String): QueryOptions = {
    copy(credentials = Option(List(new RoleBasedCredentials(login, password))))
  }

  def maxParallelism(maxParellism: Int): QueryOptions = {
    copy(maxParallelism = Option(maxParellism))
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

  // TODO BLOCKED remove
  def serverSideTimeout(serverSideTimeout: Duration): QueryOptions = {
    copy(serverSideTimeout = Option(serverSideTimeout))
  }

  def timeout(timeout: Duration): QueryOptions = {
    copy(timeout = Option(timeout))
  }
}


object QueryOptions {
  def apply() = new QueryOptions()
}