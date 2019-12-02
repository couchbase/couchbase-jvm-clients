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

package com.couchbase.client.scala.query.handlers

import com.couchbase.client.core.Core
import com.couchbase.client.core.deps.io.netty.util.CharsetUtil
import com.couchbase.client.core.msg.query.QueryRequest
import com.couchbase.client.scala.env.ClusterEnvironment
import com.couchbase.client.scala.query.QueryOptions
import com.couchbase.client.scala.transformers.JacksonTransformers
import com.couchbase.client.scala.util.{DurationConversions, Validate}
import com.couchbase.client.scala.view.ViewOptions
import java.nio.charset.StandardCharsets.UTF_8

import scala.compat.java8.OptionConverters._
import com.couchbase.client.core.msg.view.ViewRequest
import com.couchbase.client.scala.view.DesignDocumentNamespace.Development

import scala.concurrent.duration.Duration
import scala.util.Try

/**
  * Handles requests and responses for view operations.
  *
  * @author Graham Pople
  * @since 1.0.0
  */
private[scala] class ViewHandler() {
  import DurationConversions._

  def request[T](
      designDoc: String,
      viewName: String,
      options: ViewOptions,
      core: Core,
      environment: ClusterEnvironment,
      bucketName: String
  ): Try[ViewRequest] = {
    val validations: Try[ViewRequest] = for {
      _ <- Validate.notNullOrEmpty(designDoc, "designDoc")
      _ <- Validate.notNullOrEmpty(viewName, "viewName")
      _ <- Validate.notNull(options, "options")
      _ <- Validate.optNotNull(options.namespace, "namespace")
      _ <- Validate.optNotNull(options.reduce, "reduce")
      _ <- Validate.optNotNull(options.limit, "limit")
      _ <- Validate.optNotNull(options.group, "group")
      _ <- Validate.optNotNull(options.groupLevel, "groupLevel")
      _ <- Validate.optNotNull(options.inclusiveEnd, "inclusiveEnd")
      _ <- Validate.optNotNull(options.skip, "skip")
      _ <- Validate.optNotNull(options.scanConsistency, "scanConsistency")
      _ <- Validate.optNotNull(options.onError, "onError")
      _ <- Validate.optNotNull(options.debug, "debug")
      _ <- Validate.optNotNull(options.order, "order")
      _ <- Validate.optNotNull(options.key, "key")
      _ <- Validate.optNotNull(options.startKeyDocId, "startKeyDocId")
      _ <- Validate.optNotNull(options.endKeyDocId, "endKeyDocId")
      _ <- Validate.optNotNull(options.endKey, "endKey")
      _ <- Validate.optNotNull(options.startKey, "startKey")
      _ <- Validate.optNotNull(options.keys, "keys")
      _ <- Validate.optNotNull(options.timeout, "timeout")
      _ <- Validate.optNotNull(options.retryStrategy, "retryStrategy")
    } yield null

    if (validations.isFailure) {
      validations
    } else {
      val params = options.encode()

      val bytes = options.keys.map(s => s.getBytes(UTF_8))

      val timeout: Duration = options.timeout.getOrElse(environment.timeoutConfig.queryTimeout())
      val retryStrategy     = options.retryStrategy.getOrElse(environment.retryStrategy)

      val isDevelopment = options.namespace match {
        case Some(Development) => true
        case _                 => false
      }

      Try(
        new ViewRequest(
          timeout,
          core.context(),
          retryStrategy,
          core.context().authenticator(),
          bucketName,
          designDoc,
          viewName,
          params,
          bytes.asJava,
          isDevelopment,
          null /* todo: rto */
        )
      )
    }
  }
}
