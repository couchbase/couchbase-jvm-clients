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

import java.nio.charset.StandardCharsets.UTF_8
import java.util.Optional

import com.couchbase.client.core.Core
import com.couchbase.client.core.deps.io.netty.util.CharsetUtil
import com.couchbase.client.core.msg.query.QueryRequest
import com.couchbase.client.core.msg.view.ViewRequest
import com.couchbase.client.scala.env.ClusterEnvironment
import com.couchbase.client.scala.transformers.JacksonTransformers
import com.couchbase.client.scala.util.{DurationConversions, Validate}
import com.couchbase.client.scala.view.{SpatialViewOptions, ViewOptions}

import scala.concurrent.duration.Duration
import scala.util.Try

/**
  * Handles requests and responses for spatial view operations.
  *
  * @author Graham Pople
  * @since 1.0.0
  */
private[scala] class SpatialViewHandler() {

  import DurationConversions._

  def request[T](designDoc: String,
                 viewName: String,
                 options: SpatialViewOptions,
                 core: Core,
                 environment: ClusterEnvironment,
                 bucketName: String)
  : Try[ViewRequest] = {
    val validations: Try[ViewRequest] = for {
      _ <- Validate.notNullOrEmpty(designDoc, "designDoc")
      _ <- Validate.notNullOrEmpty(viewName, "viewName")
      _ <- Validate.notNull(options, "options")
      _ <- Validate.optNotNull(options.development, "development")
      _ <- Validate.optNotNull(options.limit, "limit")
      _ <- Validate.optNotNull(options.skip, "skip")
      _ <- Validate.optNotNull(options.stale, "stale")
      _ <- Validate.optNotNull(options.onError, "onError")
      _ <- Validate.optNotNull(options.startRange, "startRange")
      _ <- Validate.optNotNull(options.endRange, "endRange")
      _ <- Validate.optNotNull(options.timeout, "timeout")
      _ <- Validate.optNotNull(options.retryStrategy, "retryStrategy")
    } yield null

    if (validations.isFailure) {
      validations
    }
    else {
      val params = options.encode()

      val timeout: Duration = options.timeout.getOrElse(environment.timeoutConfig.queryTimeout())
      val retryStrategy = options.retryStrategy.getOrElse(environment.retryStrategy)

      Try(new ViewRequest(timeout,
        core.context(),
        retryStrategy,
        environment.credentials,
        bucketName,
        designDoc,
        viewName,
        params,
        Optional.empty(),
        options.development.getOrElse(false),
        true))
    }
  }
}
