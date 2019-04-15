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
import com.couchbase.client.core.msg.analytics.AnalyticsRequest
import com.couchbase.client.core.msg.query.QueryRequest
import com.couchbase.client.scala.analytics.AnalyticsOptions
import com.couchbase.client.scala.env.ClusterEnvironment
import com.couchbase.client.scala.query.QueryOptions
import com.couchbase.client.scala.transformers.JacksonTransformers
import com.couchbase.client.scala.util.{DurationConversions, Validate}

import scala.concurrent.duration.Duration
import scala.util.Try

/**
  * Handles requests and responses for Analytics operations.
  *
  * @author Graham Pople
  * @since 1.0.0
  */
private[scala] class AnalyticsHandler() {
  import DurationConversions._

  def request[T](statement: String, options: AnalyticsOptions, core: Core, environment: ClusterEnvironment)
  : Try[AnalyticsRequest] = {

    val validations: Try[AnalyticsRequest] = for {
      _ <- Validate.notNullOrEmpty(statement, "statement")
      _ <- Validate.notNull(options, "options")
      _ <- Validate.optNotNull(options.namedParameters, "namedParameters")
      _ <- Validate.optNotNull(options.positionalParameters, "positionalParameters")
      _ <- Validate.optNotNull(options.clientContextId, "clientContextId")
      _ <- Validate.optNotNull(options.retryStrategy, "retryStrategy")
      _ <- Validate.optNotNull(options.serverSideTimeout, "serverSideTimeout")
      _ <- Validate.optNotNull(options.timeout, "timeout")
      _ <- Validate.notNull(options.priority, "priority")
    } yield null

    if (validations.isFailure) {
      validations
    }
    else {
      val params = options.encode()
      params.put("statement", statement)

      Try(JacksonTransformers.MAPPER.writeValueAsString(params)).map(queryStr => {
        val queryBytes = queryStr.getBytes(CharsetUtil.UTF_8)

        val timeout: Duration = options.timeout.getOrElse(environment.timeoutConfig.analyticsTimeout())
        val retryStrategy = options.retryStrategy.getOrElse(environment.retryStrategy())

        new AnalyticsRequest(timeout,
          core.context(),
          retryStrategy,
          environment.credentials(),
          queryBytes,
          if (options.priority) -1 else 0)
      })
    }
  }
}
