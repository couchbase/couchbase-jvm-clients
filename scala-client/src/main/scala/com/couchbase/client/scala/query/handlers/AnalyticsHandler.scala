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
import com.couchbase.client.core.cnc.TracingIdentifiers
import com.couchbase.client.core.deps.io.netty.util.CharsetUtil
import com.couchbase.client.core.error.ErrorCodeAndMessage
import com.couchbase.client.core.msg.analytics.AnalyticsRequest
import com.couchbase.client.scala.HandlerBasicParams
import com.couchbase.client.scala.analytics.{
  AnalyticsMetaData,
  AnalyticsMetrics,
  AnalyticsOptions,
  AnalyticsResult,
  AnalyticsStatus,
  AnalyticsWarning,
  ReactiveAnalyticsResult
}
import com.couchbase.client.scala.env.ClusterEnvironment
import com.couchbase.client.scala.transformers.JacksonTransformers
import com.couchbase.client.scala.util.{DurationConversions, FutureConversions, Validate}
import reactor.core.scala.publisher.SMono

import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.util.{Success, Try}
import scala.compat.java8.OptionConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.jdk.CollectionConverters._

/**
  * Handles requests and responses for Analytics operations.
  *
  * @author Graham Pople
  * @since 1.0.0
  */
private[scala] class AnalyticsHandler(hp: HandlerBasicParams) {
  import DurationConversions._

  def request[T](
      statement: String,
      options: AnalyticsOptions,
      core: Core,
      environment: ClusterEnvironment,
      bucket: Option[String],
      scope: Option[String]
  ): Try[AnalyticsRequest] = {

    val validations: Try[AnalyticsRequest] = for {
      _ <- Validate.notNullOrEmpty(statement, "statement")
      _ <- Validate.notNull(options, "options")
      _ <- Validate.optNotNull(options.parameters, "parameters")
      _ <- Validate.optNotNull(options.clientContextId, "clientContextId")
      _ <- Validate.optNotNull(options.retryStrategy, "retryStrategy")
      _ <- Validate.optNotNull(options.timeout, "timeout")
      _ <- Validate.notNull(options.priority, "priority")
      _ <- Validate.optNotNull(options.readonly, "readonly")
      _ <- Validate.optNotNull(options.parentSpan, "parentSpan")
    } yield null

    if (validations.isFailure) {
      validations
    } else {
      val params = options.encode()
      params.put("statement", statement)

      if (bucket.isDefined && scope.isDefined) {
        params.put("query_context", AnalyticsRequest.queryContext(bucket.get, scope.get))
      }

      Try(JacksonTransformers.MAPPER.writeValueAsString(params)).map(queryStr => {
        val queryBytes = queryStr.getBytes(CharsetUtil.UTF_8)

        val timeout: Duration =
          options.timeout.getOrElse(environment.timeoutConfig.analyticsTimeout())
        val retryStrategy = options.retryStrategy.getOrElse(environment.retryStrategy)

        new AnalyticsRequest(
          timeout,
          core.context(),
          retryStrategy,
          core.context().authenticator(),
          queryBytes,
          if (options.priority) -1 else 0,
          options.readonly.getOrElse(false),
          params.str("client_context_id"),
          statement,
          hp.tracer
            .requestSpan(TracingIdentifiers.SPAN_REQUEST_ANALYTICS, options.parentSpan.orNull),
          bucket.orNull,
          scope.orNull
        )
      })
    }
  }

  def queryAsync(
      request: AnalyticsRequest
  )(implicit ec: ExecutionContext): Future[AnalyticsResult] = {
    hp.core.send(request)

    val ret: Future[AnalyticsResult] = FutureConversions
      .javaCFToScalaMono(request, request.response(), propagateCancellation = true)
      .flatMap(
        response =>
          FutureConversions
            .javaFluxToScalaFlux(response.rows())
            .collectSeq()
            .flatMap(
              rows =>
                FutureConversions
                  .javaMonoToScalaMono(response.trailer())
                  .map(trailer => {
                    val warnings: collection.Seq[AnalyticsWarning] = trailer.warnings.asScala
                      .map(
                        warnings =>
                          ErrorCodeAndMessage
                            .fromJsonArray(warnings)
                            .asScala
                            .map(codeAndMessage => AnalyticsWarning(codeAndMessage))
                      )
                      .getOrElse(Seq.empty)

                    AnalyticsResult(
                      rows,
                      AnalyticsMetaData(
                        response.header().requestId(),
                        response.header().clientContextId().orElse(""),
                        response.header().signature.asScala,
                        AnalyticsMetrics.fromBytes(trailer.metrics),
                        warnings,
                        AnalyticsStatus.from(trailer.status)
                      )
                    )
                  })
            )
      )
      .toFuture

    ret.onComplete(_ => request.context.logicallyComplete())
    ret
  }

  def queryReactive(request: AnalyticsRequest): SMono[ReactiveAnalyticsResult] = {
    SMono.defer(() => {
      hp.core.send(request)

      FutureConversions
        .javaCFToScalaMono(request, request.response(), false)
        .map(response => {
          val meta: SMono[AnalyticsMetaData] = FutureConversions
            .javaMonoToScalaMono(response.trailer())
            .map(trailer => {
              val warnings: collection.Seq[AnalyticsWarning] = trailer.warnings.asScala
                .map(
                  warnings =>
                    ErrorCodeAndMessage
                      .fromJsonArray(warnings)
                      .asScala
                      .map(codeAndMessage => AnalyticsWarning(codeAndMessage))
                )
                .getOrElse(Seq.empty)

              AnalyticsMetaData(
                response.header().requestId(),
                response.header().clientContextId().orElse(""),
                response.header().signature.asScala,
                AnalyticsMetrics.fromBytes(trailer.metrics()),
                warnings,
                AnalyticsStatus.from(trailer.status)
              )
            })
            .doOnTerminate(() => request.context().logicallyComplete())

          val rows = FutureConversions.javaFluxToScalaFlux(response.rows())

          ReactiveAnalyticsResult(
            rows,
            meta
          )
        })
    })
  }
}
