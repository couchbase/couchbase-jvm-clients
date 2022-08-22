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

package com.couchbase.client.scala.kv.handlers

import com.couchbase.client.core.cnc.{RequestSpan, TracingIdentifiers}
import com.couchbase.client.core.error.DocumentNotFoundException
import com.couchbase.client.core.error.context.KeyValueErrorContext
import com.couchbase.client.core.msg.ResponseStatus
import com.couchbase.client.core.msg.kv.{GetAndTouchRequest, GetAndTouchResponse, KeyValueRequest}
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.HandlerParams
import com.couchbase.client.scala.codec.Transcoder
import com.couchbase.client.scala.kv.{DefaultErrors, GetResult}
import com.couchbase.client.scala.util.Validate

import scala.util.{Success, Try}

/**
  * Handles requests and responses for KV get-and-touch operations.
  *
  * @author Graham Pople
  * @since 1.0.0
  */
private[scala] class GetAndTouchHandler(hp: HandlerParams)
    extends KeyValueRequestHandlerWithTranscoder[GetAndTouchResponse, GetResult] {

  def request[T](
      id: String,
      expiration: java.time.Duration,
      timeout: java.time.Duration,
      retryStrategy: RetryStrategy,
      parentSpan: Option[RequestSpan]
  ): Try[GetAndTouchRequest] = {
    val validations: Try[GetAndTouchRequest] = for {
      _ <- Validate.notNullOrEmpty(id, "id")
      _ <- Validate.notNull(expiration, "expiration")
      _ <- Validate.notNull(timeout, "timeout")
      _ <- Validate.notNull(retryStrategy, "retryStrategy")
      _ <- Validate.notNull(parentSpan, "parentSpan")
    } yield null

    if (validations.isFailure) {
      validations
    } else {

      Success(
        new GetAndTouchRequest(
          id,
          timeout,
          hp.core.context(),
          hp.collectionIdentifier,
          retryStrategy,
          expiration.getSeconds,
          hp.tracer.requestSpan(TracingIdentifiers.SPAN_REQUEST_KV_GET_AND_TOUCH, parentSpan.orNull)
        )
      )
    }
  }

  override def response(
      request: KeyValueRequest[GetAndTouchResponse],
      id: String,
      response: GetAndTouchResponse,
      transcoder: Transcoder
  ): GetResult = {
    response.status() match {
      case ResponseStatus.SUCCESS =>
        GetResult(
          id,
          Left(response.content),
          response.flags(),
          response.cas,
          Option.empty,
          transcoder
        )

      case ResponseStatus.NOT_FOUND =>
        val ctx = KeyValueErrorContext.completedRequest(request, response)
        throw new DocumentNotFoundException(ctx)
      case _ => throw DefaultErrors.throwOnBadResult(request, response)
    }
  }
}
