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

import com.couchbase.client.core.error.{DocumentNotFoundException, KeyValueErrorContext}
import com.couchbase.client.core.msg.ResponseStatus
import com.couchbase.client.core.msg.kv.{GetRequest, GetResponse, KeyValueRequest}
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.HandlerParams
import com.couchbase.client.scala.codec.Transcoder
import com.couchbase.client.scala.kv.{DefaultErrors, GetResult}
import com.couchbase.client.scala.util.Validate

import scala.util.{Success, Try}

/**
  * Handles requests and responses for KV regular get operations.
  *
  * @author Graham Pople
  * @since 1.0.0
  */
private[scala] class GetFullDocHandler(hp: HandlerParams)
    extends KeyValueRequestHandlerWithTranscoder[GetResponse, GetResult] {

  def request[T](
      id: String,
      timeout: java.time.Duration,
      retryStrategy: RetryStrategy
  ): Try[GetRequest] = {
    val validations: Try[GetRequest] = for {
      _ <- Validate.notNullOrEmpty(id, "id")
      _ <- Validate.notNull(timeout, "timeout")
      _ <- Validate.notNull(retryStrategy, "retryStrategy")
    } yield null

    if (validations.isFailure) {
      validations
    } else {
      Success(
        new GetRequest(
          id,
          timeout,
          hp.core.context(),
          hp.collectionIdentifier,
          retryStrategy,
          null /* todo: add rto */
        )
      )
    }
  }

  override def response(
      request: KeyValueRequest[GetResponse],
      id: String,
      response: GetResponse,
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
        val ctx = KeyValueErrorContext.completedRequest(request, response.status())
        throw new DocumentNotFoundException(ctx)

      case _ => throw DefaultErrors.throwOnBadResult(request, response)
    }
  }
}
