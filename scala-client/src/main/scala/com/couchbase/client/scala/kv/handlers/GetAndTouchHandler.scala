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

import com.couchbase.client.core.msg.ResponseStatus
import com.couchbase.client.core.msg.kv.{GetAndTouchRequest, GetAndTouchResponse}
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.HandlerParams
import com.couchbase.client.scala.durability.Durability
import com.couchbase.client.scala.durability.Durability._
import com.couchbase.client.scala.kv.{DefaultErrors, GetResult}
import com.couchbase.client.scala.util.Validate
import io.opentracing.Span

import scala.util.{Success, Try}


/**
  * Handles requests and responses for KV get-and-touch operations.
  *
  * @author Graham Pople
  * @since 1.0.0
  */
private[scala] class GetAndTouchHandler(hp: HandlerParams)
  extends RequestHandler[GetAndTouchResponse, GetResult] {

  def request[T](id: String,
                 expiration: java.time.Duration,
                 durability: Durability = Disabled,
                 parentSpan: Option[Span] = None,
                 timeout: java.time.Duration,
                 retryStrategy: RetryStrategy)
  : Try[GetAndTouchRequest] = {
    val validations: Try[GetAndTouchRequest] = for {
      _ <- Validate.notNullOrEmpty(id, "id")
      _ <- Validate.notNull(durability, "durability")
      _ <- Validate.notNull(expiration, "expiration")
      _ <- Validate.notNull(parentSpan, "parentSpan")
      _ <- Validate.notNull(timeout, "timeout")
      _ <- Validate.notNull(retryStrategy, "retryStrategy")
    } yield null

    if (validations.isFailure) {
      validations
    }
    else {

      Success(new GetAndTouchRequest(id,
        hp.collectionIdEncoded,
        timeout,
        hp.core.context(),
        hp.bucketName,
        retryStrategy,
        expiration,
        durability.toDurabilityLevel))
    }
  }

  def response(id: String, response: GetAndTouchResponse): GetResult = {
    response.status() match {
      case ResponseStatus.SUCCESS =>
        new GetResult(id, Left(response.content), response.flags(), response.cas, Option.empty)

      case _ => throw DefaultErrors.throwOnBadResult(response.status())
    }
  }
}