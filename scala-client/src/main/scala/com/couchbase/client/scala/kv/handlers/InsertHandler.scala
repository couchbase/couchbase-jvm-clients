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

import com.couchbase.client.core.cnc.{RequestSpan, RequestTracer, TracingIdentifiers}
import com.couchbase.client.core.error.context.KeyValueErrorContext
import com.couchbase.client.core.error.{DocumentExistsException, EncodingFailureException}
import com.couchbase.client.core.msg.ResponseStatus
import com.couchbase.client.core.msg.kv.{InsertRequest, InsertResponse, KeyValueRequest}
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.HandlerParams
import com.couchbase.client.scala.codec._
import com.couchbase.client.scala.durability.Durability
import com.couchbase.client.scala.kv.{DefaultErrors, MutationResult}
import com.couchbase.client.scala.util.Validate

import scala.compat.java8.OptionConverters._
import scala.util.{Failure, Success, Try}

/**
  * Handles requests and responses for KV insert operations.
  *
  * @author Graham Pople
  * @since 1.0.0
  */
private[scala] class InsertHandler(hp: HandlerParams)
    extends KeyValueRequestHandler[InsertResponse, MutationResult] {

  def request[T](
      id: String,
      content: T,
      durability: Durability,
      expiration: java.time.Duration,
      timeout: java.time.Duration,
      retryStrategy: RetryStrategy,
      transcoder: Transcoder,
      serializer: JsonSerializer[T],
      parentSpan: Option[RequestSpan]
  ): Try[InsertRequest] = {

    val validations: Try[InsertRequest] = for {
      _ <- Validate.notNullOrEmpty(id, "id")
      _ <- Validate.notNull(content, "content")
      _ <- Validate.notNull(durability, "durability")
      _ <- Validate.notNull(expiration, "expiration")
      _ <- Validate.notNull(timeout, "timeout")
      _ <- Validate.notNull(retryStrategy, "retryStrategy")
      _ <- Validate.notNull(parentSpan, "parentSpan")
    } yield null

    if (validations.isFailure) {
      validations
    } else {
      val span = hp.tracer.requestSpan(TracingIdentifiers.SPAN_REQUEST_KV_INSERT, parentSpan.orNull)

      val encodeSpan = hp.tracer.requestSpan(TracingIdentifiers.SPAN_REQUEST_ENCODING, span)
      val start      = System.nanoTime()

      val encoded: Try[EncodedValue] = transcoder match {
        case x: TranscoderWithSerializer    => x.encode(content, serializer)
        case x: TranscoderWithoutSerializer => x.encode(content)
      }

      val end = System.nanoTime()
      encodeSpan.`end`(hp.tracer)

      encoded match {
        case Success(en) =>
          val out = new InsertRequest(
            id,
            en.encoded,
            expiration.getSeconds,
            en.flags,
            timeout,
            hp.core.context(),
            hp.collectionIdentifier,
            retryStrategy,
            durability.toDurabilityLevel,
            span
          )
          out
            .context()
            .encodeLatency(end - start)
          Success(out)

        case Failure(err) =>
          Failure(new EncodingFailureException(err))
      }
    }
  }

  def response(
      request: KeyValueRequest[InsertResponse],
      id: String,
      response: InsertResponse
  ): MutationResult = {
    response.status() match {
      case ResponseStatus.EXISTS => {
        val ctx = KeyValueErrorContext.completedRequest(request, response.status())
        throw new DocumentExistsException(ctx)
      }

      case ResponseStatus.SUCCESS =>
        MutationResult(response.cas(), response.mutationToken().asScala)

      case _ => throw DefaultErrors.throwOnBadResult(request, response)
    }
  }
}
