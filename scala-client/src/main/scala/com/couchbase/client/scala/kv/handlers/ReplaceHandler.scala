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

import com.couchbase.client.core.cnc.{CbTracing, RequestSpan, RequestTracer, TracingIdentifiers}
import com.couchbase.client.core.error.EncodingFailureException
import com.couchbase.client.core.msg.ResponseStatus
import com.couchbase.client.core.msg.kv._
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.HandlerParams
import com.couchbase.client.scala.codec._
import com.couchbase.client.scala.durability.Durability
import com.couchbase.client.scala.kv.{DefaultErrors, MutationResult}
import com.couchbase.client.scala.util.Validate

import scala.compat.java8.OptionConverters._
import scala.util.{Failure, Success, Try}

/**
  * Handles requests and responses for KV replace operations.
  *
  * @author Graham Pople
  * @since 1.0.0
  */
private[scala] class ReplaceHandler(hp: HandlerParams)
    extends KeyValueRequestHandler[ReplaceResponse, MutationResult] {

  def request[T](
      id: String,
      content: T,
      cas: Long,
      durability: Durability,
      expiryEpochTimeSecs: Long,
      preserveExpiry: Boolean,
      timeout: java.time.Duration,
      retryStrategy: RetryStrategy,
      transcoder: Transcoder,
      serializer: JsonSerializer[T],
      parentSpan: Option[RequestSpan]
  ): Try[ReplaceRequest] = {
    val validations: Try[ReplaceRequest] = for {
      _ <- Validate.notNullOrEmpty(id, "id")
      _ <- Validate.notNull(content, "content")
      _ <- Validate.notNull(cas, "cas")
      _ <- Validate.notNull(durability, "durability")
      _ <- Validate.notNull(expiryEpochTimeSecs, "expiration")
      _ <- Validate.notNull(timeout, "timeout")
      _ <- Validate.notNull(retryStrategy, "retryStrategy")
      _ <- Validate.notNull(parentSpan, "parentSpan")
    } yield null

    if (validations.isFailure) {
      validations
    } else {
      val span =
        hp.tracer.requestSpan(TracingIdentifiers.SPAN_REQUEST_KV_REPLACE, parentSpan.orNull)

      val encodeSpan = CbTracing.newSpan(hp.tracer, TracingIdentifiers.SPAN_REQUEST_ENCODING, span)
      val start      = System.nanoTime()

      val encoded: Try[EncodedValue] = transcoder match {
        case x: TranscoderWithSerializer    => x.encode(content, serializer)
        case x: TranscoderWithoutSerializer => x.encode(content)
      }
      val end = System.nanoTime()
      encodeSpan.`end`()

      encoded match {
        case Success(en) =>
          val out = new ReplaceRequest(
            id,
            en.encoded,
            expiryEpochTimeSecs,
            preserveExpiry,
            en.flags,
            timeout,
            cas,
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
      request: KeyValueRequest[ReplaceResponse],
      id: String,
      response: ReplaceResponse
  ): MutationResult = {
    response.status() match {
      case ResponseStatus.SUCCESS =>
        MutationResult(response.cas(), response.mutationToken().asScala)

      case _ => throw DefaultErrors.throwOnBadResult(request, response)
    }
  }
}
