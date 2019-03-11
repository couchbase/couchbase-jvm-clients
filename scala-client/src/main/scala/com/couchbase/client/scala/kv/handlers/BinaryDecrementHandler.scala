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
import com.couchbase.client.core.msg.kv.{DecrementRequest, DecrementResponse}
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.HandlerParams
import com.couchbase.client.scala.api.CounterResult
import com.couchbase.client.scala.durability.Durability
import com.couchbase.client.scala.kv.DefaultErrors
import com.couchbase.client.scala.util.Validate
import io.opentracing.Span

import scala.compat.java8.OptionConverters._
import scala.util.{Success, Try}

/**
  * Handles requests and responses for KV counter decrement operations.
  *
  * @author Graham Pople
  * @since 1.0.0
  */
private[scala] class BinaryDecrementHandler(hp: HandlerParams)
  extends RequestHandler[DecrementResponse, CounterResult] {

  def request[T](id: String,
                 delta: Long,
                 initial: Option[Long] = None,
                 cas: Long = 0,
                 durability: Durability,
                 expiration: java.time.Duration,
                 parentSpan: Option[Span],
                 timeout: java.time.Duration,
                 retryStrategy: RetryStrategy)
  : Try[DecrementRequest] = {

    val validations: Try[DecrementRequest] = for {
      _ <- Validate.notNullOrEmpty(id, "id")
      _ <- Validate.notNull(delta, "delta")
      _ <- Validate.optNotNull(initial, "initial")
      _ <- Validate.notNull(cas, "cas")
      _ <- Validate.notNull(durability, "durability")
      _ <- Validate.notNull(parentSpan, "parentSpan")
      _ <- Validate.notNull(timeout, "timeout")
      _ <- Validate.notNull(retryStrategy, "retryStrategy")
    } yield null

    if (validations.isFailure) {
      validations
    }
    else {
      Success(new DecrementRequest(
        timeout,
        hp.core.context(),
        hp.bucketName,
        retryStrategy,
        id,
        cas,
        hp.collectionIdEncoded,
        delta,
        initial.asJava.map(_.asInstanceOf[java.lang.Long]),
        expiration.getSeconds.toInt,
        durability.toDurabilityLevel
      ))
    }
  }

  def response(id: String, response: DecrementResponse): CounterResult = {
    response.status() match {
      case ResponseStatus.SUCCESS =>
        CounterResult(response.cas(), response.mutationToken().asScala, response.value())

      case _ => throw DefaultErrors.throwOnBadResult(response.status())
    }
  }
}
