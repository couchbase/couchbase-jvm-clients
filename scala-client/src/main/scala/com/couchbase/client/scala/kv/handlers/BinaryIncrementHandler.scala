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

import java.util.Optional

import com.couchbase.client.core.msg.ResponseStatus
import com.couchbase.client.core.msg.kv.{IncrementRequest, IncrementResponse}
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.HandlerParams
import com.couchbase.client.scala.api.CounterResult
import com.couchbase.client.scala.durability.Durability
import com.couchbase.client.scala.kv.DefaultErrors
import com.couchbase.client.scala.util.Validate

import scala.compat.java8.OptionConverters._
import scala.util.{Success, Try}

/**
  * Handles requests and responses for KV increment operations.
  *
  * @author Graham Pople
  * @since 1.0.0
  */
private[scala] class BinaryIncrementHandler(hp: HandlerParams)
    extends RequestHandler[IncrementResponse, CounterResult] {

  def request[T](
      id: String,
      delta: Long,
      initial: Option[Long] = None,
      cas: Long = 0,
      durability: Durability,
      expiration: java.time.Duration,
      timeout: java.time.Duration,
      retryStrategy: RetryStrategy
  ): Try[IncrementRequest] = {

    val validations: Try[IncrementRequest] = for {
      _ <- Validate.notNullOrEmpty(id, "id")
      _ <- Validate.notNull(delta, "delta")
      _ <- Validate.optNotNull(initial, "initial")
      _ <- Validate.notNull(cas, "cas")
      _ <- Validate.notNull(durability, "durability")
      _ <- Validate.notNull(timeout, "timeout")
      _ <- Validate.notNull(retryStrategy, "retryStrategy")
    } yield null

    if (validations.isFailure) {
      validations
    } else {
      val i: Optional[java.lang.Long] = initial match {
        case Some(x) => Optional.of(x.asInstanceOf[java.lang.Long])
        case _       => Optional.empty()
      }

      Success(
        new IncrementRequest(
          timeout,
          hp.core.context(),
          hp.collectionIdentifier,
          retryStrategy,
          id,
          cas,
          delta,
          i,
          expiration.getSeconds.toInt,
          durability.toDurabilityLevel
        )
      )
    }
  }

  def response(id: String, response: IncrementResponse): CounterResult = {
    response.status() match {
      case ResponseStatus.SUCCESS =>
        CounterResult(response.cas(), response.mutationToken().asScala, response.value())

      case _ => throw DefaultErrors.throwOnBadResult(id, response.status())
    }
  }
}
