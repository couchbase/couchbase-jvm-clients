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
import com.couchbase.client.core.msg.kv.{KeyValueRequest, UnlockRequest, UnlockResponse}
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.HandlerParams
import com.couchbase.client.scala.kv.DefaultErrors
import com.couchbase.client.scala.util.Validate

import scala.util.{Success, Try}

/**
  * Handles requests and responses for KV unlock operations.
  *
  * @author Graham Pople
  * @since 1.0.0
  */
private[scala] class UnlockHandler(hp: HandlerParams)
    extends KeyValueRequestHandler[UnlockResponse, Unit] {

  def request[T](
      id: String,
      cas: Long,
      timeout: java.time.Duration,
      retryStrategy: RetryStrategy
  ): Try[UnlockRequest] = {
    val validations: Try[UnlockRequest] = for {
      _ <- Validate.notNullOrEmpty(id, "id")
      _ <- Validate.notNull(cas, "cas")
      _ <- Validate.notNull(timeout, "timeout")
      _ <- Validate.notNull(retryStrategy, "retryStrategy")
    } yield null

    if (validations.isFailure) {
      validations
    } else {
      Success(
        new UnlockRequest(
          timeout,
          hp.core.context(),
          hp.collectionIdentifier,
          retryStrategy,
          id,
          cas,
          null /* todo: rto */
        )
      )
    }
  }

  def response(
      request: KeyValueRequest[UnlockResponse],
      id: String,
      response: UnlockResponse
  ): Unit = {
    response.status() match {
      case ResponseStatus.SUCCESS =>
      case _                      => throw DefaultErrors.throwOnBadResult(id, response.status())
    }
  }
}
