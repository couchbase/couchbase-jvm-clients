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
import com.couchbase.client.core.config.CouchbaseBucketConfig
import com.couchbase.client.core.error.CommonExceptions
import com.couchbase.client.core.msg.ResponseStatus
import com.couchbase.client.core.msg.kv.{
  GetRequest,
  GetResponse,
  KeyValueRequest,
  ReplicaGetRequest
}
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.HandlerParams
import com.couchbase.client.scala.codec.Transcoder
import com.couchbase.client.scala.kv.{DefaultErrors, GetReplicaResult, GetResult}
import com.couchbase.client.scala.util.Validate

import scala.util.{Failure, Success, Try}

/** Handles requests and responses for KV get-from-replica operations.
  *
  * @author Graham Pople
  * @since 1.0.0
  */
private[scala] class GetFromReplicaHandler(hp: HandlerParams) {

  def requestAll[T](
      id: String,
      timeout: java.time.Duration,
      retryStrategy: RetryStrategy,
      parentSpan: Option[RequestSpan]
  ): Try[Seq[GetRequest]] = {
    val validations: Try[Seq[GetRequest]] = for {
      _ <- Validate.notNullOrEmpty(id, "id")
      _ <- Validate.notNull(timeout, "timeout")
      _ <- Validate.notNull(retryStrategy, "retryStrategy")
      _ <- Validate.notNull(parentSpan, "parentSpan")
    } yield null

    if (validations.isFailure) {
      validations
    } else {
      hp.core.clusterConfig().bucketConfig(hp.bucketName) match {
        case config: CouchbaseBucketConfig =>
          val numReplicas = config.numberOfReplicas()

          val replicaRequests: Seq[ReplicaGetRequest] = Range(0, numReplicas)
            .map(replicaIndex =>
              new ReplicaGetRequest(
                id,
                timeout,
                hp.core.context(),
                hp.collectionIdentifier,
                retryStrategy,
                (replicaIndex + 1).shortValue(),
                hp.tracer
                  .requestSpan(TracingIdentifiers.SPAN_REQUEST_KV_GET_REPLICA, parentSpan.orNull)
              )
            )

          val activeRequest =
            new GetRequest(
              id,
              timeout,
              hp.core.context(),
              hp.collectionIdentifier,
              retryStrategy,
              hp.tracer.requestSpan(TracingIdentifiers.SPAN_REQUEST_KV_GET, parentSpan.orNull)
            )

          val requests: Seq[GetRequest] = activeRequest +: replicaRequests

          Success(requests)

        case _ =>
          Failure(CommonExceptions.getFromReplicaNotCouchbaseBucket)
      }
    }
  }

  def response(
      request: KeyValueRequest[GetResponse],
      id: String,
      response: GetResponse,
      isReplica: Boolean,
      transcoder: Transcoder
  ): Option[GetReplicaResult] = {
    response.status() match {
      case ResponseStatus.SUCCESS =>
        Some(
          new GetReplicaResult(
            id,
            Left(response.content),
            response.flags(),
            response.cas,
            Option.empty,
            isReplica,
            transcoder
          )
        )

      case ResponseStatus.NOT_FOUND => None

      case _ => throw DefaultErrors.throwOnBadResult(request, response)
    }
  }

}
