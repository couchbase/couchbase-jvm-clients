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
import com.couchbase.client.core.error.context.{KeyValueErrorContext, ReducedKeyValueErrorContext}
import com.couchbase.client.core.error.{
  CasMismatchException,
  CouchbaseException,
  DocumentExistsException
}
import com.couchbase.client.core.msg.ResponseStatus
import com.couchbase.client.core.msg.kv._
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.core.util.BucketConfigUtil
import com.couchbase.client.scala.HandlerParams
import com.couchbase.client.scala.codec.Transcoder
import com.couchbase.client.scala.durability.Durability
import com.couchbase.client.scala.kv._
import com.couchbase.client.scala.util.Validate
import reactor.core.scala.publisher.SMono

import scala.compat.java8.OptionConverters._
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Try}

/**
  * Handles requests and responses for KV SubDocument mutation operations.
  *
  * @author Graham Pople
  * @since 1.0.0
  */
private[scala] class MutateInHandler(hp: HandlerParams) {

  def request[T](
      id: String,
      spec: collection.Seq[MutateInSpec],
      cas: Long,
      document: StoreSemantics = StoreSemantics.Replace,
      durability: Durability,
      expiryEpochTimeSecs: Long,
      preserveExpiry: Boolean,
      timeout: java.time.Duration,
      retryStrategy: RetryStrategy,
      accessDeleted: Boolean,
      createAsDeleted: Boolean,
      transcoder: Transcoder,
      parentSpan: Option[RequestSpan]
  )(implicit ec: ExecutionContext): SMono[SubdocMutateRequest] = {
    val validations: Try[SubdocMutateRequest] = for {
      _ <- Validate.notNullOrEmpty(id, "id")
      _ <- Validate.notNull(cas, "cas")
      _ <- Validate.notNull(durability, "durability")
      _ <- Validate.notNull(expiryEpochTimeSecs, "expiration")
      _ <- Validate.notNull(timeout, "timeout")
      _ <- Validate.notNull(retryStrategy, "retryStrategy")
      _ <- Validate.notNull(parentSpan, "parentSpan")
    } yield null

    if (validations.isFailure) {
      SMono.fromTry(validations)
    } else {
      // Find any decode failure
      val failed = spec.collectFirst {
        case v: MutateInSpecStandard if v.fragment.isFailure => v
      }

      failed match {
        case Some(failed) =>
          // If any of the decodes failed, abort
          SMono.fromTry(Failure(failed.fragment.failed.get))

        case _ =>
          val commands = new java.util.ArrayList[SubdocMutateRequest.Command]()

          spec.zipWithIndex
            .map(v => v._1.convert(v._2))
            // xattrs need to come first
            .sortBy(!_.xattr())
            .foreach(v => commands.add(v))

          if (commands.isEmpty) {
            SMono.fromTry(
              Failure(SubdocMutateRequest.errIfNoCommands(ReducedKeyValueErrorContext.create(id)))
            )
          } else if (commands.size > SubdocMutateRequest.SUBDOC_MAX_FIELDS) {
            SMono.fromTry(
              Failure(
                SubdocMutateRequest.errIfTooManyCommands(ReducedKeyValueErrorContext.create(id))
              )
            )
          } else {
            val requiresBucketConfig = createAsDeleted
            val span =
              hp.tracer.requestSpan(TracingIdentifiers.SPAN_REQUEST_KV_MUTATE_IN, parentSpan.orNull)

            if (requiresBucketConfig) {
              SMono(BucketConfigUtil.waitForBucketConfig(hp.core, hp.bucketName, timeout))
                .flatMap(bucketConfig => {
                  SMono.just(
                    new SubdocMutateRequest(
                      timeout,
                      hp.core.context(),
                      hp.collectionIdentifier,
                      bucketConfig,
                      retryStrategy,
                      id,
                      document == StoreSemantics.Insert,
                      document == StoreSemantics.Upsert,
                      false,
                      accessDeleted,
                      createAsDeleted,
                      commands,
                      expiryEpochTimeSecs,
                      preserveExpiry,
                      cas,
                      durability.toDurabilityLevel,
                      span
                    )
                  )
                })
            } else {
              SMono.just(
                new SubdocMutateRequest(
                  timeout,
                  hp.core.context(),
                  hp.collectionIdentifier,
                  null,
                  retryStrategy,
                  id,
                  document == StoreSemantics.Insert,
                  document == StoreSemantics.Upsert,
                  false,
                  accessDeleted,
                  createAsDeleted,
                  commands,
                  expiryEpochTimeSecs,
                  preserveExpiry,
                  cas,
                  durability.toDurabilityLevel,
                  span
                )
              )
            }
          }
      }
    }
  }

  def response(
      request: KeyValueRequest[SubdocMutateResponse],
      id: String,
      document: StoreSemantics = StoreSemantics.Replace,
      response: SubdocMutateResponse
  ): MutateInResult = {
    response.status() match {

      case ResponseStatus.SUCCESS =>
        val values: Array[SubDocumentField] = response.values()

        MutateInResult(id, values, response.cas(), response.mutationToken().asScala)

      case ResponseStatus.SUBDOC_FAILURE =>
        response.error().asScala match {
          case Some(err) => throw err
          case _ => {
            throw new CouchbaseException(
              "Unknown SubDocument failure occurred",
              KeyValueErrorContext.completedRequest(request, response.status())
            )
          }
        }

      case ResponseStatus.NOT_STORED =>
        document match {
          case StoreSemantics.Insert => {
            val ctx = KeyValueErrorContext.completedRequest(request, response.status())
            throw new DocumentExistsException(ctx)
          }
          case _ => throw DefaultErrors.throwOnBadResult(request, response)
        }

      case ResponseStatus.EXISTS =>
        document match {
          case StoreSemantics.Insert => {
            val ctx = KeyValueErrorContext.completedRequest(request, response.status())
            throw new DocumentExistsException(ctx)
          }
          case _ => {
            val ctx = KeyValueErrorContext.completedRequest(request, response.status())
            throw new CasMismatchException(ctx)
          }
        }

      case _ => throw DefaultErrors.throwOnBadResult(request, response)
    }
  }
}
