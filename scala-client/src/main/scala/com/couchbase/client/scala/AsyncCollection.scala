/*
 * Copyright (c) 2018 Couchbase, Inc.
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

package com.couchbase.client.scala

import java.nio.charset.Charset
import java.time.Duration
import java.util.Optional
import java.util.concurrent.TimeUnit

import com.couchbase.client.core.Core
import com.couchbase.client.core.error._
import com.couchbase.client.core.error.subdoc.SubDocumentException
import com.couchbase.client.core.msg.ResponseStatus
import com.couchbase.client.core.msg.kv._
import com.couchbase.client.core.retry.{BestEffortRetryStrategy, RetryStrategy}
import com.couchbase.client.core.util.{UnsignedLEB128, Validators}
import io.circe._
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._

import scala.compat.java8.FunctionConverters._
import com.couchbase.client.scala.api._
import com.couchbase.client.scala.document.{LookupInResult, _}
import com.couchbase.client.scala.durability.{Disabled, Durability}
import com.couchbase.client.scala.env.ClusterEnvironment
import com.fasterxml.jackson.databind.ObjectMapper
import io.netty.buffer.ByteBuf
import io.netty.util.CharsetUtil
import io.opentracing.Span
import reactor.core.scala.publisher.Mono
import rx.RxReactiveStreams

import scala.compat.java8.FutureConverters
import scala.concurrent.{ExecutionContext, Future, JavaConversions}
import scala.concurrent.duration.{Duration, FiniteDuration, _}
import scala.util.{Failure, Success, Try}
import scala.reflect.runtime.universe._
import ujson._

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.compat.java8.OptionConverters._
import collection.JavaConverters._


class AsyncCollection(name: String,
                      collectionId: Long,
                      bucketName: String,
                      core: Core,
                      val environment: ClusterEnvironment)
                     (implicit ec: ExecutionContext) {
  private val mapper = new ObjectMapper()
  private val kvTimeout = javaDurationToScala(environment.kvTimeout())
  private val collectionIdEncoded = UnsignedLEB128.encode(collectionId)

  import annotation.implicitNotFound

//  @implicitNotFound("No member of type class in scope for ${T}")
//  def encode[T](content: T)
//               (implicit ev: Conversions.Encodable[T])
//  : Try[Array[Byte]] = {
//    //    Try.apply(mapper.writeValueAsBytes(content))
//
//    ev.encode(content).map(_._1)
//
//    //    content match {
//    //        // My JsonType
//    //        // TODO MVP probably remove my JsonType
//    ////      case v: JsonType =>
//    ////        val json = v.asJson
//    ////          json.as[Array[Byte]].toTry
//    //
//    //        // circe's Json
//    //      case v: Json =>
//    //        v.as[Array[Byte]].toTry
//    //
//    //        // ujson's Json
//    //      case v: ujson.Value =>
//    //        Try(transform(v, BytesRenderer()).toBytes)
//    //
//    //      case v: Array[Byte] =>
//    //        // TODO check it's not MessagePack from upickle.default.writeBinary
//    //        Try(v)
//    //
//    //      case _ =>
//    //        // TODO MVP support json string
//    ////        val circe = content.asJson
//    //        val dbg = mapper.writeValueAsString(content)
//    //        Try.apply(mapper.writeValueAsBytes(content))
//    ////          circe.as[Array[Byte]].toTry
//    //    }
//  }


  implicit def scalaDurationToJava(in: scala.concurrent.duration.FiniteDuration): java.time.Duration = {
    java.time.Duration.ofNanos(in.toNanos)
  }

  implicit def javaDurationToScala(in: java.time.Duration): scala.concurrent.duration.FiniteDuration = {
    FiniteDuration.apply(in.toNanos, TimeUnit.NANOSECONDS)
  }

  private def throwOnBadResult(status: ResponseStatus): RuntimeException = {
    status match {
      case ResponseStatus.EXISTS => new DocumentAlreadyExistsException()
      case ResponseStatus.LOCKED | ResponseStatus.TEMPORARY_FAILURE => new TemporaryLockFailureException()
      case ResponseStatus.NOT_FOUND => new DocumentDoesNotExistException()
      case ResponseStatus.SERVER_BUSY => new TemporaryFailureException()
      case ResponseStatus.OUT_OF_MEMORY => new CouchbaseOutOfMemoryException()
      // TODO remaining failures
    }
  }

  def exists[T](id: String,
                parentSpan: Option[Span] = None,
                timeout: FiniteDuration = kvTimeout,
                retryStrategy: RetryStrategy = environment.retryStrategy()
               )
  : Future[ExistsResult] = {
    val request = new ObserveViaCasRequest(timeout, core.context(), bucketName, retryStrategy, id, collectionIdEncoded)

    core.send(request)

    FutureConverters.toScala(request.response())
      .map(response => {
        response.status() match {
          case ResponseStatus.SUCCESS =>
            val exists: Boolean = response.observeStatus() match {
              case ObserveViaCasResponse.ObserveStatus.FOUND_PERSISTED | ObserveViaCasResponse.ObserveStatus.FOUND_NOT_PERSISTED => true
              case _ => false
            }

            ExistsResult(exists)

          case _ => throw throwOnBadResult(response.status())
        }
      })
  }

  def insert[T](id: String,
                content: T,
                durability: Durability = Disabled,
                expiration: FiniteDuration = 0.seconds,
                parentSpan: Option[Span] = None,
                timeout: FiniteDuration = kvTimeout,
                retryStrategy: RetryStrategy = environment.retryStrategy()
               )
               (implicit ev: Conversions.Encodable[T])
  : Future[MutationResult] = {
    // TODO validation with Try
    Validators.notNullOrEmpty(id, "id")
    Validators.notNull(content, "content")
    Validators.notNull(content, "timeout")

    ev.encode(content) match {
      case Success(encoded) =>
        val request = new InsertRequest(id,
          collectionIdEncoded, encoded._1, expiration.toSeconds, encoded._2.flags, timeout, core.context(), bucketName, retryStrategy, durability.toDurabilityLevel)
        core.send(request)
        FutureConverters.toScala(request.response())
          .map(response => {
            response.status() match {
              case ResponseStatus.EXISTS =>
                throw new DocumentAlreadyExistsException()
              case ResponseStatus.SUCCESS =>
                MutationResult(response.cas(), response.mutationToken().asScala)
            }
          })
      case Failure(err) =>
        Future.failed(new EncodingFailedException(err))
    }
  }

  def replace[T](id: String,
                 content: T,
                 cas: Long = 0,
                 durability: Durability = Disabled,
                 expiration: FiniteDuration = 0.seconds,
                 parentSpan: Option[Span] = None,
                 timeout: FiniteDuration = kvTimeout,
                 retryStrategy: RetryStrategy = environment.retryStrategy()
                )
                (implicit ev: Conversions.Encodable[T]): Future[MutationResult] = {
    Validators.notNullOrEmpty(id, "id")
    Validators.notNull(content, "content")
    Validators.notNull(cas, "cas")

    ev.encode(content) match {
      case Success(encoded) =>
        val request = new ReplaceRequest(id,
          collectionIdEncoded, encoded._1, expiration.toSeconds, encoded._2.flags, timeout, cas, core.context(), bucketName, retryStrategy, durability.toDurabilityLevel)
        core.send(request)
        FutureConverters.toScala(request.response())
          .map(response => {
            response.status() match {
              case ResponseStatus.SUCCESS =>
                MutationResult(response.cas(), response.mutationToken().asScala)
              case _ => throw throwOnBadResult(response.status())
            }
          })
      case Failure(err) =>
        Future.failed(new EncodingFailedException(err))
    }
  }

  def upsert[T](id: String,
                content: T,
                durability: Durability = Disabled,
                expiration: FiniteDuration = 0.seconds,
                parentSpan: Option[Span] = None,
                timeout: FiniteDuration = kvTimeout,
                retryStrategy: RetryStrategy = environment.retryStrategy()
               )
               (implicit ev: Conversions.Encodable[T]): Future[MutationResult] = {
    Validators.notNullOrEmpty(id, "id")
    Validators.notNull(content, "content")

    ev.encode(content) match {
      case Success(encoded) =>
        val request = new UpsertRequest(id,
          collectionIdEncoded, encoded._1, expiration.toSeconds, encoded._2.flags, timeout, core.context(), bucketName, retryStrategy, durability.toDurabilityLevel)
        core.send(request)
        FutureConverters.toScala(request.response())
          .map(response => {
            response.status() match {
              case ResponseStatus.SUCCESS =>
                MutationResult(response.cas(), response.mutationToken().asScala)
              case _ => throw throwOnBadResult(response.status())
            }
          })
      case Failure(err) =>
        Future.failed(new EncodingFailedException(err))
    }
  }


  def remove(id: String,
             cas: Long = 0,
             durability: Durability = Disabled,
             parentSpan: Option[Span] = None,
             timeout: FiniteDuration = kvTimeout,
             retryStrategy: RetryStrategy = environment.retryStrategy()
            ): Future[MutationResult] = {
    Validators.notNullOrEmpty(id, "id")
    Validators.notNull(cas, "cas")

    val request = new RemoveRequest(id,
      collectionIdEncoded, cas, timeout, core.context(), bucketName, retryStrategy, durability.toDurabilityLevel)
    core.send(request)
    FutureConverters.toScala(request.response())
      .map(response => {
        response.status() match {
          case ResponseStatus.SUCCESS =>
            MutationResult(response.cas(), response.mutationToken().asScala)
          case _ => throw throwOnBadResult(response.status())
        }
      })
  }

  def get(id: String,
          withExpiration: Boolean = false,
          parentSpan: Option[Span] = None,
          timeout: FiniteDuration = kvTimeout,
          retryStrategy: RetryStrategy = environment.retryStrategy())
  : Future[GetResult] = {
    Validators.notNullOrEmpty(id, "id")
    Validators.notNull(timeout, "timeout")

    if (withExpiration) {
      getSubDoc(id, LookupInSpec.getDoc, withExpiration, parentSpan, timeout, retryStrategy).map(lookupInResult =>
        GetResult(id, lookupInResult.documentAsBytes.get, lookupInResult.flags, lookupInResult.cas, lookupInResult.expiration))
    }
    else {
      getFullDoc(id, parentSpan, timeout, retryStrategy)
    }
  }

  private def getFullDoc(id: String,
                         parentSpan: Option[Span] = None,
                         timeout: FiniteDuration = kvTimeout,
                         retryStrategy: RetryStrategy = environment.retryStrategy()): Future[GetResult] = {
    val request = new GetRequest(id, collectionIdEncoded, timeout, core.context(), bucketName, retryStrategy)

    core.send(request)

    FutureConverters.toScala(request.response())
      .map(response => {
        response.status() match {
          case ResponseStatus.SUCCESS =>
            new GetResult(id, response.content, response.flags(), response.cas, Option.empty)

          case _ => throw throwOnBadResult(response.status())
        }
      })
  }

  private val ExpTime = "$document.exptime"

  private def getSubDoc(id: String,
                        spec: LookupInSpec,
                        withExpiration: Boolean,
                        parentSpan: Option[Span] = None,
                        timeout: FiniteDuration = kvTimeout,
                        retryStrategy: RetryStrategy = environment.retryStrategy()): Future[LookupInResult] = {
    // TODO support projections
    // TODO support expiration (probs works, check unit tested)

    val commands = new java.util.ArrayList[SubdocGetRequest.Command]()

    if (withExpiration) {
      commands.add(new SubdocGetRequest.Command(SubdocCommandType.GET, ExpTime, true))
    }

    spec.operations.map {
      case x: GetOperation => new SubdocGetRequest.Command(SubdocCommandType.GET, x.path, x.xattr)
      case x: GetFullDocumentOperation => new SubdocGetRequest.Command(SubdocCommandType.GET_DOC, "", false)
      case x: ExistsOperation => new SubdocGetRequest.Command(SubdocCommandType.EXISTS, x.path, x.xattr)
      case x: CountOperation => new SubdocGetRequest.Command(SubdocCommandType.COUNT, x.path, x.xattr)
    }.foreach(commands.add)


    if (commands.isEmpty) {
      Future.failed(new IllegalArgumentException("No SubDocument commands provided"))
    }
    else {
      // TODO flags?
      val request = new SubdocGetRequest(timeout, core.context(), bucketName, retryStrategy, id, collectionIdEncoded, 0, commands)

      core.send(request)

      FutureConverters.toScala(request.response())
        .map(response => {
          response.status() match {

            case ResponseStatus.SUCCESS =>
              val values = response.values().asScala

              var exptime: Option[FiniteDuration] = None
              var fulldoc: Option[Array[Byte]] = None
              val fields = collection.mutable.Map.empty[String, SubdocField]

              values.foreach(value => {
                if (value.path() == ExpTime) {
                  val str = new java.lang.String(value.value(), CharsetUtil.UTF_8)
                  exptime = Some(FiniteDuration(str.toLong, TimeUnit.SECONDS))
                }
                else if (value.path == "") {
                  fulldoc = Some(value.value())
                }
                else {
                  fields += value.path() -> value
                }
              })

              LookupInResult(id, fulldoc, fields, DocumentFlags.Json, response.cas(), exptime)

            case ResponseStatus.SUBDOC_FAILURE =>

              response.error().asScala match {
                case Some(err) => throw err
                case _ => throw new SubDocumentException("Unknown SubDocument failure occurred") {}
              }

            case _ => throw throwOnBadResult(response.status())
          }
        })
    }
  }

  def mutateIn(id: String,
               spec: MutateInSpec,
               cas: Long = 0,
               insertDocument: Boolean = false,
               durability: Durability = Disabled,
               parentSpan: Option[Span] = None,
               expiration: FiniteDuration,
               timeout: FiniteDuration = kvTimeout,
               retryStrategy: RetryStrategy = environment.retryStrategy()): Future[MutateInResult] = {
    val failed: Option[MutateOperation] = spec.operations
      .filter(_.isInstanceOf[MutateOperationSimple])
        .find(v => v.asInstanceOf[MutateOperationSimple].fragment.isFailure)


    failed match {
      case Some(failed: MutateOperationSimple) =>
        // If any of the decodes failed, abort
        Future.failed(failed.fragment.failed.get)
      case _ =>

        val commands = new java.util.ArrayList[SubdocMutateRequest.Command]()
        spec.operations.map(_.convert).foreach(commands.add)

        if (commands.isEmpty) {
          Future.failed(new IllegalArgumentException("No SubDocument commands provided"))
        }
        else if (commands.size > SubdocMutateRequest.SUBDOC_MAX_FIELDS) {
          Future.failed(new IllegalArgumentException(s"A maximum of ${SubdocMutateRequest.SUBDOC_MAX_FIELDS} fields can be provided"))
        }
        else {
          val request = new SubdocMutateRequest(timeout, core.context(), bucketName, retryStrategy, id,
            collectionIdEncoded, insertDocument, commands,  expiration.toSeconds, durability.toDurabilityLevel)

          core.send(request)

          FutureConverters.toScala(request.response())
            .map(response => {

              response.status() match {

                case ResponseStatus.SUCCESS =>
                  val values: Seq[SubdocField] = response.values().asScala
                  val fields: Map[String, SubdocField] = values.map(v => v.path() -> v).toMap

                  MutateInResult(id, fields, response.cas(), response.mutationToken().asScala)

                case ResponseStatus.SUBDOC_FAILURE =>

                  response.error().asScala match {
                    case Some(err) => throw err
                    case _ => throw new SubDocumentException("Unknown SubDocument failure occurred") {}
                  }

                case _ => throw throwOnBadResult(response.status())
              }
            })
        }
    }
  }


  def getAndLock(id: String,
                 lockFor: FiniteDuration = 30.seconds,
                 parentSpan: Option[Span] = None,
                 timeout: FiniteDuration = kvTimeout,
                 retryStrategy: RetryStrategy = environment.retryStrategy()
                ): Future[GetResult] = {
    val request = new GetAndLockRequest(id, collectionIdEncoded, timeout, core.context(), bucketName, retryStrategy, lockFor)

    core.send(request)

    FutureConverters.toScala(request.response())
      .map(response => {
        response.status() match {
          case ResponseStatus.SUCCESS =>
            new GetResult(id, response.content, response.flags(), response.cas, Option.empty)

          case _ => throw throwOnBadResult(response.status())
        }
      })
  }

  def getAndTouch(id: String,
                  expiration: FiniteDuration,
                  parentSpan: Option[Span] = None,
                  durability: Durability = Disabled,
                  timeout: FiniteDuration = kvTimeout,
                  retryStrategy: RetryStrategy = environment.retryStrategy()
                 ): Future[GetResult] = {
    val request = new GetAndTouchRequest(id, collectionIdEncoded, timeout, core.context(),
      bucketName, retryStrategy, expiration, durability.toDurabilityLevel)

    core.send(request)

    FutureConverters.toScala(request.response())
      .map(response => {
        response.status() match {
          case ResponseStatus.SUCCESS =>
            new GetResult(id, response.content, response.flags(), response.cas, Option.empty)

          case _ => throw throwOnBadResult(response.status())
        }
      })
  }


  def lookupIn(id: String,
               spec: LookupInSpec,
               parentSpan: Option[Span] = None,
               timeout: FiniteDuration = kvTimeout,
               retryStrategy: RetryStrategy = environment.retryStrategy()
              ): Future[LookupInResult] = {
    // Set withExpiration to false as it makes all subdoc lookups multi operations, which changes semantics - app
    // may expect error to be raised and it won't
    getSubDoc(id, spec, false, parentSpan, timeout, retryStrategy)
  }

}
