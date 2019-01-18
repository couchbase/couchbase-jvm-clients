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

import java.time.Duration
import java.util.Optional
import java.util.concurrent.TimeUnit

import com.couchbase.client.core.Core
import com.couchbase.client.core.error.{CouchbaseException, DocumentDoesNotExistException, EncodingFailed}
import com.couchbase.client.core.msg.ResponseStatus
import com.couchbase.client.core.msg.kv._
import com.couchbase.client.core.retry.BestEffortRetryStrategy
import com.couchbase.client.core.util.{UnsignedLEB128, Validators}
import io.circe._
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._

import scala.compat.java8.FunctionConverters._
import com.couchbase.client.scala.api._
import com.couchbase.client.scala.document.{GetResult, JsonObject, JsonType}
import com.couchbase.client.scala.env.ClusterEnvironment
import com.fasterxml.jackson.databind.ObjectMapper
import io.netty.buffer.ByteBuf
import reactor.core.scala.publisher.Mono
import rx.RxReactiveStreams

import scala.compat.java8.FutureConverters
import scala.concurrent.{ExecutionContext, Future, JavaConversions}
import scala.concurrent.duration.{FiniteDuration, _}
import scala.util.{Failure, Success, Try}
import scala.reflect.runtime.universe._
import ujson._

class AsyncCollection(name: String,
                      collectionId: Long,
                      bucketName: String,
                      core: Core,
                      environment: ClusterEnvironment)
                     (implicit ec: ExecutionContext) {
  private val mapper = new ObjectMapper()
  private val kvTimeout = javaDurationToScala(environment.kvTimeout())
  private val collectionIdEncoded = UnsignedLEB128.encode(collectionId)

  def encode[T](content: T)
//                       (implicit tag: TypeTag[T])
  : Try[Array[Byte]] = {
//    Try.apply(mapper.writeValueAsBytes(content))
    content match {
        // My JsonType
      case v: JsonType =>
        val json = v.asJson
          json.as[Array[Byte]].toTry

        // circe's Json
      case v: Json =>
        v.as[Array[Byte]].toTry

        // ujson's Json
      case v: ujson.Value =>
        Try(transform(v, BytesRenderer()).toBytes)

      case _ =>
        // TODO get circe working
//        val circe = content.asJson
        val dbg = mapper.writeValueAsString(content)
        Try.apply(mapper.writeValueAsBytes(content))
//          circe.as[Array[Byte]].toTry
    }
  }

  implicit def scalaDurationToJava(in: scala.concurrent.duration.FiniteDuration): java.time.Duration = {
    java.time.Duration.ofNanos(in.toNanos)
  }

  implicit def javaDurationToScala(in: java.time.Duration): scala.concurrent.duration.FiniteDuration = {
    FiniteDuration.apply(in.toNanos, TimeUnit.NANOSECONDS)
  }

  def insert[T](id: String,
                content: T,
                timeout: FiniteDuration = kvTimeout,
                expiration: FiniteDuration = 0.seconds,
                replicateTo: ReplicateTo.Value = ReplicateTo.None,
                persistTo: PersistTo.Value = PersistTo.None
               )
//               (implicit tag: TypeTag[T])
  : Future[MutationResult] = {
    Validators.notNullOrEmpty(id, "id")
    Validators.notNull(content, "content")

    // TODO custom encoders
    encode(content) match {
      case Success(encoded) =>
        // TODO flags
        // TODO datatype
        // TODO retry strategies
        val retryStrategy = BestEffortRetryStrategy.INSTANCE
        val request = new InsertRequest(id,
          collectionIdEncoded, encoded, expiration.toSeconds, 0, timeout, core.context(), bucketName, retryStrategy)
        core.send(request)
        FutureConverters.toScala(request.response())
          .map(response => {
            // TODO MVP error handling
            // TODO MVP MutationTokens
            MutationResult(response.cas(), Option.empty)

            // TODO
            //        if (response.status().isSuccess) {
            //          val out = JSON_OBJECT_TRANSCODER.newDocument(doc.id(), doc.expiry(), doc.content(), response.cas(), response.mutationToken())
            //          out
            //        }
            //        // TODO move this to core
            //        else response.status() match {
            //          case ResponseStatus.TOO_BIG =>
            //            throw addDetails(new RequestTooBigException, response)
            //          case ResponseStatus.EXISTS =>
            //            throw addDetails(new DocumentAlreadyExistsException, response)
            //          case ResponseStatus.TEMPORARY_FAILURE | ResponseStatus.SERVER_BUSY =>
            //            throw addDetails(new TemporaryFailureException, response)
            //          case ResponseStatus.OUT_OF_MEMORY =>
            //            throw addDetails(new CouchbaseOutOfMemoryException, response)
            //          case _ =>
            //            throw addDetails(new CouchbaseException(response.status.toString), response)
            //        }

          })
      case Failure(err) =>
        Future.failed(new EncodingFailed(err))
    }
  }

  def insert[T](id: String,
                content: T,
                options: InsertOptions
               )
               (implicit tag: TypeTag[T]): Future[MutationResult] = {
    Validators.notNull(options, "options")
    insert(id, content, options.timeout, options.expiration, options.replicateTo, options.persistTo)
  }

  def replace[T](id: String,
                 content: T,
                 cas: Long,
                 timeout: FiniteDuration = kvTimeout,
                 expiration: FiniteDuration = 0.seconds,
                 replicateTo: ReplicateTo.Value = ReplicateTo.None,
                 persistTo: PersistTo.Value = PersistTo.None
                ): Future[MutationResult] = {
    Validators.notNullOrEmpty(id, "id")
    Validators.notNull(content, "content")
    Validators.notNull(cas, "cas")

    null
  }

  def replace[T](id: String,
                 content: T,
                 cas: Long,
                 options: ReplaceOptions
                ): Future[MutationResult] = {
    replace(id, content, cas, options.timeout, options.expiration, options.replicateTo, options.persistTo)
  }

  def upsert(id: String,
             content: JsonObject,
             cas: Long,
             timeout: FiniteDuration = kvTimeout,
             expiration: FiniteDuration = 0.seconds,
             replicateTo: ReplicateTo.Value = ReplicateTo.None,
             persistTo: PersistTo.Value = PersistTo.None
            ): Future[MutationResult] = {
    Validators.notNullOrEmpty(id, "id")
    Validators.notNull(content, "content")
    Validators.notNull(cas, "cas")

    null
  }

  def upsert(id: String,
             content: JsonObject,
             cas: Long,
             options: UpsertOptions
            ): Future[MutationResult] = {
    upsert(id, content, cas, options.timeout, options.expiration, options.replicateTo, options.persistTo)
  }


  def remove(id: String,
             cas: Long,
             timeout: FiniteDuration = kvTimeout,
             replicateTo: ReplicateTo.Value = ReplicateTo.None,
             persistTo: PersistTo.Value = PersistTo.None
            ): Future[MutationResult] = {
    Validators.notNullOrEmpty(id, "id")
    Validators.notNull(cas, "cas")

    null
  }

  def remove(id: String,
             cas: Long,
             options: RemoveOptions
            ): Future[MutationResult] = {
    remove(id, cas, options.timeout)
  }

  def lookupInAs[T](id: String,
                    operations: GetSpec,
                    timeout: FiniteDuration = kvTimeout)
                   : Future[T] = {
    return null;
  }

  def get(id: String,
          timeout: FiniteDuration = kvTimeout)
         : Future[GetResult] = {

    Validators.notNullOrEmpty(id, "id")
    Validators.notNull(timeout, "timeout")

    // TODO support projections
    // TODO support expiration
    val retryStrategy = BestEffortRetryStrategy.INSTANCE

    val request = new GetRequest(id, collectionIdEncoded, timeout, core.context(), bucketName, retryStrategy)

    core.send(request)

    FutureConverters.toScala(request.response())
      .map(response => {
        if (response.status.success) {
          new GetResult(id, response.content, response.cas, Option.empty)
        }
        else if (response.status == ResponseStatus.NOT_FOUND) {
          throw new DocumentDoesNotExistException()
        }
        else { // todo: implement me
          throw new UnsupportedOperationException("fixme")
        }

      })

//    // TODO retry strategies
//    val request = new InsertRequest(id,
//      collectionIdEncoded, encoded, expiration.toSeconds, 0, timeout, core.context(), bucketName, retryStrategy)
//    core.send(request)

//    val retryStrategy = BestEffortRetryStrategy.INSTANCE // todo fixme from env
//    val bucketName = "BUCKETNAME" // todo fixme from constructor chain
//    val collection = null
//
//    val request = new GetRequest(id, collection, timeout, coreContext, bucketName, retryStrategy)
//    core.send(request)
//    FutureConverters.toScala(request.response())
//      .map(v => {
//        if (v.status() == ResponseStatus.NOT_FOUND) {
//          None
//        }
//        else if (v.status() != ResponseStatus.SUCCESS) {
//          // TODO
//          throw new CouchbaseException()
//        }
//        else {
//          // TODO
//          //          val content = JsonObject.create()
//          //          Some(new GetResult(id, v.cas(), v.content()))
//          null
//        }
//      })

    //    val request = new GetRequest(id, Duration.ofNanos(timeout.toNanos), coreContext)
    //
    //    dispatch[GetRequest, GetResponse](request)
    //      .map(response => {
    //        if (response.status().success()) {
    //          val content = mapper.readValue(response.content(), classOf[JsonObject])
    ////          val doc = JSON_OBJECT_TRANSCODER.decode(id, response.content(), response.cas(), 0, response.flags(), response.status())
    //          val doc = JsonDocument.create(id, content, response.cas())
    //          Option(doc)
    //        }
    //        // TODO move this to core
    //        else response.status match {
    //          case ResponseStatus.NOT_FOUND =>
    //            Option.empty[JsonDocument]
    //          case _ =>
    //            throw addDetails(new CouchbaseException(response.status.toString), response)
    //        }
    //      })
  }

  def get(id: String,
          options: GetOptions
         ): Future[GetResult] = {
    get(id, options.timeout)
  }

//  def getOrError(id: String,
//                 timeout: FiniteDuration = kvTimeout)
//                : Future[GetResult] = {
//    get(id, timeout).map(doc => {
//      if (doc.isEmpty) throw new DocumentDoesNotExistException()
//      else doc.get
//    })
//  }
//
//  def getOrError(id: String,
//                 options: GetOptions)
//                : Future[GetResult] = {
//    getOrError(id, options.timeout)
//  }

  def getAndLock(id: String,
                 lockFor: FiniteDuration,
                 timeout: FiniteDuration = kvTimeout)
                 = Future {
    Option.empty
  }

  def getAndLock(id: String,
                 lockFor: FiniteDuration,
                 options: GetAndLockOptions)
                 = Future {
    Option.empty
  }


}
