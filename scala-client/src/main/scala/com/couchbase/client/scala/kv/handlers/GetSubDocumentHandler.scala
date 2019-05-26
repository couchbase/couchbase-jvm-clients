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

import java.util.concurrent.TimeUnit

import com.couchbase.client.core.deps.io.netty.util.CharsetUtil
import com.couchbase.client.core.error.subdoc.SubDocumentException
import com.couchbase.client.core.msg.ResponseStatus
import com.couchbase.client.core.msg.kv._
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.HandlerParams
import com.couchbase.client.scala.codec.DocumentFlags
import com.couchbase.client.scala.json.JsonObject
import com.couchbase.client.scala.kv._
import com.couchbase.client.scala.util.{FunctionalUtil, Validate}
import io.opentracing.Span

import scala.collection.JavaConverters._
import scala.compat.java8.OptionConverters._
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}


/**
  * Handles requests and responses for KV SubDocument operations.
  *
  * @author Graham Pople
  * @since 1.0.0
  */
private[scala] class GetSubDocumentHandler(hp: HandlerParams) {
  private val ExpTime = "$document.exptime"

  def request[T](id: String,
                 spec: Seq[LookupInSpec],
                 withExpiration: Boolean,
                 parentSpan: Option[Span],
                 timeout: java.time.Duration,
                 retryStrategy: RetryStrategy)
  : Try[SubdocGetRequest] = {
    val validations: Try[SubdocGetRequest] = for {
      _ <- Validate.notNullOrEmpty(id, "id")
      _ <- Validate.notNull(spec, "spec")
      _ <- Validate.notNull(parentSpan, "parentSpan")
      _ <- Validate.notNull(timeout, "timeout")
      _ <- Validate.notNull(retryStrategy, "retryStrategy")
    } yield null

    if (validations.isFailure) {
      validations
    }
    else {
      val commands = new java.util.ArrayList[SubdocGetRequest.Command]()

      // Put expiration on the end so it doesn't mess up indexing
      // Update: no, all xattr commands need to at start. But only support expiration with full doc anyway (to avoid
      // app accidentally going over 16 subdoc commands), so can put it here
      if (withExpiration) {
        commands.add(new SubdocGetRequest.Command(SubdocCommandType.GET, ExpTime, true))
      }

      spec.map {
        case x: Get => new SubdocGetRequest.Command(SubdocCommandType.GET, x.path, x._xattr)
        case x: GetFullDocument => new SubdocGetRequest.Command(SubdocCommandType.GET_DOC, "", false)
        case x: Exists => new SubdocGetRequest.Command(SubdocCommandType.EXISTS, x.path, x._xattr)
        case x: Count => new SubdocGetRequest.Command(SubdocCommandType.COUNT, x.path, x._xattr)
      }.foreach(commands.add)

      if (commands.isEmpty) {
        Failure(new IllegalArgumentException("No SubDocument commands provided"))
      }
      else {
        Success(new SubdocGetRequest(timeout,
          hp.core.context(),
          hp.collectionIdentifier,
          retryStrategy,
          id,
          0,
          commands))
      }
    }
  }

  def requestProject[T](id: String,
                 project: Seq[String],
                 parentSpan: Option[Span],
                 timeout: java.time.Duration,
                 retryStrategy: RetryStrategy)
  : Try[SubdocGetRequest] = {
    val validations: Try[SubdocGetRequest] = for {
      _ <- Validate.notNullOrEmpty(project, "project")
    } yield null

    if (validations.isFailure) {
      validations
    }
    else if (project.size > SubdocMutateRequest.SUBDOC_MAX_FIELDS) {
      Failure(new IllegalArgumentException(s"A maximum of ${SubdocMutateRequest.SUBDOC_MAX_FIELDS} projection fields are supported"))
    }
    else {
      val spec = project.map(v => LookupInSpec.get(v))

      request(id, spec, false, parentSpan, timeout, retryStrategy)
    }
  }

  def response(id: String, response: SubdocGetResponse): Option[LookupInResult] = {
    response.status() match {
      case ResponseStatus.SUCCESS =>
        val values: Seq[SubdocField] = response.values().asScala

        var exptime: Option[Duration] = None

        values.foreach(value => {
          if (value.path() == ExpTime) {
            val str = new java.lang.String(value.value(), CharsetUtil.UTF_8)
            exptime = Some(Duration(str.toLong, TimeUnit.SECONDS))
          }
        })

        Some(LookupInResult(id, values, DocumentFlags.Json, response.cas(), exptime))


      case ResponseStatus.NOT_FOUND => None

      case ResponseStatus.SUBDOC_FAILURE =>

        response.error().asScala match {
          case Some(err) => throw err
          case _ => throw new SubDocumentException("Unknown SubDocument failure occurred") {}
        }

      case _ => throw DefaultErrors.throwOnBadResult(id, response.status())
    }
  }

  def responseProject(id: String, response: SubdocGetResponse): Try[Option[GetResult]] = {
    response.status() match {
      case ResponseStatus.SUCCESS =>
        val values: Seq[SubdocField] = response.values().asScala

        val out = JsonObject.create

        val x: Seq[Try[JsonObject]] = values.map(value => {
          ProjectionsApplier.parse(out, value.path(), value.value())
        })

        // If any op failed, return the first failure
        val y: Try[Seq[JsonObject]] = FunctionalUtil.traverse(x.toList)

        y.map(_ => Some(GetResult(id, Right(out), DocumentFlags.Json, response.cas(), None)))

      case ResponseStatus.NOT_FOUND => Success(None)

      case ResponseStatus.SUBDOC_FAILURE =>

        response.error().asScala match {
          case Some(err) => Failure(err)
          case _ => Failure(new SubDocumentException("Unknown SubDocument failure occurred") {})
        }

      case _ => Failure(DefaultErrors.throwOnBadResult(id, response.status()))
    }
  }
}