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
import com.couchbase.client.core.error.{
  DocumentNotFoundException,
  KeyValueErrorContext,
  ReducedKeyValueErrorContext
}
import com.couchbase.client.core.error.subdoc.SubDocumentException
import com.couchbase.client.core.msg.ResponseStatus
import com.couchbase.client.core.msg.kv._
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.HandlerParams
import com.couchbase.client.scala.codec.{DocumentFlags, Transcoder}
import com.couchbase.client.scala.json.JsonObject
import com.couchbase.client.scala.kv._
import com.couchbase.client.scala.util.{FunctionalUtil, Validate}

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

  def request[T](
      id: String,
      spec: Seq[LookupInSpec],
      withExpiration: Boolean,
      timeout: java.time.Duration,
      retryStrategy: RetryStrategy
  ): Try[SubdocGetRequest] = {
    val validations: Try[SubdocGetRequest] = for {
      _ <- Validate.notNullOrEmpty(id, "id")
      _ <- Validate.notNull(spec, "spec")
      _ <- Validate.notNull(timeout, "timeout")
      _ <- Validate.notNull(retryStrategy, "retryStrategy")
    } yield null

    if (validations.isFailure) {
      validations
    } else {
      val commands = new java.util.ArrayList[SubdocGetRequest.Command]()

      if (withExpiration) {
        commands.add(new SubdocGetRequest.Command(SubdocCommandType.GET, ExpTime, true, spec.size))
      }

      spec.zipWithIndex
        .map {
          case (x: Get, idx) =>
            val cmd = if (x.path == "") SubdocCommandType.GET_DOC else SubdocCommandType.GET
            new SubdocGetRequest.Command(cmd, x.path, x._xattr, idx)
          case (x: Exists, idx) =>
            new SubdocGetRequest.Command(SubdocCommandType.EXISTS, x.path, x._xattr, idx)
          case (x: Count, idx) =>
            new SubdocGetRequest.Command(SubdocCommandType.COUNT, x.path, x._xattr, idx)
        }
        // xattrs need to come first
        .sortBy(!_.xattr())
        .foreach(commands.add)

      if (commands.isEmpty) {
        Failure(new IllegalArgumentException("No SubDocument commands provided"))
      } else {
        Success(
          new SubdocGetRequest(
            timeout,
            hp.core.context(),
            hp.collectionIdentifier,
            retryStrategy,
            id,
            0,
            commands,
            null /* todo: add rto */
          )
        )
      }
    }
  }

  def requestProject[T](
      id: String,
      project: Seq[String],
      timeout: java.time.Duration,
      retryStrategy: RetryStrategy
  ): Try[SubdocGetRequest] = {
    val validations: Try[SubdocGetRequest] = for {
      _ <- Validate.notNullOrEmpty(project, "project")
    } yield null

    if (validations.isFailure) {
      validations
    } else if (project.size > SubdocMutateRequest.SUBDOC_MAX_FIELDS) {
      Failure(
        new IllegalArgumentException(
          s"A maximum of ${SubdocMutateRequest.SUBDOC_MAX_FIELDS} projection fields are supported"
        )
      )
    } else {
      val spec = project.map(v => LookupInSpec.get(v))

      request(id, spec, false, timeout, retryStrategy)
    }
  }

  def response(
      request: KeyValueRequest[SubdocGetResponse],
      id: String,
      response: SubdocGetResponse,
      withExpiration: Boolean,
      transcoder: Transcoder
  ): LookupInResult = {
    response.status() match {
      case ResponseStatus.SUCCESS =>
        val values: Seq[SubDocumentField] = response.values()

        if (withExpiration) {
          var exptime: Option[Duration] = None

          val removingExpTime = values.filter(value => {
            if (value.path() == ExpTime) {
              val str = new java.lang.String(value.value(), CharsetUtil.UTF_8)
              exptime = Some(Duration(str.toLong, TimeUnit.SECONDS))
              false
            } else true
          })

          LookupInResult(
            id,
            removingExpTime,
            DocumentFlags.Json,
            response.cas(),
            exptime,
            transcoder
          )
        } else {
          LookupInResult(id, values, DocumentFlags.Json, response.cas(), None, transcoder)
        }

      case ResponseStatus.NOT_FOUND =>
        val ctx = KeyValueErrorContext.completedRequest(request, response.status())
        throw new DocumentNotFoundException(ctx)

      case ResponseStatus.SUBDOC_FAILURE =>
        response.error().asScala match {
          case Some(err) => throw err
          case _ => {
            val ctx = ReducedKeyValueErrorContext.create(id)
            throw new SubDocumentException("Unknown SubDocument failure occurred", ctx, 0)
          }
        }

      case _ => throw DefaultErrors.throwOnBadResult(request, response)
    }
  }

  def responseProject(
      request: KeyValueRequest[SubdocGetResponse],
      id: String,
      response: SubdocGetResponse,
      transcoder: Transcoder
  ): Try[GetResult] = {
    response.status() match {
      case ResponseStatus.SUCCESS =>
        val values: Seq[SubDocumentField] = response.values()

        val out = JsonObject.create

        val x: Seq[Try[JsonObject]] = values.map(value => {
          ProjectionsApplier.parse(out, value.path(), value.value())
        })

        // If any op failed, return the first failure
        val y: Try[Seq[JsonObject]] = FunctionalUtil.traverse(x.toList)

        y.map(
          _ => GetResult(id, Right(out), DocumentFlags.Json, response.cas(), None, transcoder)
        )

      case ResponseStatus.NOT_FOUND =>
        val ctx = KeyValueErrorContext.completedRequest(request, response.status())
        Failure(new DocumentNotFoundException(ctx))

      case ResponseStatus.SUBDOC_FAILURE =>
        response.error().asScala match {
          case Some(err) => Failure(err)
          case _ => {
            val ctx = ReducedKeyValueErrorContext.create(id)
            Failure(new SubDocumentException("Unknown SubDocument failure occurred", ctx, 0))
          }
        }

      case _ => Failure(DefaultErrors.throwOnBadResult(request, response))
    }
  }
}
