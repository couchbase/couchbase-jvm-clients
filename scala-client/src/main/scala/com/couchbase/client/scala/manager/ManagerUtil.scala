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
package com.couchbase.client.scala.manager

import java.nio.charset.StandardCharsets.UTF_8

import com.couchbase.client.core.Core
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled
import com.couchbase.client.core.deps.io.netty.handler.codec.http.{
  DefaultFullHttpRequest,
  HttpHeaderValues,
  HttpMethod,
  HttpVersion
}
import com.couchbase.client.core.error.CouchbaseException
import com.couchbase.client.core.msg.ResponseStatus
import com.couchbase.client.core.msg.manager.{GenericManagerRequest, GenericManagerResponse}
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.core.util.UrlQueryStringBuilder
import com.couchbase.client.scala.util.DurationConversions._
import com.couchbase.client.scala.util.FutureConversions
import reactor.core.scala.publisher.SMono

import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

object ManagerUtil {
  def sendRequest(core: Core, request: GenericManagerRequest): SMono[GenericManagerResponse] = {
    SMono.defer(() => {
      core.send(request)
      FutureConversions
        .javaCFToScalaMono(request, request.response, true)
        .doOnTerminate(() => request.context().logicallyComplete())
    })
  }

  def sendRequest(
      core: Core,
      method: HttpMethod,
      path: String,
      timeout: Duration,
      retryStrategy: RetryStrategy
  ): SMono[GenericManagerResponse] = {
    val idempotent = method == HttpMethod.GET
    sendRequest(
      core,
      new GenericManagerRequest(
        timeout,
        core.context,
        retryStrategy,
        () => new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, path),
        idempotent
      )
    )
  }

  def sendRequest(
      core: Core,
      method: HttpMethod,
      path: String,
      body: UrlQueryStringBuilder,
      timeout: Duration,
      retryStrategy: RetryStrategy
  ): SMono[GenericManagerResponse] = {
    val idempotent = method == HttpMethod.GET
    sendRequest(
      core,
      new GenericManagerRequest(timeout, core.context, retryStrategy, () => {
        val content = Unpooled.copiedBuffer(body.build, UTF_8)
        val req     = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, path, content)
        req.headers.add("Content-Type", HttpHeaderValues.APPLICATION_X_WWW_FORM_URLENCODED)
        req.headers.add("Content-Length", content.readableBytes)
        req
      }, idempotent)
    )
  }

  def checkStatus(response: GenericManagerResponse, action: String): Try[Unit] = {
    if (response.status != ResponseStatus.SUCCESS) {
      Failure(
        new CouchbaseException(
          "Failed to " + action + "; response status=" + response.status + "; response " +
            "body=" + new String(response.content, UTF_8)
        )
      )
    } else Success(())
  }
}
