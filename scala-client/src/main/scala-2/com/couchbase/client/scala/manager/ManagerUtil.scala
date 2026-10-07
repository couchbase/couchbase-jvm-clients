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
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod
import com.couchbase.client.core.endpoint.http.{
  CoreCommonOptions,
  CoreHttpPath,
  CoreHttpRequest,
  CoreHttpResponse
}
import com.couchbase.client.core.error.CouchbaseException
import com.couchbase.client.core.msg.{RequestTarget, ResponseStatus}
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.core.util.UrlQueryStringBuilder
import com.couchbase.client.scala.util.DurationConversions._
import com.couchbase.client.scala.util.FutureConversions
import reactor.core.scala.publisher.SMono

import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

object ManagerUtil {
  def sendRequest(core: Core, request: CoreHttpRequest): SMono[CoreHttpResponse] = {
    SMono.defer(() => {
      core.send(request)
      FutureConversions
        .javaCFToScalaMono(request, request.response, true)
        .doOnNext(_ => request.context.logicallyComplete)
        .doOnError(err => request.context().logicallyComplete(err))
    })
  }

  def sendRequest(
      core: Core,
      method: HttpMethod,
      path: String,
      timeout: Duration,
      retryStrategy: RetryStrategy
  ): SMono[CoreHttpResponse] = {
    sendRequest(core, requestBuilder(core, method, path, timeout, retryStrategy).build())
  }

  def sendRequest(
      core: Core,
      method: HttpMethod,
      path: String,
      body: UrlQueryStringBuilder,
      timeout: Duration,
      retryStrategy: RetryStrategy
  ): SMono[CoreHttpResponse] = {
    sendRequest(core, requestBuilder(core, method, path, timeout, retryStrategy).form(body).build())
  }

  private def requestBuilder(
      core: Core,
      method: HttpMethod,
      path: String,
      timeout: Duration,
      retryStrategy: RetryStrategy
  ): CoreHttpRequest.Builder = {
    CoreHttpRequest
      .builder(
        CoreCommonOptions.of(timeout, retryStrategy, null),
        core.context,
        method,
        CoreHttpPath.path(path),
        RequestTarget.manager()
      )
      .idempotent(method == HttpMethod.GET)
      .failOnErrorStatus(false) // callers check the status
  }

  def checkStatus(response: CoreHttpResponse, action: String): Try[Unit] = {
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
