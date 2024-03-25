/*
 * Copyright 2021 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.scala.manager.eventing

import com.couchbase.client.core.annotation.Stability
import com.couchbase.client.core.cnc.RequestSpan
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.util.DurationConversions._
import reactor.core.scala.publisher.SMono

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration
class ReactiveEventingFunctionManager(private val async: AsyncEventingFunctionManager)(
    implicit ec: ExecutionContext
) {
  private val DefaultTimeout       = async.DefaultTimeout
  private val DefaultRetryStrategy = async.DefaultRetryStrategy

  def upsertFunction(
      function: EventingFunction,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy,
      parentSpan: Option[RequestSpan] = None
  ): SMono[Unit] = {
    SMono.fromFuture(async.upsertFunction(function, timeout, retryStrategy, parentSpan))
  }

  def getFunction(
      name: String,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy,
      parentSpan: Option[RequestSpan] = None
  ): SMono[EventingFunction] = {
    SMono.fromFuture(async.getFunction(name, timeout, retryStrategy, parentSpan))
  }

  def dropFunction(
      name: String,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy,
      parentSpan: Option[RequestSpan] = None
  ): SMono[Unit] = {
    SMono.fromFuture(async.dropFunction(name, timeout, retryStrategy, parentSpan))
  }

  def deployFunction(
      name: String,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy,
      parentSpan: Option[RequestSpan] = None
  ): SMono[Unit] = {
    SMono.fromFuture(async.deployFunction(name, timeout, retryStrategy, parentSpan))
  }

  def getAllFunctions(
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy,
      parentSpan: Option[RequestSpan] = None
  ): SMono[Seq[EventingFunction]] = {
    SMono.fromFuture(async.getAllFunctions(timeout, retryStrategy, parentSpan))
  }

  def pauseFunction(
      name: String,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy,
      parentSpan: Option[RequestSpan] = None
  ): SMono[Unit] = {
    SMono.fromFuture(async.pauseFunction(name, timeout, retryStrategy, parentSpan))
  }

  def resumeFunction(
      name: String,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy,
      parentSpan: Option[RequestSpan] = None
  ): SMono[Unit] = {
    SMono.fromFuture(async.resumeFunction(name, timeout, retryStrategy, parentSpan))
  }

  def undeployFunction(
      name: String,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy,
      parentSpan: Option[RequestSpan] = None
  ): SMono[Unit] = {
    SMono.fromFuture(async.undeployFunction(name, timeout, retryStrategy, parentSpan))
  }

  def functionsStatus(
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy,
      parentSpan: Option[RequestSpan] = None
  ): SMono[EventingStatus] = {
    SMono.fromFuture(async.functionsStatus(timeout, retryStrategy, parentSpan))
  }
}
