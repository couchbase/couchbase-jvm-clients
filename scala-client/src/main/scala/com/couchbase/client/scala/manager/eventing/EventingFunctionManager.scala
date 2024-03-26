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
import com.couchbase.client.scala.Collection
import com.couchbase.client.scala.util.DurationConversions._

import scala.concurrent.duration.Duration
import scala.util.Try

/** This interface allows managing eventing functions.
  *
  * Note that an eventing function exists in a particular scope.  This interface is for working with eventing functions in
  * the admin ("*.*") scope.  For working with eventing functions in different scopes, see [[ScopedEventingFunctionManager]],
  * accessed from [[com.couchbase.client.scala.Scope.eventingFunctions]]
  */
class EventingFunctionManager(private val async: AsyncEventingFunctionManager) {
  private val DefaultTimeout       = async.DefaultTimeout
  private val DefaultRetryStrategy = async.DefaultRetryStrategy

  /** Upsert an eventing function into the admin scope ('*.*').
    *
    * @param function the eventing function to upsert.
    * @param timeout timeout to apply to the operation.
    * @param retryStrategy controls how the operation is retried, if it needs to be.
    * @param parentSpan controls the parent tracing span to use for the operation.
    */
  def upsertFunction(
      function: EventingFunction,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy,
      parentSpan: Option[RequestSpan] = None
  ): Try[Unit] = {
    Collection.block(async.upsertFunction(function, timeout, retryStrategy, parentSpan))
  }

  /** Fetch an eventing function from the admin scope ('*.*').
    *
    * @param name      the name of the eventing function to fetch.
    * @param timeout       timeout to apply to the operation.
    * @param retryStrategy controls how the operation is retried, if it needs to be.
    * @param parentSpan    controls the parent tracing span to use for the operation.
    */
  def getFunction(
      name: String,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy,
      parentSpan: Option[RequestSpan] = None
  ): Try[EventingFunction] = {
    Collection.block(async.getFunction(name, timeout, retryStrategy, parentSpan))
  }

  /** Drop an eventing function from the admin scope ('*.*').
    *
    * @param name          the name of the eventing function to fetch.
    * @param timeout       timeout to apply to the operation.
    * @param retryStrategy controls how the operation is retried, if it needs to be.
    * @param parentSpan    controls the parent tracing span to use for the operation.
    */
  def dropFunction(
      name: String,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy,
      parentSpan: Option[RequestSpan] = None
  ): Try[Unit] = {
    Collection.block(async.dropFunction(name, timeout, retryStrategy, parentSpan))
  }

  /** Deploy an eventing function in the admin scope ('*.*').
    *
    * @param name          the name of the eventing function to fetch.
    * @param timeout       timeout to apply to the operation.
    * @param retryStrategy controls how the operation is retried, if it needs to be.
    * @param parentSpan    controls the parent tracing span to use for the operation.
    */
  def deployFunction(
      name: String,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy,
      parentSpan: Option[RequestSpan] = None
  ): Try[Unit] = {
    Collection.block(async.deployFunction(name, timeout, retryStrategy, parentSpan))
  }

  /** Undeploy an eventing function in the admin scope ('*.*').
    *
    * @param name          the name of the eventing function to fetch.
    * @param timeout       timeout to apply to the operation.
    * @param retryStrategy controls how the operation is retried, if it needs to be.
    * @param parentSpan    controls the parent tracing span to use for the operation.
    */
  def undeployFunction(
      name: String,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy,
      parentSpan: Option[RequestSpan] = None
  ): Try[Unit] = {
    Collection.block(async.undeployFunction(name, timeout, retryStrategy, parentSpan))
  }

  /** Get all eventing functions from the admin scope ('*.*').
    *
    * @param timeout       timeout to apply to the operation.
    * @param retryStrategy controls how the operation is retried, if it needs to be.
    * @param parentSpan    controls the parent tracing span to use for the operation.
    */
  def getAllFunctions(
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy,
      parentSpan: Option[RequestSpan] = None
  ): Try[Seq[EventingFunction]] = {
    Collection.block(async.getAllFunctions(timeout, retryStrategy, parentSpan))
  }

  /** Pause an eventing function in the admin scope ('*.*').
    *
    * @param timeout       timeout to apply to the operation.
    * @param retryStrategy controls how the operation is retried, if it needs to be.
    * @param parentSpan    controls the parent tracing span to use for the operation.
    */
  def pauseFunction(
      name: String,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy,
      parentSpan: Option[RequestSpan] = None
  ): Try[Unit] = {
    Collection.block(async.pauseFunction(name, timeout, retryStrategy, parentSpan))
  }

  /** Resume a paused eventing function in the admin scope ('*.*').
    *
    * @param timeout       timeout to apply to the operation.
    * @param retryStrategy controls how the operation is retried, if it needs to be.
    * @param parentSpan    controls the parent tracing span to use for the operation.
    */
  def resumeFunction(
      name: String,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy,
      parentSpan: Option[RequestSpan] = None
  ): Try[Unit] = {
    Collection.block(async.resumeFunction(name, timeout, retryStrategy, parentSpan))
  }

  /** Get the status of an eventing function in the admin scope ('*.*').
    *
    * @param timeout       timeout to apply to the operation.
    * @param retryStrategy controls how the operation is retried, if it needs to be.
    * @param parentSpan    controls the parent tracing span to use for the operation.
    * @return an [[EventingStatus]] containing the status of the eventing function.
    */
  def functionsStatus(
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy,
      parentSpan: Option[RequestSpan] = None
  ): Try[EventingStatus] = {
    Collection.block(async.functionsStatus(timeout, retryStrategy, parentSpan))
  }
}
