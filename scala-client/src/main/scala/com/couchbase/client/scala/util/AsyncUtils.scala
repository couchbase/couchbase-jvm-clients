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

package com.couchbase.client.scala.util

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Try

/** Utility functions to deal with asynchronous API.
  *
  * @author Graham Pople
  * @since 1.0.0
  */
private[scala] object AsyncUtils {
  // All operations should be bounded by core or the server
  private val DefaultTimeout = Duration.Inf

  def block[A](in: Future[A]): Try[A] = block(in, DefaultTimeout)

  def block[A](in: Future[A], timeout: Duration): Try[A] = {
    Try(Await.result(in, timeout))
  }
}
