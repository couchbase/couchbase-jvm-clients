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

import scala.concurrent.{Await, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Try}
import scala.concurrent.duration._

private[scala] object AsyncUtils {
  // TODO what should this be?
  private val DefaultTimeout = 10.seconds

  def block[A](in: Future[A], timeout: Duration = DefaultTimeout): Try[A] = {
    Try(Await.result(in, timeout))
  }
}
