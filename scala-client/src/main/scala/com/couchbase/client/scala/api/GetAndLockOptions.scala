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

package com.couchbase.client.scala.api

import scala.concurrent.duration.FiniteDuration

case class GetAndLockOptions(timeout: FiniteDuration = null) {
  def timeout(timeout: FiniteDuration) = copy(timeout = timeout)
}

//case class GetAndLockOptions() {
//  private var timeout: FiniteDuration = null
//
//  def timeout(timeout: FiniteDuration): GetAndLockOptions = {
//    this.timeout = timeout
//    this
//  }
//
//  def build(): GetAndLockOptionsBuilt = GetAndLockOptionsBuilt(timeout)
//}

object GetAndLockOptions {
  def apply() = new GetAndLockOptions()
}