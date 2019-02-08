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

import com.couchbase.client.scala.api._
import com.couchbase.client.scala.document.{GetResult}
import reactor.core.scala.publisher.Mono

import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{ExecutionContext, Future}

// During prototyping, leaving reactive collection support very skimpy.  It's just the same as Collection but returning Mono
//class ReactiveCollection(val coll: Collection) {
//  private val async = coll.async()
//  private val kvTimeout = coll.kvTimeout
//
//  def insertContent(id: String,
//                    content: JsonObject,
//                    timeout: FiniteDuration = kvTimeout,
//                    expiration: FiniteDuration = 0.seconds,
//                    replicateTo: ObserveReplicateTo.Value = ObserveReplicateTo.None,
//                    persistTo: ObservePersistTo.Value = ObservePersistTo.None
//            )(implicit ec: ExecutionContext): Mono[MutationResult] = {
//    Mono.fromFuture(async.insert(id, content, timeout, expiration, replicateTo, persistTo))
//  }
//
//  def remove(id: String,
//             cas: Long,
//             options: RemoveOptions = RemoveOptions()): Mono[MutationResult] = ???
//
//  def get(id: String,
//          timeout: FiniteDuration = kvTimeout)
//         (implicit ec: ExecutionContext): Mono[Option[GetResult]] = {
//    Mono.fromFuture(async.get(id, timeout))
//  }
//
//  def get(id: String,
//          options: GetOptions)
//         (implicit ec: ExecutionContext): Mono[Option[GetResult]] = {
//    Mono.fromFuture(async.get(id, options))
//  }
//
//  def getOrError(id: String,
//                 timeout: FiniteDuration = kvTimeout)
//                (implicit ec: ExecutionContext): Mono[GetResult] = {
//    Mono.fromFuture(async.getOrError(id, timeout))
//  }
//
//  def getOrError(id: String,
//                 options: GetOptions)
//                (implicit ec: ExecutionContext): Mono[GetResult] = {
//    Mono.fromFuture(async.getOrError(id, options))
//  }
//
//  def getAndLock(id: String,
//                 lockFor: FiniteDuration,
//                 timeout: FiniteDuration = kvTimeout)
//                (implicit ec: ExecutionContext): Mono[Option[GetResult]] = {
//    Mono.fromFuture(async.getAndLock(id, lockFor, timeout))
//  }
//
//  def getAndLock(id: String,
//                 lockFor: FiniteDuration,
//                 options: GetAndLockOptions)
//                (implicit ec: ExecutionContext): Mono[Option[GetResult]] = {
//    Mono.fromFuture(async.getAndLock(id, lockFor, options))
//  }
//}
