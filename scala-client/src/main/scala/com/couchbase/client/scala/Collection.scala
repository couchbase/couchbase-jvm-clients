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

import java.util.concurrent.TimeUnit

import com.couchbase.client.scala.api._
import com.couchbase.client.scala.document._
//import com.couchbase.client.scala.query.N1qlQueryResult

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal
import scala.reflect.runtime.universe._

class Collection(val async: AsyncCollection,
                 bucketName: String)
                (implicit ec: ExecutionContext) {
  private val config: CouchbaseEnvironment = null // scope.cluster.env
//  private val asyncCollection = new AsyncCollection(this)
//  private val reactiveColl = new ReactiveCollection(this)
  // TODO binary collection
  // TODO MVP reactive collection
  private val SafetyTimeout = 1.second
//  val kvTimeout = FiniteDuration(config.kvTimeout(), TimeUnit.MILLISECONDS)
  val kvTimeout = FiniteDuration(2500, TimeUnit.MILLISECONDS)

  def block[T](in: Future[T], timeout: FiniteDuration): Try[T] = {
    try {
      Try(Await.result(in, timeout + SafetyTimeout))
    }
    catch {
      case NonFatal(err) => Failure(err)
    }
  }

  def insert[T](id: String,
                content: T,
                timeout: FiniteDuration = kvTimeout,
                expiration: FiniteDuration = 0.seconds,
                  replicateTo: ReplicateTo.Value = ReplicateTo.None,
  persistTo: PersistTo.Value = PersistTo.None,
  durability: Durability.Value = Durability.None
               // TODO durability
            )
               (implicit ev: Conversions.Convertable[T])
//               (implicit tag: TypeTag[T])
  : Try[MutationResult] = {
    block(async.insert(id, content, timeout, expiration), timeout)
  }

//  def insert[T](id: String,
//             content: T,
//             options: InsertOptions,
//            ): Try[MutationResult] = {
//    block(asyncCollection.insert(id, content, options), timeout)
//  }

  def replace[T](id: String,
                 content: T,
                 cas: Long,
                 timeout: FiniteDuration = kvTimeout,
                 expiration: FiniteDuration = 0.seconds,
                 replicateTo: ReplicateTo.Value = ReplicateTo.None,
                 persistTo: PersistTo.Value = PersistTo.None,
                 durability: Durability.Value = Durability.None
                 // TODO durability
                ): Try[MutationResult] = {
    block(async.replace(id, content, cas, timeout, expiration), timeout)
  }

//  def replace[T](id: String,
//              content: T,
//              cas: Long,
//              options: ReplaceOptions,
//            ): Try[MutationResult] = {
//    block(asyncCollection.replace(id, content, cas, options), timeout)
//  }

  def upsert[T](id: String,
                 content: T,
                 timeout: FiniteDuration = kvTimeout,
                 expiration: FiniteDuration = 0.seconds,
                replicateTo: ReplicateTo.Value = ReplicateTo.None,
                persistTo: PersistTo.Value = PersistTo.None,
                durability: Durability.Value = Durability.None
                 // TODO durability
                ): Try[MutationResult] = ???

//  def upsert[T](id: String,
//                 content: T,
//                 cas: Long,
//                 options: UpsertOptions,
//                ): Try[MutationResult] = ???

  def remove(id: String,
             cas: Long = 0,
             timeout: FiniteDuration = kvTimeout,
             replicateTo: ReplicateTo.Value = ReplicateTo.None,
             persistTo: PersistTo.Value = PersistTo.None,
             durability: Durability.Value = Durability.None
             // TODO durability
            ): Try[MutationResult] = {
    block(async.remove(id, cas, timeout), timeout)
  }

//  def remove(id: String,
//             cas: Long,
//             options: RemoveOptions
//            ): Try[MutationResult] = {
//    block(asyncCollection.remove(id, cas, options), timeout)
//  }

//  def mutateIn(id: String,
//               spec: MutateInSpec,
//               options: MutateInOptions)
//              : Try[MutationResult] = ???

  def mutateIn(id: String,
               spec: MutateInSpec,
               cas: Long = 0,
               timeout: FiniteDuration = kvTimeout,
               replicateTo: ReplicateTo.Value = ReplicateTo.None,
               persistTo: PersistTo.Value = PersistTo.None,
               durability: Durability.Value = Durability.None
              ): Try[MutationResult] = ???


  def get(id: String,
          operations: GetSpec = GetSpec().getDoc,
          timeout: FiniteDuration = kvTimeout,
          withExpiry: Boolean = false)
         : Try[GetResult] = {
    block(async.get(id, timeout), timeout)
  }

//  def get(id: String,
//          operations: GetSpec = GetSpec().getFullDocument,
//          options: GetOptions)
//         : Option[GetResult] = {
//    block(asyncCollection.get(id, options), timeout)
//  }

//  def readOrError(id: String,
//                  timeout: FiniteDuration = kvTimeout)
//                 : GetResult = {
//    block(asyncCollection.getOrError(id, timeout), timeout)
//  }
//
//  def readOrError(id: String,
//                  options: GetOptions)
//                 : GetResult = {
//    block(asyncCollection.getOrError(id, options), timeout)
//  }

//  def readAndLock(id: String,
//                  lockFor: FiniteDuration,
//                  timeout: FiniteDuration = kvTimeout)
//                 : Option[GetResult] = {
//    block(asyncCollection.getAndLock(id, lockFor, timeout), timeout)
//  }
//
//  def readAndLock(id: String,
//                  lockFor: FiniteDuration,
//                  options: GetAndLockOptions)
//                 : Option[GetResult] = {
//    block(asyncCollection.getAndLock(id, lockFor, options), timeout)
//  }

//  def query(statement: String, query: QueryOptions = QueryOptions()): Try[N1qlQueryResult] = ???


//    def reactive(): ReactiveCollection = reactiveColl
}
