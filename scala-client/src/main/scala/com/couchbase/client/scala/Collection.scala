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
import com.couchbase.client.scala.query.N1qlQueryResult

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._

class Collection(val name: String,
                 val scope: Scope) {
  private val config: CouchbaseEnvironment = null // scope.cluster.env
  private val asyncColl = new AsyncCollection(this)
  private val reactiveColl = new ReactiveCollection(this)
  private val safetyTimeout = 60.minutes
//  val kvTimeout = FiniteDuration(config.kvTimeout(), TimeUnit.MILLISECONDS)
  val kvTimeout = FiniteDuration(2500, TimeUnit.MILLISECONDS)

  def insert[T](id: String,
                content: T,
                timeout: FiniteDuration = kvTimeout,
                expiration: FiniteDuration = 0.seconds,
                  replicateTo: ReplicateTo.Value = ReplicateTo.None,
  persistTo: PersistTo.Value = PersistTo.None,
  durability: Durability.Value = Durability.None
               // TODO durability
            )(implicit ec: ExecutionContext): MutationResult = {
    Await.result(asyncColl.insert(id, content, timeout, expiration), safetyTimeout)
  }

//  def insert[T](id: String,
//             content: T,
//             options: InsertOptions,
//            )(implicit ec: ExecutionContext): MutationResult = {
//    Await.result(asyncColl.insert(id, content, options), safetyTimeout)
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
                )(implicit ec: ExecutionContext): MutationResult = {
    Await.result(asyncColl.replace(id, content, cas, timeout, expiration), safetyTimeout)
  }

//  def replace[T](id: String,
//              content: T,
//              cas: Long,
//              options: ReplaceOptions,
//            )(implicit ec: ExecutionContext): MutationResult = {
//    Await.result(asyncColl.replace(id, content, cas, options), safetyTimeout)
//  }

  def upsert[T](id: String,
                 content: T,
                 timeout: FiniteDuration = kvTimeout,
                 expiration: FiniteDuration = 0.seconds,
                replicateTo: ReplicateTo.Value = ReplicateTo.None,
                persistTo: PersistTo.Value = PersistTo.None,
                durability: Durability.Value = Durability.None
                 // TODO durability
                )(implicit ec: ExecutionContext): MutationResult = ???

//  def upsert[T](id: String,
//                 content: T,
//                 cas: Long,
//                 options: UpsertOptions,
//                )(implicit ec: ExecutionContext): MutationResult = ???

  def remove(id: String,
             cas: Long,
             timeout: FiniteDuration = kvTimeout,
             replicateTo: ReplicateTo.Value = ReplicateTo.None,
             persistTo: PersistTo.Value = PersistTo.None,
             durability: Durability.Value = Durability.None
             // TODO durability
            )(implicit ec: ExecutionContext): MutationResult = {
    Await.result(asyncColl.remove(id, cas, timeout), safetyTimeout)
  }

//  def remove(id: String,
//             cas: Long,
//             options: RemoveOptions
//            )(implicit ec: ExecutionContext): MutationResult = {
//    Await.result(asyncColl.remove(id, cas, options), safetyTimeout)
//  }

//  def mutateIn(id: String,
//               spec: MutateInSpec,
//               options: MutateInOptions)
//              (implicit ec: ExecutionContext): MutationResult = ???

  def mutateIn(id: String,
               spec: MutateInSpec,
               cas: Long = 0,
               timeout: FiniteDuration = kvTimeout,
               replicateTo: ReplicateTo.Value = ReplicateTo.None,
               persistTo: PersistTo.Value = PersistTo.None,
               durability: Durability.Value = Durability.None
              )(implicit ec: ExecutionContext): MutationResult = ???


  def get(id: String,
          operations: GetSpec = GetSpec().getDoc,
          timeout: FiniteDuration = kvTimeout,
          withExpiry: Boolean = false)
         (implicit ec: ExecutionContext): Option[GetResult] = {
    Await.result(asyncColl.get(id, timeout), safetyTimeout)
  }

//  def get(id: String,
//          operations: GetSpec = GetSpec().getFullDocument,
//          options: GetOptions)
//         (implicit ec: ExecutionContext): Option[GetResult] = {
//    Await.result(asyncColl.get(id, options), safetyTimeout)
//  }

//  def readOrError(id: String,
//                  timeout: FiniteDuration = kvTimeout)
//                 (implicit ec: ExecutionContext): GetResult = {
//    Await.result(asyncColl.getOrError(id, timeout), safetyTimeout)
//  }
//
//  def readOrError(id: String,
//                  options: GetOptions)
//                 (implicit ec: ExecutionContext): GetResult = {
//    Await.result(asyncColl.getOrError(id, options), safetyTimeout)
//  }

//  def readAndLock(id: String,
//                  lockFor: FiniteDuration,
//                  timeout: FiniteDuration = kvTimeout)
//                 (implicit ec: ExecutionContext): Option[GetResult] = {
//    Await.result(asyncColl.getAndLock(id, lockFor, timeout), safetyTimeout)
//  }
//
//  def readAndLock(id: String,
//                  lockFor: FiniteDuration,
//                  options: GetAndLockOptions)
//                 (implicit ec: ExecutionContext): Option[GetResult] = {
//    Await.result(asyncColl.getAndLock(id, lockFor, options), safetyTimeout)
//  }

  def query(statement: String, query: QueryOptions = QueryOptions()): N1qlQueryResult = ???


  def async(): AsyncCollection = asyncColl
  def reactive(): ReactiveCollection = reactiveColl
}
