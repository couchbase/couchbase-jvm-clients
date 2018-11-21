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

import scala.concurrent.duration.{FiniteDuration, _}

case class ReplaceOptions(timeout: FiniteDuration = null,
                               expiration: FiniteDuration = 0.seconds,
                               replicateTo: ReplicateTo.Value = ReplicateTo.NONE,
                               persistTo: PersistTo.Value = PersistTo.NONE) {
  def timeout(timeout: FiniteDuration): ReplaceOptions = copy(timeout = timeout)
  def expiration(expiration: FiniteDuration): ReplaceOptions = copy(expiration = expiration)
  def replicateTo(replicateTo: ReplicateTo.Value): ReplaceOptions = copy(replicateTo = replicateTo)
  def persistTo(persistTo: PersistTo.Value): ReplaceOptions = copy(persistTo = persistTo)
}

//case class ReplaceOptionsBuilt(timeout: FiniteDuration,
//                              expiration: FiniteDuration,
//                              replicateTo: ReplicateTo.Value,
//                              persistTo: PersistTo.Value)
//
//class ReplaceOptions() {
//  private var timeout: FiniteDuration = null
//  private var expiration: FiniteDuration = 0.seconds
//  private var replicateTo: ReplicateTo.Value = ReplicateTo.NONE
//  private var persistTo: PersistTo.Value = PersistTo.NONE
//
//  def timeout(timeout: FiniteDuration): ReplaceOptions = {
//    this.timeout = timeout
//    this
//  }
//
//  def expiration(expiration: FiniteDuration): ReplaceOptions = {
//    this.expiration = expiration
//    this
//  }
//
//  def replicateTo(replicateTo: ReplicateTo.Value): ReplaceOptions = {
//    this.replicateTo = replicateTo
//    this
//  }
//
//  def persistTo(persistTo: PersistTo.Value): ReplaceOptions = {
//    this.persistTo = persistTo
//    this
//  }
//
//  def build(): ReplaceOptionsBuilt = ReplaceOptionsBuilt(timeout,
//    expiration,
//    replicateTo,
//    persistTo)
//}

object ReplaceOptions {
  def apply(): ReplaceOptions = new ReplaceOptions()
}




