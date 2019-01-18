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

import com.couchbase.client.core.Core
import com.couchbase.client.scala.util.AsyncUtils

import scala.concurrent.ExecutionContext
import scala.util.Try

// For now, use SDK 2 Cluster & Bucket objects as our base layer for SDK 3 prototyping
class Scope(val async: AsyncScope,
            bucketName: String)
           (implicit ec: ExecutionContext) {

  def name = async.name

  def collection(name: String): Try[Collection] = {
    AsyncUtils.block(async.collection(name))
      .map(asyncCollection => new Collection(asyncCollection, bucketName))
  }

  def defaultCollection() = {
    collection(Defaults.DefaultCollection)
  }
}

