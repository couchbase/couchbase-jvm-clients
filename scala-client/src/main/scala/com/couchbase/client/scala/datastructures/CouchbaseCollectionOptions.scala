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
package com.couchbase.client.scala.datastructures

import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.Collection
import com.couchbase.client.scala.durability.Durability
import com.couchbase.client.scala.util.DurationConversions

import scala.concurrent.duration.Duration

case class CouchbaseCollectionOptions(
    timeout: Duration,
    retryStrategy: RetryStrategy,
    durability: Durability = Durability.Disabled
)

object CouchbaseCollectionOptions {
  def apply(collection: Collection): CouchbaseCollectionOptions = {
    CouchbaseCollectionOptions(
      timeout = DurationConversions.javaDurationToScala(
        collection.async.environment.timeoutConfig.kvTimeout()
      ),
      retryStrategy = collection.async.environment.retryStrategy
    )
  }
}
