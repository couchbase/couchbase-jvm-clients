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

import com.couchbase.client.core.error.CASMismatchException
import com.couchbase.client.scala.Collection
import com.couchbase.client.scala.codec.{Conversions, JsonDeserializer, JsonSerializer}
import com.couchbase.client.scala.kv.{LookupInSpec, MutateInSpec}

import scala.util.{Failure, Success}
import scala.reflect.runtime.universe._


/** Presents a Scala Queue interface on top of a mutable persistent data structure, in the form of a document stored
  * on the cluster.
  */
class CouchbaseQueue[T](id: String,
                        collection: Collection,
                        options: Option[CouchbaseCollectionOptions] = None)
                       (implicit decode: JsonDeserializer[T], encode: JsonSerializer[T], tag: TypeTag[T])
  extends CouchbaseBuffer(id, collection, options) {

  def enqueue(elems: T*): Unit = this ++= elems

  def dequeue(): T = {
    val op = collection.lookupIn(id,
      Array(LookupInSpec.get("[-1]")),
      timeout = opts.timeout,
      retryStrategy = opts.retryStrategy)

    val result = op.flatMap(result => result.contentAs[T](0))

    result match {
      case Success(value) =>
        val mutateResult = collection.mutateIn(id,
          Array(MutateInSpec.remove("[-1]")),
            timeout = opts.timeout,
            retryStrategy = opts.retryStrategy,
            cas = op.get.cas,
            durability = opts.durability)

        mutateResult match {
          case Success(_) => value
          case Failure(err: CASMismatchException) =>
            // Recurse to try again
            dequeue()
          case Failure(err) => throw err
        }
      case Failure(err) => throw err
    }
  }
}
