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

import com.couchbase.client.core.error.subdoc.PathNotFoundException
import com.couchbase.client.core.error.{CasMismatchException, DocumentNotFoundException}
import com.couchbase.client.scala.Collection
import com.couchbase.client.scala.codec.{JsonDeserializer, JsonSerializer}
import com.couchbase.client.scala.json.JsonObjectSafe
import com.couchbase.client.scala.kv._

import scala.collection.mutable
import scala.reflect.runtime.universe._
import scala.util.{Failure, Success, Try}

/** Presents a Scala Map interface on top of a mutable persistent data structure, in the form of a document stored
  * on the cluster.
  */
class CouchbaseMap[T](
    id: String,
    collection: Collection,
    options: Option[CouchbaseCollectionOptions] = None
)(implicit decode: JsonDeserializer[T], encode: JsonSerializer[T], tag: WeakTypeTag[T])
    extends mutable.AbstractMap[String, T] {

  private val Values = "values."

  private val opts: CouchbaseCollectionOptions = options match {
    case Some(v) => v
    case _       => CouchbaseCollectionOptions(collection)
  }

  private val lookupInOptions = LookupInOptions()
    .timeout(opts.timeout)
    .retryStrategy(opts.retryStrategy)
  private val mutateInOptions = MutateInOptions()
    .timeout(opts.timeout)
    .retryStrategy(opts.retryStrategy)
    .durability(opts.durability)
  private val getOptions = GetOptions()
    .timeout(opts.timeout)
    .retryStrategy(opts.retryStrategy)
  private val insertOptions = InsertOptions()
    .timeout(opts.timeout)
    .retryStrategy(opts.retryStrategy)
    .durability(opts.durability)
  private val removeOptions = RemoveOptions()
    .timeout(opts.timeout)
    .retryStrategy(opts.retryStrategy)
    .durability(opts.durability)

  def getInternal(key: String): Try[Option[T]] = {
    val op = collection.lookupIn(
      id,
      Seq(LookupInSpec.get(key)),
      lookupInOptions
    )

    val result: Try[Option[T]] = op.map(result => {
      if (!result.exists(0)) None
      else {
        // The .get will throw on fail, which is what we want
        Some(result.contentAs[T](0).get)
      }
    })

    result
  }

  override def get(key: String): Option[T] = {
    val result = getInternal(key)

    result match {
      case Success(value)                          => value
      case Failure(err: PathNotFoundException)     => None
      case Failure(err: DocumentNotFoundException) => None
      case Failure(err)                            => throw err
    }
  }

  override def apply(key: String): T = {
    val result = getInternal(key)

    result match {
      case Success(Some(value)) => value
      case Success(None) | Failure(_: DocumentNotFoundException) |
          Failure(_: PathNotFoundException) =>
        throw new NoSuchElementException(s"Key $key not found")
      case Failure(err) => throw err
    }
  }

  // A fast version of `remove` that does not have to return the element
  def removeAt(key: String): Unit = {
    collection
      .mutateIn(
        id,
        Seq(MutateInSpec.remove(key)),
        mutateInOptions
      ) match {
      case Success(_)                              =>
      case Failure(err: DocumentNotFoundException) =>
      case Failure(err: PathNotFoundException)     =>
      case Failure(err)                            => throw err
    }
  }

  // Note `removeAt` is more performant as it does not have to return the removed element
  override def remove(key: String): Option[T] = {
    val op = collection.lookupIn(
      id,
      Seq(LookupInSpec.get(key)),
      lookupInOptions
    )

    val result = op.flatMap(result => result.contentAs[T](0))

    result match {
      case Success(value) =>
        val mutateResult = collection.mutateIn(
          id,
          Seq(MutateInSpec.remove(key)),
          mutateInOptions.cas(op.get.cas)
        )

        mutateResult match {
          case Success(_)                         => Some(value)
          case Failure(err: CasMismatchException) =>
            // Recurse to try again
            remove(key)
          case Failure(err) => throw err
        }

      case Failure(err: DocumentNotFoundException) => None
      case Failure(err: PathNotFoundException)     => None
      case Failure(err)                            => throw err
    }
  }

  private def retryIfDocDoesNotExist[_](f: () => Try[_]): Unit = {
    val result: Try[_] = f()

    result match {
      case Success(_) =>
      case Failure(_: DocumentNotFoundException) =>
        initialize()
        retryIfDocDoesNotExist(f)
      case Failure(err) => throw err
    }
  }

  private def initialize(): Unit = {
    // The .get will throw if anything goes wrong
    collection.insert(id, JsonObjectSafe.create).get
  }

  override def size(): Int = {
    val op = collection.lookupIn(
      id,
      Seq(LookupInSpec.count("")),
      lookupInOptions
    )

    val result = op.flatMap(result => result.contentAs[Int](0))

    result match {
      case Success(count)                        => count
      case Failure(_: DocumentNotFoundException) => 0
      case Failure(err)                          => throw err
    }
  }

  private def all(): Map[String, T] = {
    val op = collection.get(id, getOptions)

    val result = op
      .flatMap(_.contentAs[JsonObjectSafe])
      .map(obj => obj.toMap.asInstanceOf[mutable.AnyRefMap[String, T]])

    result match {
      case Success(values: mutable.AnyRefMap[String, T]) => values.toMap
      case Failure(_: DocumentNotFoundException)         => Map.empty[String, T]
      case Failure(err)                                  => throw err
    }
  }

  override def addOne(kv: (String, T)): this.type = {
    val f = () =>
      collection.mutateIn(
        id,
        Seq(MutateInSpec.upsert(kv._1, kv._2)),
        mutateInOptions
      )
    retryIfDocDoesNotExist(f)
    this
  }

  override def subtractOne(key: String): this.type = {
    val result = collection.mutateIn(
      id,
      Seq(MutateInSpec.remove(key)),
      mutateInOptions
    )

    result match {
      case Success(_)                            => this
      case Failure(_: PathNotFoundException)     => this
      case Failure(_: DocumentNotFoundException) => this
      case Failure(err)                          => throw err
    }
  }

  override def iterator: Iterator[(String, T)] = {
    all().iterator
  }
}
