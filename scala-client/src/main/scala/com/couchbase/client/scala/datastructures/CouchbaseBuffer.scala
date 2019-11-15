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

import com.couchbase.client.core.error.{CasMismatchException, DocumentNotFoundException}
import com.couchbase.client.scala.Collection
import com.couchbase.client.scala.codec.{Conversions, JsonDeserializer, JsonSerializer}
import com.couchbase.client.scala.json.JsonArraySafe
import com.couchbase.client.scala.kv.{LookupInSpec, MutateInSpec}

import scala.collection.mutable
import scala.util.{Failure, Success, Try}
import scala.reflect.runtime.universe._

/** Presents a Scala Buffer interface on top of a mutable persistent data structure, in the form of a document stored
  * on the cluster.
  */
class CouchbaseBuffer[T](
    id: String,
    collection: Collection,
    options: Option[CouchbaseCollectionOptions] = None
)(implicit decode: JsonDeserializer[T], encode: JsonSerializer[T], tag: WeakTypeTag[T])
    extends mutable.Buffer[T] {

  protected val opts: CouchbaseCollectionOptions = options match {
    case Some(v) => v
    case _       => CouchbaseCollectionOptions(collection)
  }

  override def apply(index: Int): T = {
    val op = collection.lookupIn(
      id,
      Array(LookupInSpec.get("[" + index + "]")),
      timeout = opts.timeout,
      retryStrategy = opts.retryStrategy
    )

    val result = op.flatMap(result => result.contentAs[T](0))

    result match {
      case Success(value) => value
      case Failure(err)   => throw err
    }
  }

  // A fast version of `remove` that does not have to return the element
  def removeAt(index: Int): Unit = {
    collection
      .mutateIn(
        id,
        Array(MutateInSpec.remove("[" + index + "]")),
        timeout = opts.timeout,
        retryStrategy = opts.retryStrategy,
        durability = opts.durability
      )
      .get
  }

  // Note `remoteAt` is more performant as it does not have to return the removed element
  override def remove(index: Int): T = {
    val op = collection.lookupIn(
      id,
      Array(LookupInSpec.get("[" + index + "]")),
      timeout = opts.timeout,
      retryStrategy = opts.retryStrategy
    )

    val result = op.flatMap(result => result.contentAs[T](0))

    result match {
      case Success(value) =>
        val mutateResult = collection.mutateIn(
          id,
          Array(MutateInSpec.remove("[" + index + "]")),
          cas = op.get.cas,
          timeout = opts.timeout,
          retryStrategy = opts.retryStrategy,
          durability = opts.durability
        )

        mutateResult match {
          case Success(_)                         => value
          case Failure(err: CasMismatchException) =>
            // Recurse to try again
            remove(index)
          case Failure(err) => throw err
        }

      case Failure(err) => throw err
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

  def append(value: T): this.type = {
    val f = () =>
      collection.mutateIn(
        id,
        Array(MutateInSpec.arrayAppend("", Seq(value))),
        timeout = opts.timeout,
        retryStrategy = opts.retryStrategy,
        durability = opts.durability
      )
    retryIfDocDoesNotExist(f)
    this
  }

  def prepend(value: T): this.type = {
    val f = () =>
      collection.mutateIn(
        id,
        Array(MutateInSpec.arrayPrepend("", Seq(value))),
        timeout = opts.timeout,
        retryStrategy = opts.retryStrategy,
        durability = opts.durability
      )
    retryIfDocDoesNotExist(f)
    this
  }

  private def initialize(): Unit = {
    // The .get will throw if anything goes wrong
    collection
      .insert(
        id,
        JsonArraySafe.create,
        timeout = opts.timeout,
        retryStrategy = opts.retryStrategy,
        durability = opts.durability
      )
      .get
  }

  override def size(): Int = {
    val op = collection.lookupIn(
      id,
      Array(LookupInSpec.count("")),
      timeout = opts.timeout,
      retryStrategy = opts.retryStrategy
    )

    val result = op.flatMap(result => result.contentAs[Int](0))

    result match {
      case Success(count)                        => count
      case Failure(_: DocumentNotFoundException) => 0
      case Failure(err)                          => throw err
    }
  }

  private def all(): Seq[T] = {
    val op = collection.get(id, timeout = opts.timeout, retryStrategy = opts.retryStrategy)

    val result = op
      .flatMap(_.contentAs[JsonArraySafe])
      .map(array => array.toSeq.asInstanceOf[Seq[T]])

    result match {
      case Success(values: Seq[T])               => values
      case Failure(_: DocumentNotFoundException) => Seq.empty[T]
      case Failure(err)                          => throw err
    }
  }

  override def foreach[U](f: T => U): Unit = {
    all().foreach(f)
  }

  override def iterator: Iterator[T] = {
    all().iterator
  }

  override def length: Int = size()

  override def update(index: Int, value: T): Unit = {
    val result = collection.mutateIn(
      id,
      Array(MutateInSpec.upsert("[" + index + "]", value)),
      timeout = opts.timeout,
      retryStrategy = opts.retryStrategy,
      durability = opts.durability
    )

    result match {
      case Success(_) =>
      case Failure(_: DocumentNotFoundException) =>
        initialize()
        update(index, value)
      case Failure(err) => throw err
    }
  }

  override def +=(elem: T): this.type = append(elem)

  override def clear(): Unit = {
    collection.remove(
      id,
      timeout = opts.timeout,
      retryStrategy = opts.retryStrategy,
      durability = opts.durability
    ) match {
      case Failure(_: DocumentNotFoundException) =>
      case Failure(err)                          => throw err
      case _                                     =>
    }
  }

  override def +=:(elem: T): this.type = prepend(elem)

  override def insertAll(index: Int, values: Traversable[T]): Unit = {
    val result = collection.mutateIn(
      id,
      Array(MutateInSpec.arrayAppend("[" + index + "]", values.toSeq)),
      timeout = opts.timeout,
      retryStrategy = opts.retryStrategy,
      durability = opts.durability
    )

    result match {
      case Success(_) =>
      case Failure(_: DocumentNotFoundException) =>
        initialize()
        insertAll(index, values)
      case Failure(err) => throw err
    }
  }

}
