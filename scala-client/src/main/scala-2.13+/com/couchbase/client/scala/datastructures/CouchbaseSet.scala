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

import com.couchbase.client.core.error.subdoc.{PathExistsException, PathNotFoundException}
import com.couchbase.client.core.error.{CasMismatchException, DocumentNotFoundException}
import com.couchbase.client.scala.Collection
import com.couchbase.client.scala.codec.{JsonDeserializer, JsonSerializer}
import com.couchbase.client.scala.json.JsonArraySafe
import com.couchbase.client.scala.kv._

import scala.collection.mutable
import scala.reflect.runtime.universe._
import scala.util.{Failure, Success}

/** Presents a Scala Set interface on top of a mutable persistent data structure, in the form of a document stored
  * on the cluster.
  */
class CouchbaseSet[T](
    id: String,
    collection: Collection,
    options: Option[CouchbaseCollectionOptions] = None
)(implicit decode: JsonDeserializer[T], encode: JsonSerializer[T], tag: WeakTypeTag[T])
    extends mutable.AbstractSet[T] {

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
  private val upsertOptions = UpsertOptions()
    .timeout(opts.timeout)
    .retryStrategy(opts.retryStrategy)
    .durability(opts.durability)

  override def remove(elem: T): Boolean = {
    var out = false

    val op = collection.get(id, getOptions)

    val result = op
      .flatMap(_.contentAs[JsonArraySafe])
      .map(array => array.toSeq)

    result match {
      case Success(values: Seq[T]) =>
        val zipped: Seq[(T, Int)] = values.toSeq.zipWithIndex

        for (eAndIndex <- zipped) {
          val e: T       = eAndIndex._1
          val index: Int = eAndIndex._2

          if (e == elem) {
            val mutateResult = collection.mutateIn(
              id,
              Seq(MutateInSpec.remove("[" + index + "]")),
              mutateInOptions.cas(op.get.cas)
            )

            out = mutateResult match {
              case Success(value)                     => true
              case Failure(_: PathNotFoundException)  => false
              case Failure(err: CasMismatchException) =>
                // Recurse to try again
                remove(elem)
              case Failure(err: DocumentNotFoundException) => false
              case Failure(err)                            => throw err
            }
          }
        }

      case Failure(_: DocumentNotFoundException) => false
      case Failure(err)                          => throw err
    }

    out
  }

  private def initialize(): Unit = {
    // The .get will throw if anything goes wrong
    collection
      .insert(
        id,
        JsonArraySafe.create,
        insertOptions
      )
      .get
  }
  override def clear(): Unit = {
    collection
      .upsert(
        id,
        JsonArraySafe.create,
        upsertOptions
      )
      .get
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

  private def all(): Set[T] = {
    val op = collection.get(id, getOptions)

    val result = op
      .flatMap(_.contentAs[JsonArraySafe])
      .map(array => array.toSeq.toSet.asInstanceOf[Set[T]])

    result match {
      case Success(values: Set[T])               => values
      case Failure(_: DocumentNotFoundException) => Set.empty[T]
      case Failure(err)                          => throw err
    }
  }

  override def foreach[U](f: T => U): Unit = {
    all().foreach(f)
  }

  override def iterator: Iterator[T] = {
    all().iterator
  }

  override def addOne(elem: T): this.type = {
    val result = collection.mutateIn(
      id,
      Seq(MutateInSpec.arrayAddUnique("", elem)),
      mutateInOptions
    )

    result match {
      case Success(_) => this
      case Failure(_: DocumentNotFoundException) =>
        initialize()
        this.+=(elem)
      case Failure(_: PathExistsException) => this
      case Failure(err)                    => throw err
    }
  }

  override def subtractOne(elem: T): this.type = {
    remove(elem)
    this
  }

  override def contains(elem: T): Boolean = {
    all().contains(elem)
  }
}
