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

package com.couchbase.client.scala.json

import java.util

import com.couchbase.client.core.error.DecodingFailedException

import scala.collection.{GenSet, mutable}
import scala.language.dynamics
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}


/** A 'safe' wrapper around JsonObject that does all operations with Try rather than throwing exceptions.
  */
case class JsonObjectSafe(private[scala] val o: JsonObject) {
  private val content = o.content

  // Instead of making JsonObject itself Dynamic, which lends itself to all kinds of accidental errors, put it in a
  // separate method
  def dyn: GetSelecterSafe = GetSelecterSafe(Left(this), Seq())

  def put(name: String, value: Any): JsonObject = o.put(name, value)

  def get(name: String): Try[Any] = {
    Try(o.get(name))
  }

  def str(name: String): Try[String] = {
    Try(o.str(name))
  }

  def num(name: String): Try[Int] = {
    Try(o.num(name))
  }

  def bool(name: String): Try[Boolean] = {
    Try(o.bool(name))
  }

  def numLong(name: String): Try[Long] = {
    Try(o.numLong(name))
  }

  def numDouble(name: String): Try[Double] = {
    Try(o.numDouble(name))
  }

  def obj(name: String): Try[JsonObjectSafe] = {
    Try(o.obj(name)).map(JsonObjectSafe(_))
  }

  def arr(name: String): Try[JsonArraySafe] = {
    Try(o.arr(name)).map(JsonArraySafe)
  }

  def remove(name: String): JsonObject = o.remove(name)

  def containsKey(name: String): Boolean = o.containsKey(name)

  def names: GenSet[String] = o.names

  def isEmpty: Boolean = o.isEmpty

  def nonEmpty: Boolean = o.nonEmpty

  def toMap: collection.GenMap[String, Any] = o.toMap

  def size: Int = o.size

  def asString: Try[String] = {
    Try(o.toString)
  }

  override def toString: String = o.toString

}



object JsonObjectSafe {
  def fromJson(json: String): Try[JsonObjectSafe] = {
    Try(JsonObject.fromJson(json)).map(JsonObjectSafe(_))
  }

  def create: JsonObjectSafe = JsonObjectSafe(JsonObject.create)

  // Note, benchmarking indicates it's roughly twice as fast to simple create and put all fields, than it is to use this.
  def apply(values: (String,Any)*): JsonObjectSafe = {
    JsonObjectSafe(JsonObject(values: _*))
  }

  def apply(values: Map[String,Any]): JsonObjectSafe = {
    JsonObjectSafe(JsonObject(values))
  }
}
