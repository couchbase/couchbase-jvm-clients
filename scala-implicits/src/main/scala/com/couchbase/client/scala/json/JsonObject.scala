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
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

//import com.fasterxml.jackson.databind.ObjectMapper

import scala.collection.JavaConverters._

/**
  * There are plenty of JSON libraries for Scala already.  Why make another one?
  *
  * Speed.  This is focussed 100% purely on performance.  It:
  *
  * - Doesn't use an abstract data type as that's slower.
  * - Uses a Java HashMap as they're substantially faster than the Scala equivalents.
  * - Is mutable as that's quick.
  * - It doesn't use Try as there's overhead there.
  */

case class JsonObject(private[scala] val content: java.util.HashMap[String, Any]) {

  // Instead of making JsonObject itself Dynamic, which lends itself to all kinds of accidental errors, put it in a
  // separate method
  def dyn: GetSelecter = GetSelecter(Left(this), Seq())

  def put(name: String, value: Any): JsonObject = {
    content.put(name, value)
//    Objects.requireNonNull(name)
//    if (!checkType(value)) throw new IllegalArgumentException("Unsupported type for JsonObject: " + value.getClass)
    this
  }

  def get(name: String): Any = {
    val check = content.get(name)
    if (check == null) if (!content.containsKey(name)) throw new IllegalArgumentException(s"Field $name does not exist")
    check
  }

  def str(name: String): String = {
    val check = content.get(name)
    if (check == null) if (!content.containsKey(name)) throw new IllegalArgumentException(s"Field $name does not exist")
    ValueConvertor.str(check, name)
  }

  def num(name: String): Int = {
    val check = content.get(name)
    if (check == null) if (!content.containsKey(name)) throw new IllegalArgumentException(s"Field $name does not exist")
    ValueConvertor.num(check, name)
  }

  def bool(name: String): Boolean = {
    val check = content.get(name)
    if (check == null) if (!content.containsKey(name)) throw new IllegalArgumentException(s"Field $name does not exist")
    ValueConvertor.bool(check, name)
  }

  def numLong(name: String): Long = {
    val check = content.get(name)
    if (check == null) if (!content.containsKey(name)) throw new IllegalArgumentException(s"Field $name does not exist")
    ValueConvertor.numLong(check, name)
  }

  def numDouble(name: String): Double = {
    val check = content.get(name)
    if (check == null) if (!content.containsKey(name)) throw new IllegalArgumentException(s"Field $name does not exist")
    ValueConvertor.numDouble(check, name)
  }

  def numFloat(name: String): Float = {
    val check = content.get(name)
    if (check == null) if (!content.containsKey(name)) throw new IllegalArgumentException(s"Field $name does not exist")
    ValueConvertor.numFloat(check, name)
  }

  def obj(name: String): JsonObject = {
    val check = content.get(name)
    if (check == null) if (!content.containsKey(name)) throw new IllegalArgumentException(s"Field $name does not exist")
    ValueConvertor.obj(check, name)
  }

  def arr(name: String): JsonArray = {
    val check = content.get(name)
    if (check == null) if (!content.containsKey(name)) throw new IllegalArgumentException(s"Field $name does not exist")
    ValueConvertor.arr(check, name)
  }

  def remove(name: String): JsonObject = {
    content.remove(name)
    this
  }

  def containsKey(name: String): Boolean = {
    content.containsKey(name)
  }

  def names: GenSet[String] = {
    content.keySet.asScala
  }

  def isEmpty: Boolean = {
    content.isEmpty
  }

  def nonEmpty: Boolean = {
    !content.isEmpty
  }

  def toMap: collection.GenMap[String, Any] = {
    val copy = new mutable.AnyRefMap[String, Any](content.size)
    import scala.collection.JavaConverters._
    for (entry <- content.entrySet.asScala) {
      val content = entry.getValue
      content match {
        case v: JsonObject => copy.put(entry.getKey, v.toMap)
        case v: JsonArray => copy.put(entry.getKey, v.toSeq)
        case _ => copy.put(entry.getKey, content)
      }
    }
    copy
  }

  def size: Int = {
    content.size
  }

  override def toString: String = {
    val sb = new StringBuilder
    sb += '{'
    val it = content.entrySet().iterator()
    while (it.hasNext) {
      val next = it.next()
      sb.append('"')
      sb.append(next.getKey)
      sb.append("\":")
      next.getValue match {
        case v: String =>
          sb += '"'
          sb.append(v)
          sb += '"'
        case v =>
          sb.append(v.toString)
      }
      if (it.hasNext) sb.append(',')
    }
      sb += '}'
    sb.toString()
  }

  def safe = JsonObjectSafe(this)
//  private def checkType(value: Any): Boolean = {
//    value match {
//      case x: String => true
//      case x: Int => true
//      case x: Long => true
//      case x: Double => true
//      case x: Boolean => true
//      case x: JsonObject => true
//      case x: JsonArray => true
//      case _ => false
//    }
//  }
}


object JsonObject {
  def fromJson(json: String): JsonObject = {
    try {
      JacksonTransformers.stringToJsonObject(json)
    }
    catch {
      case NonFatal(err) => throw new DecodingFailedException(err)
    }
  }

  def create: JsonObject = new JsonObject(new util.HashMap[String, Any]())

  // Note, benchmarking indicates it's roughly twice as fast to simple create and put all fields, than it is to use this.
  def apply(values: (String,Any)*): JsonObject = {
    val map = new util.HashMap[String, Any](values.size)
    values.foreach(v => map.put(v._1, v._2))
    new JsonObject(map)
  }

  def apply(values: Map[String,Any]): JsonObject = {
    val map = new util.HashMap[String, Any](values.size)
    values.foreach(v => map.put(v._1, v._2))
    new JsonObject(map)
  }
}
