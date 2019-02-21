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

case class JsonObject(private val content: java.util.HashMap[String, Any]) {

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
    content.get(name)
  }

  def str(name: String): String = {
    val out = content.get(name)
    out match {
      case v: String => v
      case null => throw new IllegalArgumentException(s"Field $name not found")
      case _ => out.toString
    }
  }

  // TODO test all these fields with e.g. getting an int as a string
  // TODO it's valid for these fields to contain null, should only throw if it's not there.  Can we cheaply determine?
  def int(name: String): Int = {
    val out = content.get(name)
    out match {
      case v: Int => v
      case v: Long => v.toInt
      case v: Double => v.toInt
      case v: Short => v.toInt
      case v: String => v.toInt
      case null => throw new IllegalArgumentException(s"Field $name not found")
      case _ => throw new IllegalArgumentException(s"Field $name '$out' cannot be converted to Int")
    }
  }

  def bool(name: String): Boolean = {
    val out = content.get(name)
    out match {
      case v: Boolean => v
      case null => throw new IllegalArgumentException(s"Field $name not found")
      case _ => throw new IllegalArgumentException(s"Field $name '$out' cannot be converted to Boolean")
    }
  }

  def long(name: String): Long = {
    val out = content.get(name)
    out match {
      case v: Long => v
      case v: Int => v.toLong
      case v: Double => v.toInt
      case v: Short => v.toInt
      case v: String => v.toInt
      case null => throw new IllegalArgumentException(s"Field $name not found")
      case _ => throw new IllegalArgumentException(s"Field $name '$out' cannot be converted to Long")
    }
  }

  def double(name: String): Double = {
    val out = content.get(name)
    out match {
      case v: Double => v.toInt
      case v: Long => v
      case v: Int => v.toLong
      case v: Short => v.toInt
      case v: String => v.toInt
      case null => throw new IllegalArgumentException(s"Field $name not found")
      case _ => throw new IllegalArgumentException(s"Field $name '$out' cannot be converted to Double")
    }
  }

  def obj(name: String): JsonObject = {
    val out = content.get(name)
    out match {
      case v: JsonObject => v
      case null => throw new IllegalArgumentException(s"Field $name not found")
      case _ => throw new IllegalArgumentException(s"Field $name '$out' cannot be converted to JsonObject")
    }
  }

  def arr(name: String): JsonArray = {
    val out = content.get(name)
    out match {
      case v: JsonArray => v
      case null => throw new IllegalArgumentException(s"Field $name not found")
      case _ => throw new IllegalArgumentException(s"Field $name '$out' cannot be converted to JsonArray")
    }
  }


  def gett(name: String): Try[Any] = {
    Try(get(name))
  }

  def strt(name: String): Try[String] = {
    Try(str(name))
  }

  def intt(name: String): Try[Int] = {
    Try(int(name))
  }

  def boolt(name: String): Try[Boolean] = {
    Try(bool(name))
  }

  def longt(name: String): Try[Long] = {
    Try(long(name))
  }

  def doublet(name: String): Try[Double] = {
    Try(double(name))
  }

  def objt(name: String): Try[JsonObject] = {
    Try(obj(name))
  }

  def arrt(name: String): Try[JsonArray] = {
    Try(arr(name))
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

  // TODO make output valid JSON
  override def toString: String = {
    val sb = new StringBuilder
    sb += '{'
    val it = content.entrySet().iterator()
    while (it.hasNext) {
      val next = it.next()
      sb.append(next.getKey)
      sb += ':'
      sb.append(next.getValue.toString)
      if (it.hasNext) sb.append(',')
    }
      sb += '}'
    sb.toString()
  }

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
  def fromJson(json: String): Try[JsonObject] = {
    try {
      Success(JacksonTransformers.stringToJsonObject(json))
    }
    catch {
      case NonFatal(err) => Failure(new DecodingFailedException(err))
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
