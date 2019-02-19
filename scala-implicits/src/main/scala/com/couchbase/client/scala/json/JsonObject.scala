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
  def getOpt(name: String): Option[Any] = {
    Option(content.get(name))
  }

  def get(name: String): Any = {
    content.get(name)
  }


  // Instead of making JsonObject itself Dynamic, which lends itself to all kinds of accidental errors, put it in a
  // separate method
  def dyn: GetSelecter = GetSelecter(Left(this), Seq())

  def put(name: String, value: Any): JsonObject = {
    content.put(name, value)
//    Objects.requireNonNull(name)
//    if (!checkType(value)) throw new IllegalArgumentException("Unsupported type for JsonObject: " + value.getClass)
    this
  }

//  def put(name: String, value: Int): JsonObject = {
//    Objects.requireNonNull(name)
//    //    if (!checkType(value)) throw new IllegalArgumentException("Unsupported type for JsonObject: " + value.getClass)
//    copy(content + (name -> JsonNumber(value)))
//  }

//  def str(name: String): String = {
//    (content.get(name)) match {
//      case v: com.jsoniter.any.Any => v.toString
//      case v: String => v
//    }
//  }


  def str(name: String): String = {
    (content.get(name)) match {
      case v: String => v
    }
  }

  def int(name: String): Int = {
//    content.get(name).asInstanceOf[com.jsoniter.any.Any].toInt
        content.get(name).asInstanceOf[Int]
  }

//  def int(name: String): Int = {
//    content.get(name).asInstanceOf[com.jsoniter.any.Any].toInt
////    content.get(name).asInstanceOf[Int]
//  }

  // TODO handle and return nulls
  def bool(name: String): Boolean = {
    content.get(name).asInstanceOf[Boolean]
  }

  def long(name: String): Long = {
    content.get(name).asInstanceOf[Long]
  }

  def double(name: String): Double = {
    content.get(name).asInstanceOf[Double]
  }

  def obj(name: String): JsonObject = {
    content.get(name).asInstanceOf[JsonObject]
  }

  def arr(name: String): JsonArray = {
    content.get(name).asInstanceOf[JsonArray]
  }

//  def strOpt(name: String): Option[String] = {
//    Option(content.get(name)).map(_.asInstanceOf[String])
//  }
//
//  def intOpt(name: String): Option[Int] = {
//    Option(content.get(name)).map(_.asInstanceOf[Int])
//  }
//
//  def boolOpt(name: String): Option[Boolean] = {
//    Option(content.get(name)).map(_.asInstanceOf[Boolean])
//  }
//
//  def longOpt(name: String): Option[Long] = {
//    Option(content.get(name)).map(_.asInstanceOf[Long])
//  }
//
//  def doubleOpt(name: String): Option[Double] = {
//    Option(content.get(name)).map(_.asInstanceOf[Double])
//  }
//
//  def objOpt(name: String): Option[JsonObject] = {
//    Option(content.get(name)).map(_.asInstanceOf[JsonObject])
//  }
//
//  def arrOpt(name: String): Option[JsonArray] = {
//    Option(content.get(name)).map(_.asInstanceOf[JsonArray])
//  }

  def strTry(name: String): Try[String] = {
    val out = content.get(name)
    if (out == null) Failure(new IllegalArgumentException(s"Field $name does not exist"))
    else Try(out.asInstanceOf[String])
  }

  def intTry(name: String): Try[Int] = {
    val out = content.get(name)
    if (out == null) Failure(new IllegalArgumentException(s"Field $name does not exist"))
    else Try(out.asInstanceOf[Int])
  }

  def boolTry(name: String): Try[Boolean] = {
    val out = content.get(name)
    if (out == null) Failure(new IllegalArgumentException(s"Field $name does not exist"))
    else Try(out.asInstanceOf[Boolean])
  }

  def longTry(name: String): Try[Long] = {
    val out = content.get(name)
    if (out == null) Failure(new IllegalArgumentException(s"Field $name does not exist"))
    else Try(out.asInstanceOf[Long])
  }

  def doubleTry(name: String): Try[Double] = {
    val out = content.get(name)
    if (out == null) Failure(new IllegalArgumentException(s"Field $name does not exist"))
    else Try(out.asInstanceOf[Double])
  }

  def objTry(name: String): Try[JsonObject] = {
    val out = content.get(name)
    if (out == null) Failure(new IllegalArgumentException(s"Field $name does not exist"))
    else Try(out.asInstanceOf[JsonObject])
  }

  def arrTry(name: String): Try[JsonArray] = {
    val out = content.get(name)
    if (out == null) Failure(new IllegalArgumentException(s"Field $name does not exist"))
    else Try(out.asInstanceOf[JsonArray])
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
