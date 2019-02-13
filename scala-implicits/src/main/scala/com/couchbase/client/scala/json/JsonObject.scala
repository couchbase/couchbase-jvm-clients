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

import scala.collection.{GenSet, mutable}
import scala.language.dynamics

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

case class JsonObject(val content: java.util.HashMap[String, Any]) {

  // Don't make this Dynamic, it makes it easy to misuse
  // TODO going back and forwards on this
  //  def dyn(): GetSelecter = GetSelecter(this, "")

//  def selectDynamic(name: String): GetSelecter = GetSelecter(this, PathElements(List(PathObjectOrField(name))))
//  def applyDynamic(name: String)(index: Int): GetSelecter = GetSelecter(this, PathElements(List(PathArray(name, index))))

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

//  def getString(name: String): String = {
//    (content.get(name)) match {
//      case v: com.jsoniter.any.Any => v.toString
//      case v: String => v
//    }
//  }


  def getString(name: String): String = {
    (content.get(name)) match {
      case v: String => v
    }
  }

  def getInt(name: String): Int = {
//    content.get(name).asInstanceOf[com.jsoniter.any.Any].toInt
        content.get(name).asInstanceOf[Int]
  }

//  def getInt(name: String): Int = {
//    content.get(name).asInstanceOf[com.jsoniter.any.Any].toInt
////    content.get(name).asInstanceOf[Int]
//  }

  def getBoolean(name: String): Boolean = {
    content.get(name).asInstanceOf[Boolean]
  }

  def getLong(name: String): Long = {
    content.get(name).asInstanceOf[Long]
  }

  def getDouble(name: String): Double = {
    content.get(name).asInstanceOf[Double]
  }

  def getObject(name: String): JsonObject = {
    content.get(name).asInstanceOf[JsonObject]
  }

  def getArray(name: String): JsonArray = {
    content.get(name).asInstanceOf[JsonArray]
  }

  def getBigInt(name: String): BigInt = {
    content.get(name).asInstanceOf[BigInt]
  }

  def getBigDecimal(name: String): BigDecimal = {
    content.get(name).asInstanceOf[BigDecimal]
  }


  def getStringOpt(name: String): Option[String] = {
    Option(content.get(name)).map(_.asInstanceOf[String])
  }

  def getIntOpt(name: String): Option[Int] = {
    Option(content.get(name)).map(_.asInstanceOf[Int])
  }

  def getBooleanOpt(name: String): Option[Boolean] = {
    Option(content.get(name)).map(_.asInstanceOf[Boolean])
  }

  def getLongOpt(name: String): Option[Long] = {
    Option(content.get(name)).map(_.asInstanceOf[Long])
  }

  def getDoubleOpt(name: String): Option[Double] = {
    Option(content.get(name)).map(_.asInstanceOf[Double])
  }

  def getObjectOpt(name: String): Option[JsonObject] = {
    Option(content.get(name)).map(_.asInstanceOf[JsonObject])
  }

  def getArrayOpt(name: String): Option[JsonArray] = {
    Option(content.get(name)).map(_.asInstanceOf[JsonArray])
  }

  def getBigIntOpt(name: String): Option[BigInt] = {
    Option(content.get(name)).map(_.asInstanceOf[BigInt])
  }

  def getBigDecimalOpt(name: String): Option[BigDecimal] = {
    Option(content.get(name)).map(_.asInstanceOf[BigDecimal])
  }

  def removeKey(name: String): JsonObject = {
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

  // TODO toString

  def size: Int = {
    content.size
  }

  private def checkType(value: Any): Boolean = {
    value match {
      case x: String => true
      case x: Int => true
      case x: Long => true
      case x: Double => true
      case x: Boolean => true
      case x: BigInt => true
      case x: BigDecimal => true
      case x: JsonObject => true
      case x: JsonArray => true
      case _ => false
    }
  }
}


object JsonObject {
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
