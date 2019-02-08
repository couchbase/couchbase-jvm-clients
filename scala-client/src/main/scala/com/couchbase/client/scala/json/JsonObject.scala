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

import scala.language.dynamics
import java.util.Objects

import scala.annotation.switch
import scala.collection.GenSet
import scala.collection.mutable.ArrayBuffer

//import com.fasterxml.jackson.databind.ObjectMapper

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

import collection.JavaConverters._

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

  def getString(name: String): String = {
    (content.get(name)) match {
      case v: com.jsoniter.any.Any => v.toString
      case v: String => v
    }
  }

  def getInt(name: String): Int = {
    content.get(name).asInstanceOf[com.jsoniter.any.Any].toInt
//    content.get(name).asInstanceOf[Int]
  }

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

//  private type ContentType = Map[String, Any]

//  @tailrec
//  private def contentRecurse(cur: ContentType, paths: List[PathElement]): Any = {
//    paths match {
//      case Nil =>
//        throw new PathNotFound()
//
//      case x :: Nil =>
//        x match {
//          case x: PathArray =>
//            cur.get(x.name).map(_.asInstanceOf[JsonArray]) match {
//              case Some(y) => y.get(x.index)
//              case _ => throw new PathNotFound()
//            }
//          case x: PathObjectOrField =>
//            cur.get(x.toString) match {
//              case Some(y) => y
//              case _ => throw new PathNotFound()
//            }
//        }
//
//      case x :: rest =>
//        x match {
//          case x: PathArray =>
//            cur.get(x.name) match {
//              case None => throw new PathNotFound()
//              case Some(y: JsonArray) =>
//                val arr = y.get(x.index).asInstanceOf[JsonObject]
//                contentRecurse(arr.content, rest)
//            }
//
//          case x: PathObjectOrField =>
//            cur.get(x.toString) match {
//              case None => throw new PathNotFound()
//              case Some(z) =>
//                val next = z match {
//                  case jo: JsonObject => jo.content
//                  case _ => z.asInstanceOf[ContentType]
//                }
//                contentRecurse(next, rest)
//            }
//        }
//    }
//  }
//
//
//  override def contentAs[T](path: PathElements): T = {
//    contentRecurse(content, path.paths).asInstanceOf[T]
//  }

//  override def exists(path: PathElements): Boolean = {
//    // TODO more performant implementation without catch
//    try {
//      contentRecurse(content, path.paths)
//      true
//    }
//    catch {
//      case e: PathNotFound => false
//    }
//  }


  // TODO toMap
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

  // TODO this also needs to walk through recursively and find any JsonArrays & JsonObjects and convert them.  Am I sure I want immutability.
//  def mutable = JsonObjectMutable(collection.mutable.Map(content.toSeq: _*))
}


object JsonObject {
  // TOOD base on Jawn
//  private val mapper = new ObjectMapper()
//
//  def fromJson(json: String): JsonObject = {
//    mapper.readValue(json, classOf[JsonObject])
//  }

//  private val EMPTY = JsonObject.create
//
//  def empty: JsonObject = EMPTY

  def create: JsonObject = new JsonObject(new util.HashMap[String, Any]())

  // TODO from(Map)
}


//case class CouldNotEncodeToJsonType(in: Any) extends RuntimeException

