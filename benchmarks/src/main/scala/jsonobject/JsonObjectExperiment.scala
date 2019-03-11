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

package jsonobject

import java.util

import scala.language.dynamics
import java.util.{HashMap, Map, Objects}

import scala.collection.{GenMap, GenSet, mutable}
import scala.collection.mutable.ArrayBuffer

//import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.ObjectMapper

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

case class JsonObjectExperiment(private val content: java.util.HashMap[String, Any]) {

  // Don't make this Dynamic, it makes it easy to misuse
  //  def selectDynamic(name: String): GetSelecter = GetSelecter(this, PathElements(List(PathObjectOrField(name))))
  //  def applyDynamic(name: String)(index: Int): GetSelecter = GetSelecter(this, PathElements(List(PathArray(name, index))))

  def put(name: String, value: Any): JsonObjectExperiment = {
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
    content.get(name).asInstanceOf[String]
  }

  def getInt(name: String): Int = {
    content.get(name).asInstanceOf[Int]
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

  def getObject(name: String): JsonObjectExperiment = {
    content.get(name).asInstanceOf[JsonObjectExperiment]
  }

  def getArray(name: String): JsonArrayExperiment = {
    content.get(name).asInstanceOf[JsonArrayExperiment]
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

  def getObjectOpt(name: String): Option[JsonObjectExperiment] = {
    Option(content.get(name)).map(_.asInstanceOf[JsonObjectExperiment])
  }

  def getArrayOpt(name: String): Option[JsonArrayExperiment] = {
    Option(content.get(name)).map(_.asInstanceOf[JsonArrayExperiment])
  }

  def getBigIntOpt(name: String): Option[BigInt] = {
    Option(content.get(name)).map(_.asInstanceOf[BigInt])
  }

  def getBigDecimalOpt(name: String): Option[BigDecimal] = {
    Option(content.get(name)).map(_.asInstanceOf[BigDecimal])
  }

  def removeKey(name: String): JsonObjectExperiment = {
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
      case x: JsonObjectExperiment => true
      case x: JsonArrayExperiment => true
      case _ => false
    }
  }

  def toMap: collection.GenMap[String, Any] = {
    val copy = new mutable.AnyRefMap[String, Any](content.size)
    import scala.collection.JavaConverters._
    for (entry <- content.entrySet.asScala) {
      val content = entry.getValue
      content match {
        case v: JsonObjectExperiment => copy.put(entry.getKey, v.toMap)
        case v: JsonArrayExperiment => copy.put(entry.getKey, v.toSeq)
        case _ => copy.put(entry.getKey, content)
      }
    }
    copy
  }
}

object JsonObjectExperiment {
//  private val mapper = new ObjectMapper()
//
//  def fromJson(json: String): JsonObject = {
//    mapper.readValue(json, classOf[JsonObject])
//  }

//  private val EMPTY = JsonObject.create
//
//  def empty: JsonObject = EMPTY

  def create: JsonObjectExperiment = new JsonObjectExperiment(new util.HashMap[String, Any]())
  def empty: JsonObjectExperiment = new JsonObjectExperiment(new util.HashMap[String, Any]())
}


//case class CouldNotEncodeToJsonType(in: Any) extends RuntimeException

// Choosing vector as we'll mostly be adding to the end of it
case class JsonArrayExperiment(val values: ArrayBuffer[Any]) {
  def add(item: Any): JsonArrayExperiment = {
    values += item
    this
  }

  def addNull: JsonArrayExperiment = {
    values += null
    this
  }
  def get(idx: Int): Any = {
    values(idx)
  }
  def getOpt(idx: Int): Option[Any] = {
    Option(values(idx))
  }
  def getString(idx: Int): String = values(idx).asInstanceOf[String]
  def getLong(idx: Int): Long = values(idx).asInstanceOf[Long]
  def getInt(idx: Int): Int = values(idx).asInstanceOf[Int]
  def getDouble(idx: Int): Double = values(idx).asInstanceOf[Double]
  def getFloat(idx: Int): Float = values(idx).asInstanceOf[Float]
  def getObject(idx: Int): JsonObjectExperiment = values(idx).asInstanceOf[JsonObjectExperiment]
  def isEmpty: Boolean = values.isEmpty
  def iterator: Iterator[Any] = values.iterator
  def toSeq: Seq[Any] = values
  def toJavaList: java.util.List[Any] = values.asJava


//  def add(item: String): JsonArray = copy(values :+ JsonString(item))
//  def add(item: Int): JsonArray = copy(values :+ JsonNumber(item))
//  def addNull: JsonArray = copy(values :+ null)
//  def get(idx: Int): Option[JsonType] = Some(values(idx))
//  def str(idx: Int): String = values(idx).asInstanceOf[String]
//  def long(idx: Int): Long = values(idx).asInstanceOf[Long]
//  def int(idx: Int): Int = values(idx).asInstanceOf[Int]
//  def double(idx: Int): Double = values(idx).asInstanceOf[Double]
//  def float(idx: Int): Float = values(idx).asInstanceOf[Float]
//  def obj(idx: Int): JsonObject = values(idx).asInstanceOf[JsonObject]
//  def isEmpty: Boolean = values.isEmpty
//  def iterator: Iterator[Any] = values.iterator
//  //  def toSeq: Seq[JsonType] = values
}
//
object JsonArrayExperiment {
  def apply(items: Any*): JsonArrayExperiment = {
    new JsonArrayExperiment(ArrayBuffer[Any](items))
  }

  def create = new JsonArrayExperiment(new ArrayBuffer[Any]())
  def empty = new JsonArrayExperiment(new ArrayBuffer[Any]())
}
