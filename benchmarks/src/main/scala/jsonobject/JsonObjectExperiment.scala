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

package jsonobject

import java.util

import scala.language.dynamics
import java.util.{HashMap, Map, Objects}

import scala.collection.{GenMap, GenSet, mutable}
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

case class JsonObjectExperiment(private val content: java.util.HashMap[String, Any]) {

  // Don't make this Dynamic, it makes it easy to misuse
  // TODO going back and forwards on this
  //  def dyn(): GetSelecter = GetSelecter(this, "")

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
      case x: JsonObjectExperiment => true
      case x: JsonArrayExperiment => true
      case _ => false
    }
  }

  def toMap: Map[String, Any] = {
    val copy = new mutable.AnyRefMap[String, Any](content.size)
    import scala.collection.JavaConversions._
    for (entry <- content.entrySet) {
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

  // TODO from(Map)
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
  // TODO does getLong & getInt make sense?  Always using wrong one...
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
//  def getString(idx: Int): String = values(idx).asInstanceOf[String]
//  def getLong(idx: Int): Long = values(idx).asInstanceOf[Long]
//  def getInt(idx: Int): Int = values(idx).asInstanceOf[Int]
//  def getDouble(idx: Int): Double = values(idx).asInstanceOf[Double]
//  def getFloat(idx: Int): Float = values(idx).asInstanceOf[Float]
//  def getObject(idx: Int): JsonObject = values(idx).asInstanceOf[JsonObject]
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
//  private val mapper = new ObjectMapper()
//
//  def fromJson(json: String): JsonArray = {
//    mapper.readValue(json, classOf[JsonArray])
//  }
//
//  def toJsonType(in: Any): Try[JsonType] = {
//    in match {
//      case x: String => Success(JsonString(x))
//      case x: Int => Success(JsonNumber(x))
//      case x: Long => Success(JsonNumber(x))
//      case x: Double => Success(JsonNumber(x))
//      case x: Boolean => Success(JsonBoolean(x))
//        // TODO MVP Seq
////      case x: Seq[_] =>
////        val arr: Try[Seq[JsonType]] = x.map(toJsonType(_))
////          arr.map(v => JsonArray(v)))
//      case _ => Failure(CouldNotEncodeToJsonType(in))
//    }
//  }
//
//  // TODO checkItems
//  def from(items: Any*): Try[JsonArray] = {
//    // TODO MVP
//    ???
////    val arr = items.map(toJsonType(_))
////    new JsonArray(.toVector
//  }
//  // TODO more advanced from that converts into JsonObjects etc
//
//  private val EMPTY = JsonArray.create
//
//  def empty: JsonArray = EMPTY
//
//  def create: JsonArray = new JsonArray(Vector.empty)
//
//  // TODO from(Map)
//}