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

package com.couchbase.client.scala.document
import scala.language.dynamics

import java.util.Objects

// TODO
case class JsonArray()

case class JsonObject(private val content: Map[String, Any]) extends Dynamic {
  def selectDynamic(name: String): Option[Any] = get(name)
  def updateDynamic(name: String)(value: Any): Unit = put(name, value)

  def put(name: String, value: Any): JsonObject = {
    Objects.requireNonNull(name)
    if (!checkType(value)) throw new IllegalArgumentException("Unsupported type for JsonObject: " + value.getClass)
    copy(content + (name -> value))
  }

  def putNull(name: String): JsonObject = {
    put(name, null)
  }

  def get(name: String): Option[Any] = {
    content.get(name)
  }

  def getString(name: String): Option[String] = {
    content.get(name).map(_.asInstanceOf[String])
  }

  def getInt(name: String): Option[Int] = {
    content.get(name).map(_.asInstanceOf[Int])
  }

  def getLong(name: String): Option[Long] = {
    content.get(name).map(_.asInstanceOf[Long])
  }

  def getDouble(name: String): Option[Double] = {
    content.get(name).map(_.asInstanceOf[Double])
  }

  def getBoolean(name: String): Option[Boolean] = {
    content.get(name).map(_.asInstanceOf[Boolean])
  }

  def getObject(name: String): Option[JsonObject] = {
    content.get(name).map(_.asInstanceOf[JsonObject])
  }

  def getArray(name: String): Option[JsonArray] = {
    content.get(name).map(_.asInstanceOf[JsonArray])
  }

  def getBigInt(name: String): Option[BigInt] = {
    content.get(name).map(_.asInstanceOf[BigInt])
  }

  def getBigDecimal(name: String): Option[BigDecimal] = {
    content.get(name).map(_.asInstanceOf[BigDecimal])
  }

  def removeKey(name: String): JsonObject = {
    copy(content - name)
  }

  def names: Set[String] = {
    content.keySet
  }

  def isEmpty: Boolean = {
    content.isEmpty
  }

  // TODO toMap
  // TODO toString

  def containsKey(name: String): Boolean = {
    content.contains(name)
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
      case x: JsonObject => true
      case x: JsonArray => true
      case _ => false
    }
  }
}


object JsonObject {
  private val EMPTY = JsonObject.create()

  def empty(): JsonObject = EMPTY

  def create(): JsonObject = EMPTY

  // TODO from(Map)
  // TODO fromJson
}