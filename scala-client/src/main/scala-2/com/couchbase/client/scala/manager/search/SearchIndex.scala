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
package com.couchbase.client.scala.manager.search

import com.couchbase.client.core.annotation.Stability

import java.nio.charset.StandardCharsets
import com.couchbase.client.core.error.CouchbaseException
import com.couchbase.client.scala.codec.JsonDeserializer
import com.couchbase.client.scala.json.JsonObject
import com.couchbase.client.scala.util.CouchbasePickler

import scala.util.{Failure, Try}

private[scala] case class SearchIndexWrapper(
    indexDef: SearchIndex,
    planPIndexes: Option[Seq[ujson.Obj]]
) {
  def numPlanPIndexes = planPIndexes match {
    case Some(v) => v.size
    case _       => 0
  }
}

private[scala] object SearchIndexWrapper {
  implicit val rw: CouchbasePickler.ReadWriter[SearchIndexWrapper] = CouchbasePickler.macroRW
}

private[scala] case class SearchIndexesWrapper(indexDefs: Map[String, SearchIndex])

private[scala] object SearchIndexesWrapper {
  implicit val rw: CouchbasePickler.ReadWriter[SearchIndexesWrapper] = CouchbasePickler.macroRW
}

case class SearchIndex(
    name: String,
    sourceName: String,
    // The UUID is server-assigned. It should not be present on a created index, but must
    // be present on an updated index.
    uuid: Option[String] = None,
    @upickle.implicits.key("type") typ: Option[String] = None,
    private[scala] val params: Option[ujson.Obj] = None,
    @upickle.implicits.key("uuid") sourceUUID: Option[String] = None,
    private[scala] val sourceParams: Option[ujson.Obj] = None,
    sourceType: Option[String] = None,
    private[scala] val planParams: Option[ujson.Obj] = None,
    private[scala] val numPlanPIndexes: Int = 0
) {
  private val DefaultSourceType = "couchbase"
  private val DefaultType       = "fulltext-index"

  private[scala] def toJson: String = {
    val output = ujson.Obj()

    uuid.foreach(v => output.update("uuid", v))
    output.update("name", name)
    output.update("sourceName", sourceName)
    output.update("type", typ.getOrElse(DefaultType))
    output.update("sourceType", sourceType.getOrElse(DefaultSourceType))
    params.foreach(v => output.update("params", v))
    sourceParams.foreach(v => output.update("sourceParams", v))
    planParams.foreach(v => output.update("planParams", v))
    output.toString
  }

  def planParamsAs[T](implicit deserializer: JsonDeserializer[T]): Try[T] =
    convert(planParams, deserializer)

  def paramsAs[T](implicit deserializer: JsonDeserializer[T]): Try[T] =
    convert(params, deserializer)

  def sourceParamsAs[T](implicit deserializer: JsonDeserializer[T]): Try[T] =
    convert(sourceParams, deserializer)

  private def convert[T](value: Option[ujson.Obj], deserializer: JsonDeserializer[T]) = {
    value match {
      case Some(pp) =>
        val bytes = upickle.default.write(pp).getBytes(StandardCharsets.UTF_8)
        deserializer.deserialize(bytes)
      case _ => Failure(new CouchbaseException("Index does not contain this field"))
    }
  }
}

object SearchIndex {
  def create(name: String, sourceName: String): SearchIndex = {
    SearchIndex(name, sourceName)
  }

  def fromJson(json: String): Try[SearchIndex] = {
    Try(CouchbasePickler.read[SearchIndex](json))
  }

  implicit val rw: CouchbasePickler.ReadWriter[SearchIndex] = CouchbasePickler.macroRW
}
