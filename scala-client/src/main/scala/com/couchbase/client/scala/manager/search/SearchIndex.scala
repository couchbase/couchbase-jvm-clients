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

import java.nio.charset.StandardCharsets

import com.couchbase.client.core.error.CouchbaseException
import com.couchbase.client.scala.codec.JsonDeserializer
import com.couchbase.client.scala.json.JsonObject
import com.couchbase.client.scala.util.CouchbasePickler

import scala.util.{Failure, Try}

private[scala] case class SearchIndexWrapper private (indexDef: SearchIndex)

private[scala] object SearchIndexWrapper {
  implicit val rw: CouchbasePickler.ReadWriter[SearchIndexWrapper] = CouchbasePickler.macroRW
}

private[scala] case class SearchIndexesWrapper private (indexDefs: Map[String, SearchIndex])

private[scala] object SearchIndexesWrapper {
  implicit val rw: CouchbasePickler.ReadWriter[SearchIndexesWrapper] = CouchbasePickler.macroRW
}

case class SearchIndex private (
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
    private[scala] val planParams: Option[ujson.Obj] = None
) {
  private val DefaultSouceType = "couchbase"
  private val DefaultType      = "fulltext-index"

  private[scala] def toJson: String = {
    val output = JsonObject.create
    uuid.foreach(v => output.put("uuid", v))
    output.put("name", name)
    output.put("sourceName", sourceName)
    output.put("type", typ.getOrElse(DefaultType))
    output.put("sourceType", sourceType.getOrElse(DefaultSouceType))
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

  implicit val rw: CouchbasePickler.ReadWriter[SearchIndex] = CouchbasePickler.macroRW
}
