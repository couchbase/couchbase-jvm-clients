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

package com.couchbase.client.scala.view

import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode
import com.couchbase.client.core.logging.RedactableArgument.redactUser
import com.couchbase.client.scala.codec.{Conversions, JsonDeserializer}
import com.couchbase.client.scala.json.JsonObject
import com.couchbase.client.scala.transformers.JacksonTransformers
import reactor.core.scala.publisher.{SFlux, SMono}

import scala.util.{Success, Try}

/** The results of a view request.
  *
  * @param rows            all rows returned from the view
  * @param meta            any additional information associated with the view result
  *
  * @author Graham Pople
  * @since 1.0.0
  */
case class ViewResult(meta: ViewMetaData, rows: Seq[ViewRow])

/** The results of a N1QL view, as returned by the reactive API.
  *
  * @param rows            a Flux of any returned rows.  If the view service returns an error while returning the
  *                        rows, it will be raised on this Flux
  * @param meta            contains additional information related to the view.
  */
case class ReactiveViewResult(meta: SMono[ViewMetaData], rows: SFlux[ViewRow])

/** An individual view result row.
  *
  * @define SupportedTypes this can be of any type for which an implicit
  *                        [[com.couchbase.client.scala.codec.JsonDeserializer]] can be found: a list
  *                        of types that are supported 'out of the box' is available at ***CHANGEME:TYPES***
  */
case class ViewRow(private val _content: Array[Byte]) {

  private val rootNode: Try[JsonNode] = Try(JacksonTransformers.MAPPER.readTree(_content))

  /** Return the value, converted into the application's preferred representation.
    *
    * @tparam T $SupportedTypes
    */
  def valueAs[T](implicit deserializer: JsonDeserializer[T]): Try[T] = {
    rootNode
      .map(rn => rn.get("value"))
      .flatMap(key => deserializer.deserialize(key.binaryValue()))
  }

  /** Return the key, converted into the application's preferred representation.
    *
    * @tparam T $SupportedTypes
    */
  def keyAs[T](implicit deserializer: JsonDeserializer[T]): Try[T] = {
    rootNode
      .map(rn => rn.get("key"))
      .flatMap(key => deserializer.deserialize(key.binaryValue()))
  }

  /** The id of this row.
    */
  def id: Try[String] = {
    rootNode
      .map(rn => rn.get("id").asText())
  }

  override def toString: String = rootNode match {
    case Success(rn) => redactUser(rn.toString).toString
    case _           => "could not decode"
  }
}

/** Additional information returned by the view service aside from any rows and errors.
  *
  * @param debug            any debug information available from the view service
  * @param totalRows        the total number of returned rows
  */
case class ViewMetaData(private val debug: Array[Byte], totalRows: Long) {

  /** Return the content, converted into the application's preferred representation.
    *
    * The content is JSON array, so a suitable  representation would be
    * [[com.couchbase.client.scala.json.JsonObject]].
    *
    * @tparam T $SupportedTypes
    */
  def debugAs[T](implicit deserializer: JsonDeserializer[T]): Try[T] = {
    deserializer.deserialize(debug)
  }
}
