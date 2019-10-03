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
import com.couchbase.client.scala.codec.Conversions
import com.couchbase.client.scala.json.JsonObject
import com.couchbase.client.scala.transformers.JacksonTransformers
import reactor.core.scala.publisher.{Flux, Mono}
import com.couchbase.client.core.logging.RedactableArgument.redactUser

import scala.util.{Success, Try}

/** The results of a view request.
  *
  * @param rows            all rows returned from the view
  * @param meta            any additional information associated with the view result
  *
  * @author Graham Pople
  * @since 1.0.0
  */
case class ViewResult(meta: ViewMetaData,
                      rows: Seq[ViewRow])

/** The results of a N1QL view, as returned by the reactive API.
  *
  * @param rows            a Flux of any returned rows.  If the view service returns an error while returning the
  *                        rows, it will be raised on this Flux
  * @param meta            contains additional information related to the view.
  */
case class ReactiveViewResult(meta: Mono[ViewMetaData],
                              rows: Flux[ViewRow])

/** An individual view result row.
  *
  * @define SupportedTypes this can be of any type for which an implicit
  *                        [[com.couchbase.client.scala.codec.Conversions.Decodable]] can be found: a list
  *                        of types that are supported 'out of the box' is available at ***CHANGEME:TYPES***
  */
case class ViewRow(private val _content: Array[Byte]) {

  private val rootNode: Try[JsonNode] = Try(JacksonTransformers.MAPPER.readTree(_content))

  /** Return the value, converted into the application's preferred representation.
    *
    * @tparam T $SupportedTypes
    */
  def valueAs[T]
  (implicit ev: Conversions.Decodable[T]): Try[T] = {
    rootNode
      .map(rn => rn.get("value"))
      .flatMap(key => ev.decode(key.binaryValue(), Conversions.JsonFlags))
  }

  /** Return the key, converted into the application's preferred representation.
    *
    * @tparam T $SupportedTypes
    */
  def keyAs[T]
  (implicit ev: Conversions.Decodable[T]): Try[T] = {
    rootNode
      .map(rn => rn.get("key"))
      .flatMap(key => ev.decode(key.binaryValue(), Conversions.JsonFlags))
  }

  /** The id of this row.
    */
  def id: Try[String] = {
    rootNode
      .map(rn => rn.get("id").asText())
  }

  override def toString: String = rootNode match {
    case Success(rn) => redactUser(rn.toString).toString
    case _ => "could not decode"
  }
}

/** Returns any debug information from a view request. */
case class ViewDebug(private val _content: Array[Byte]) {
  /** Return the content as an `Array[Byte]` */
  def contentAsBytes: Array[Byte] = {
    _content
  }

  /** Return the content, converted into the application's preferred representation.
    *
    * The content is JSON array, so a suitable  representation would be
    * [[com.couchbase.client.scala.json.JsonObject]].
    *
    * @tparam T $SupportedTypes
    */
  def contentAs[T]
  (implicit ev: Conversions.Decodable[T]): Try[T] = {
    ev.decode(_content, Conversions.JsonFlags)
  }

  override def toString: String = contentAs[JsonObject].get.toString
}


/** Additional information returned by the view service aside from any rows and errors.
  *
  * @param debug            any debug information available from the view service
  * @param totalRows        the total number of returned rows
  */
case class ViewMetaData(debug: Option[ViewDebug],
                        totalRows: Long)

