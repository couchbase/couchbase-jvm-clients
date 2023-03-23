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
package com.couchbase.client.scala.search.result

import com.couchbase.client.core.api.search.result.{CoreSearchRowLocation, CoreSearchRowLocations}

import scala.jdk.CollectionConverters._

/**
  * Represents the locations of a search result row. Locations show
  * where a given term occurs inside of a given field.
  *
  * @since 1.0.0
  */
case class SearchRowLocations private (private val internal: CoreSearchRowLocations) {

  /** Gets all locations, for any field and term. */
  def getAll: Seq[SearchRowLocation] = {
    internal.getAll.asScala.map(l => SearchRowLocation(l))
  }

  /** Gets all locations for a given field (any term). */
  def get(field: String): Seq[SearchRowLocation] = {
    internal.get(field).asScala.map(l => SearchRowLocation(l))
  }

  /** Gets all locations for a given field and term. */
  def get(field: String, term: String): Seq[SearchRowLocation] = {
    internal.get(field, term).asScala.map(l => SearchRowLocation(l))
  }

  /** Gets all fields in these locations. */
  def fields: Seq[String] = {
    internal.fields.asScala
  }

  /** Gets all terms in these locations. */
  def terms: collection.Set[String] = {
    internal.terms.asScala
  }

  /** Gets all terms for a given field. */
  def termsFor(field: String): Seq[String] = {
    internal.termsFor(field).asScala
  }
}

/** An FTS result row location indicates at which position a given term occurs inside a given field.
  * In case the field is an array, `arrayPositions` will indicate which index/indices in the
  * array contain the term.
  *
  * @since 1.0.0
  */
case class SearchRowLocation private (private val internal: CoreSearchRowLocation) {
  def field: String = internal.field

  def term: String = internal.term

  /** The position of the term within the field, starting at 1. */
  def pos: Int = internal.pos.intValue

  /** The start offset (in bytes) of the term in the field. */
  def start: Int = internal.start.intValue

  /** The end offset (in bytes) of the term in the field. */
  def end: Int = internal.end.intValue

  /** Contains the positions of the term within any elements, if applicable. */
  def arrayPositions: Option[Seq[Int]] =
    Option(internal.arrayPositions).map(l => l.map(v => v.intValue).toSeq)
}
