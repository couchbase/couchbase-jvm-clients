/*
 * Copyright (c) 2023 Couchbase, Inc.
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
package com.couchbase.client.scala.kv

import com.couchbase.client.core.api.kv.CoreSubdocGetResult
import com.couchbase.client.scala.codec.JsonDeserializer

import java.time.Instant
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag
import scala.util.Try

/** The results of a SubDocument lookupIn operation when reading from replicas.
  *
  * When doing a `lookupIn` the application provides a sequence of [[LookupInSpec]].  The indexes into this sequence
  * are used when retrieving the results.
  *
  * @param isReplica whether this document is from a replica (versus the active)
  * @param expiryTime the document's expiration time, if it was fetched with the `withExpiry` flag set.  If that flag
  *                   was not set, this will be None.  The time is the point in time when the document expires.
  * @define Index          the index of the [[LookupInSpec]] provided to the `lookupIn`
  * @define SupportedTypes this can be of any type for which an implicit
  *                        `com.couchbase.client.scala.codec.JsonDeserializer` can be found: a list
  *                        of types that are supported 'out of the box' is available at
  *                        [[https://docs.couchbase.com/scala-sdk/current/howtos/json.html these JSON docs]]
  * @author Graham Pople
  * @since 1.0.0
  **/
case class LookupInReplicaResult private (
    private val internal: CoreSubdocGetResult,
    expiryTime: Option[Instant],
    isReplica: Boolean
) {

  /** The unique identifier of the document. */
  def id: String = internal.key

  /** The document's CAS value at the time of the lookup. */
  def cas: Long = internal.cas

  /** If the document was fetched with the `withExpiry` flag set then this will contain the
    * document's expiration value.  Otherwise it will be None.
    *
    * The time is expressed as a duration from the start of 'epoch time' until when the document expires.
    *
    * Also see [[expiryTime]] which also provides the expiration time, but in the form of the point of time at which
    * the document expires.
    */
  def expiry: Option[Duration] = expiryTime.map(i => Duration(i.getEpochSecond, TimeUnit.SECONDS))

  /** Retrieve the content returned for a particular `LookupInSpec`, converted into the application's preferred
    * representation.
    *
    * @param index $Index
    * @tparam T $SupportedTypes.  For an `exists` operation, only an output type of `Boolean` is supported.
    */
  def contentAs[T](
      index: Int
  )(implicit deserializer: JsonDeserializer[T], tag: ClassTag[T]): Try[T] = {
    LookupInResult.contentAs(id, internal, index, deserializer, tag)
  }

  /** Returns the raw JSON bytes of the content at the given index.
    *
    * Note that if the field is a string then it will be surrounded by quotation marks, as this is the raw response from
    * the server.  E.g. "foo" will return a 5-byte array.
    *
    * @param index the index of the subdoc value to retrieve.
    * @return the JSON content as a byte array
    */
  def contentAsBytes(index: Int): Try[Array[Byte]] = contentAs[Array[Byte]](index)

  /** Returns whether content has successfully been returned for a particular `LookupInSpec`.
    *
    * Important note: be careful with the naming similarity to the `exists` `LookupInSpec`, which will return a field
    * with this `exists(idx) == true` and
    * `.contentAs[Boolean](idx) == true|false`
    *
    * @param index $Index
    */
  def exists(index: Int): Boolean = {
    LookupInResult.exists(internal, index)
  }
}
