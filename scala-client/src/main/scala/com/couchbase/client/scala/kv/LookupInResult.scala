package com.couchbase.client.scala.kv

import com.couchbase.client.core.msg.kv.{SubDocumentOpResponseStatus, SubdocField}
import com.couchbase.client.scala.codec.Conversions

import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}
import scala.compat.java8.OptionConverters._

/** The results of a SubDocument 'lookupIn' operation.
  *
  * When doing a `lookupIn` the application provides a sequence of [[LookupInSpec]].  The indexes into this sequence
  * are used when retrieving the results.
  *
  * @param id  the unique identifier of the document
  * @param cas the document's CAS value at the time of the lookup
  * @define Index          the index of the [[LookupInSpec]] provided to the `lookupIn`
  * @define SupportedTypes this can be of any type for which an implicit
  *                        [[com.couchbase.client.scala.codec.Conversions.Decodable]] can be found: a list
  *                        of types that are supported 'out of the box' is available at ***CHANGEME:TYPES***
  * @author Graham Pople
  * @since 1.0.0
  **/
case class LookupInResult(
                           id: String,
                           private val content: Seq[SubdocField],
                           private[scala] val flags: Int,
                           cas: Long,
                           private[scala] val expiration: Option[Duration]) {

  /** Retrieve the content returned for a particular `LookupInSpec`, converted into the application's preferred
    * representation.
    *
    * @param index $Index
    * @tparam T $SupportedTypes.  For an `exists` operation, only an output type of `Boolean` is supported.
    */
  def contentAs[T](index: Int)
                  (implicit ev: Conversions.Decodable[T]): Try[T] = {
    if (index < 0 || index >= content.size) {
      Failure(new IllegalArgumentException(s"$index is out of bounds"))
    }
    else {
      val field = content(index)
      field.error().asScala match {
        case Some(err) => Failure(err)
        case _ =>
          ev.decodeSubDocumentField(field, Conversions.JsonFlags)
      }
    }
  }


  /** Returns whether content has successfully been returned for a particular `LookupInSpec`.
    *
    * Important note: be careful with the naming similarity to the `exists` `LookupInSpec`, which will return a field
    * with this `exists(idx) == true` and
    * [[LookupInResult.contentAs`[Boolean`](idx) == true|false]]
    *
    * @param index $Index
    */
  def exists(index: Int): Boolean = {
    if (index < 0 || index >= content.size) {
      false
    }
    else {
      val field = content(index)
      field.error().asScala match {
        case Some(err) => false
        case _ => true
      }
    }
  }

  /** Retrieve the content returned for a particular `LookupInSpec`, as an `Array[Byte]`.
    *
    * @param index $Index
    */
  def contentAsBytes(index: Int): Try[Array[Byte]] = {
    if (index < 0 || index >= content.size) {
      Failure(new IllegalArgumentException(s"$index is out of bounds"))
    }
    else {
      val field = content(index)
      field.error().asScala match {
        case Some(err) => Failure(err)
        case _ => Success(field.value())
      }
    }
  }
}
