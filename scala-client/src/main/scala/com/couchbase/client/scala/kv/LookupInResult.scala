package com.couchbase.client.scala.kv

import com.couchbase.client.core.error.DecodingFailedException
import com.couchbase.client.core.msg.kv.{
  SubDocumentOpResponseStatus,
  SubdocCommandType,
  SubdocField
}
import com.couchbase.client.scala.codec.{Conversions, JsonDeserializer, JsonTranscoder, Transcoder}

import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}
import scala.compat.java8.OptionConverters._
import scala.reflect.runtime.universe._

/** The results of a SubDocument 'lookupIn' operation.
  *
  * When doing a `lookupIn` the application provides a sequence of [[LookupInSpec]].  The indexes into this sequence
  * are used when retrieving the results.
  *
  * @param id  the unique identifier of the document
  * @param cas the document's CAS value at the time of the lookup
  *
  * @define Index          the index of the [[LookupInSpec]] provided to the `lookupIn`
  * @define SupportedTypes this can be of any type for which an implicit
  *                        [[com.couchbase.client.scala.codec.JsonDeserializer]] can be found: a list
  *                        of types that are supported 'out of the box' is available at ***CHANGEME:TYPES***
  * @author Graham Pople
  * @since 1.0.0
  **/
case class LookupInResult(
    id: String,
    private val content: Seq[SubdocField],
    private[scala] val flags: Int,
    cas: Long,
    expiry: Option[Duration],
    transcoder: Transcoder
) {

  /** Retrieve the content returned for a particular `LookupInSpec`, converted into the application's preferred
    * representation.
    *
    * @param index $Index
    * @tparam T $SupportedTypes.  For an `exists` operation, only an output type of `Boolean` is supported.
    */
  def contentAs[T](
      index: Int
  )(implicit deserializer: JsonDeserializer[T], tag: TypeTag[T]): Try[T] = {
    if (index < 0 || index >= content.size) {
      Failure(new IllegalArgumentException(s"$index is out of bounds"))
    } else {
      val field = content(index)
      field.error().asScala match {
        case Some(err) => Failure(err)
        case _ =>
          field.`type` match {
            case SubdocCommandType.EXISTS =>
              if (tag.mirror.runtimeClass(tag.tpe).isAssignableFrom(classOf[Boolean])) {
                val exists = field.status == SubDocumentOpResponseStatus.SUCCESS
                Success(exists.asInstanceOf[T])
              } else {
                Failure(
                  new DecodingFailedException("Exists results can only be returned as Boolean")
                )
              }
            case _ => deserializer.deserialize(field.value)
          }
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
    } else {
      val field = content(index)
      field.error().asScala match {
        case Some(err) => false
        case _         => true
      }
    }
  }
}
