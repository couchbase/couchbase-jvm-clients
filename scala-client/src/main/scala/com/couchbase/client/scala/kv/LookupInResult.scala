package com.couchbase.client.scala.kv

import com.couchbase.client.core.annotation.Stability
import com.couchbase.client.core.api.kv.CoreSubdocGetResult

import java.time.Instant
import java.util.concurrent.TimeUnit
import com.couchbase.client.core.error.InvalidArgumentException
import com.couchbase.client.core.error.context.ReducedKeyValueErrorContext
import com.couchbase.client.core.msg.kv.{
  SubDocumentField,
  SubDocumentOpResponseStatus,
  SubdocCommandType
}
import com.couchbase.client.scala.codec.{JsonDeserializer, Transcoder}

import scala.compat.java8.OptionConverters._
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}
import scala.jdk.CollectionConverters._

/** The results of a SubDocument 'lookupIn' operation.
  *
  * When doing a `lookupIn` the application provides a sequence of [[LookupInSpec]].  The indexes into this sequence
  * are used when retrieving the results.
  *
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
case class LookupInResult private (
    private val internal: CoreSubdocGetResult,
    expiryTime: Option[Instant],
    transcoder: Transcoder
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
  @Stability.Uncommitted
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

private[scala] object LookupInResult {
  def contentAs[T](
      id: String,
      internal: CoreSubdocGetResult,
      index: Int,
      deserializer: JsonDeserializer[T],
      tag: ClassTag[T]
  ): Try[T] = {
    Try(internal.field(index))
      .flatMap(field => {
        field.error().asScala match {
          case Some(err) => Failure(err)
          case _ =>
            field.`type` match {
              case SubdocCommandType.EXISTS =>
                if (tag.runtimeClass.isAssignableFrom(classOf[Boolean])) {
                  val exists = field.status == SubDocumentOpResponseStatus.SUCCESS
                  Success(exists.asInstanceOf[T])
                } else {
                  Failure(
                    new InvalidArgumentException(
                      "Exists results can only be returned as Boolean",
                      null,
                      ReducedKeyValueErrorContext.create(id)
                    )
                  )
                }
              case _ => deserializer.deserialize(field.value)
            }
        }
      })
  }

  def exists(internal: CoreSubdocGetResult, index: Int): Boolean = {
    val content = internal.fields.asScala
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
