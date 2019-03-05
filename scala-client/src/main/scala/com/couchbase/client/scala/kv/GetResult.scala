package com.couchbase.client.scala.kv

import com.couchbase.client.scala.codec.{Conversions, EncodeParams}
import com.couchbase.client.scala.json.JsonObject

import scala.concurrent.duration.Duration
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

/** The result of a `get` operation, e.g. the contents of a document.
  *
  * @param id         the unique identifier of the document
  * @param cas        the document's CAS value at the time of the lookup
  * @param expiration if the document was fetched with the `withExpiration` flag set then this will contain the
  *                   document's expiration value.  Otherwise it will be None.
  * @define SupportedTypes this can be of any type for which an implicit
  *                        [[com.couchbase.client.scala.codec.Conversions.Decodable]] can be found: a list
  *                        of types that are supported 'out of the box' is available at ***CHANGEME:TYPES***
  */
case class GetResult(id: String,
                     // It's Right only in the case where projections were requested
                     private val _content: Either[Array[Byte], JsonObject],
                     private[scala] val flags: Int,
                     cas: Long,
                     expiration: Option[Duration]) {

  /** Return the content, converted into the application's preferred representation.
    *
    * <b>Projections</b>: if the advanced feature projections has been used (e.g. if a `project` array was provided
    * to the `get` call), then the results can only be converted into a
    * [[JsonObject]] - though it would be possible to change this, please let us know if you'd like to see support for
    * other types.
    *
    * @tparam T $SupportedTypes
    */
  def contentAs[T]
  (implicit ev: Conversions.Decodable[T], tag: ClassTag[T]): Try[T] = {
    _content match {
      case Left(bytes) =>
        // Regular case
        ev.decode(bytes, EncodeParams(flags))

      case Right(obj) =>
        // Projection
        tag.unapply(obj) match {
          case Some(o) => Success(o)
          // TODO looks weird...
          case _ => Failure(new IllegalArgumentException("Projection results can currently only be returned with " +
            "contentAs[JsonObject]"))
        }
    }
  }

  /** Return the content as an `Array[Byte]` */
  def contentAsBytes: Array[Byte] = _content match {
    case Left(bytes) => bytes
    case Right(obj) =>
      // A JsonObject can always be converted to Array[Byte], so the get is safe
      Conversions.encode(obj).map(_._1).get
  }
}
