package com.couchbase.client.scala.kv

import com.couchbase.client.scala.codec.{Conversions, EncodeParams}
import com.couchbase.client.scala.json.JsonObject

import scala.concurrent.duration.Duration
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

case class GetResult(id: String,
                    // It's Right only in the case where projections were requested
                     private val _content: Either[Array[Byte], JsonObject],
                     private[scala] val flags: Int,
                     cas: Long,
                     expiration: Option[Duration]) {

  def contentAsBytes: Array[Byte] = _content match {
    case Left(bytes) => bytes
    case Right(obj) =>
      // A JsonObject can always be converted to Array[Byte], so the get is safe
      Conversions.encode(obj).map(_._1).get
  }

  // TODO support contentAs[JsonObjectSafe]
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
          case _ => Failure(new IllegalArgumentException("Projection results can currently only be returned with contentAs[JsonObject]"))
        }
    }
  }
}
