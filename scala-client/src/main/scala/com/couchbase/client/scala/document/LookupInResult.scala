package com.couchbase.client.scala.document

import com.couchbase.client.core.error.OperationDoesNotExist
import com.couchbase.client.core.msg.ResponseStatus
import com.couchbase.client.core.msg.kv.{SubDocumentOpResponseStatus, SubdocCommandType, SubdocField, SubdocGetResponse}
import com.couchbase.client.scala.codec.Conversions

import scala.collection.GenMap
import scala.util.{Failure, Success, Try}
import scala.concurrent.duration.Duration
import scala.compat.java8.OptionConverters._


case class LookupInResult(id: String,
                          private val body: Option[Array[Byte]],
                          private val _content: GenMap[String, SubdocField],
                          private[scala] val flags: Int,
                          cas: Long,
                          expiration: Option[Duration]) {

  def contentAs[T](path: String)
                  (implicit ev: Conversions.DecodableField[T]): Try[T] = {
    _content.get(path) match {
      case Some(field) =>
        field.error().asScala match {
          case Some(err) => Failure(err)
          case _ =>
            ev.decode(field, Conversions.JsonDecodeParams)
        }
      case _ => Failure(new OperationDoesNotExist(s"Operation $path could not be found in results"))
    }
  }

  // Only valid if the entire document was requested
  def documentAs[T]
  (implicit ev: Conversions.Decodable[T]): Try[T] = {
    body match {
      case Some(b) => ev.decode(b, Conversions.JsonDecodeParams)
      case _ => Failure(new OperationDoesNotExist("Content could not be parsed"))
    }
  }

  // Only valid if the entire document was requested
  def documentAsBytes = body

  def opStatus(path: String): Try[SubDocumentOpResponseStatus] = {
    _content.get(path) match {
      case Some(field) => Success(field.status())
      case _ => Failure(new OperationDoesNotExist(s"Operation $path could not be found in results"))
    }
  }
}

