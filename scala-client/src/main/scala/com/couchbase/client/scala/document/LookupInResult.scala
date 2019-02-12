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
                          private val content: Seq[SubdocField],
                          private[scala] val flags: Int,
                          cas: Long,
                          expiration: Option[Duration]) {

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

  def opStatus(index: Int): Try[SubDocumentOpResponseStatus] = {
    if (index < 0 || index >= content.size) {
      Failure(new IllegalArgumentException(s"$index is out of bounds"))
    }
    else {
      val field = content(index)
      Success(field.status())
    }
  }
}

