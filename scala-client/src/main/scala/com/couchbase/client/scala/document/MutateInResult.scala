package com.couchbase.client.scala.document

import com.couchbase.client.core.error.OperationDoesNotExist
import com.couchbase.client.core.msg.kv.{MutationToken, SubDocumentOpResponseStatus, SubdocField}
import com.couchbase.client.scala.api.HasDurabilityTokens
import com.couchbase.client.scala.codec.Conversions

import scala.collection.GenMap
import scala.compat.java8.OptionConverters._
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}


case class MutateInResult(id: String,
                          private val content: Seq[SubdocField],
                          cas: Long,
                          mutationToken: Option[MutationToken]) extends HasDurabilityTokens {

  def contentAs[T](index: Int)
                  (implicit ev: Conversions.Decodable[T]): Try[T] = {
    if (index < 0 || index >= content.size) {
      Failure(new IllegalArgumentException(s"$index is out of bounds"))
    }
    else {
      val field = content(index)
      field.error().asScala match {
        case Some(err) => Failure(err)
        case _ => ev.decodeSubDocumentField(field, Conversions.JsonFlags)
      }
    }
  }
}

