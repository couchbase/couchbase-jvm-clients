package com.couchbase.client.scala.kv

import com.couchbase.client.core.msg.kv.{MutationToken, SubdocField}
import com.couchbase.client.scala.api.HasDurabilityTokens
import com.couchbase.client.scala.codec.Conversions

import scala.util.{Failure, Try}
import scala.compat.java8.OptionConverters._

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
