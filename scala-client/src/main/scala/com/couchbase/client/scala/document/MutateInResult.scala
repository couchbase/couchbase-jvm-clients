package com.couchbase.client.scala.document

import com.couchbase.client.core.error.OperationDoesNotExist
import com.couchbase.client.core.msg.kv.{MutationToken, SubDocumentOpResponseStatus, SubdocField}

import scala.collection.GenMap
import scala.compat.java8.OptionConverters._
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}


case class MutateInResult(id: String,
                          private val _content: GenMap[String, SubdocField],
                          cas: Long,
                          mutationToken: Option[MutationToken]) {

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
}

