package com.couchbase.client.scala.document

import com.couchbase.client.core.error.FieldDoesNotExist
import com.couchbase.client.core.msg.kv.SubdocGetRequest.CommandType
import com.couchbase.client.core.msg.kv.SubdocGetResponse

import scala.collection.GenMap
import scala.util.{Failure, Try}
//import ujson.Obj
//import upickle.default.read

import scala.concurrent.duration.Duration
import scala.compat.java8.OptionConverters._


case class LookupInResult(id: String,
                          private val body: Option[Array[Byte]],
                          private val _content: GenMap[String, SubdocGetResponse.ResponseValue],
                          cas: Long,
                          expiration: Option[Duration]) {

  def fieldAs[T](path: String)
                (implicit ev: Conversions.DecodableField[T]): Try[T] = {
    _content.get(path) match {
      case Some(field) =>
        field.error().asScala match {
          case Some(err) => Failure(err)
          case _ =>
            ev.decode(field, Conversions.JsonDecodeParams)
        }
      case _ => Failure(new FieldDoesNotExist(s"Field ${path} could not be found in results"))
    }
  }

  // TODO unsure on naming, LookupResult is not in rfc
  def bodyAs[T]
  (implicit ev: Conversions.Decodable[T]): Try[T] = {
    body match {
      case Some(b) => ev.decode(b, Conversions.JsonDecodeParams)
      case _ => Failure(new FieldDoesNotExist(s"Document body could not be found in results"))
    }
  }

  def bodyAsBytes = body
}

