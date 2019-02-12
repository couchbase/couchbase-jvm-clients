package com.couchbase.client.scala.document

import com.couchbase.client.core.error.CouchbaseException
import com.couchbase.client.scala.codec.{Conversions, EncodeParams}

import scala.util.Try
//import ujson.Obj
//import upickle.default.read

import scala.concurrent.duration.Duration


case class GetResult(id: String,
                private val _content: Array[Byte],
                     private[scala] val flags: Int,
                cas: Long,
                expiration: Option[Duration]) {

  def contentAsBytes: Array[Byte] = _content

  def contentAs[T]
  (implicit ev: Conversions.Decodable[T]): Try[T] = {
    ev.decode(_content, EncodeParams(flags))
  }
}

