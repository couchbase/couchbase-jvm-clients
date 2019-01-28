package com.couchbase.client.scala.document

import com.couchbase.client.core.error.FieldDoesNotExist
import com.couchbase.client.core.msg.kv.SubdocGetResponse

import scala.collection.GenMap
import scala.util.{Failure, Try}
//import ujson.Obj
//import upickle.default.read

import scala.concurrent.duration.Duration
//import scala.language.dynamics
//import scala.reflect.runtime.universe._
//import upickle.default._


case class LookupInResult(id: String,
                          private val body: Option[Array[Byte]],
                                          private val _content: GenMap[String, SubdocGetResponse.ResponseValue],
                     cas: Long,
                     expiration: Option[Duration]) {





//  def contentAsObject: JsonObject = ???
//
//  def contentAsObject(path: String): JsonObject = contentAs[JsonObject](path)
//
//  def contentAsArray: JsonArray = ???
//
//  def contentAsArray(path: String): JsonArray = contentAs[JsonArray](path)

//  def content: JsonType = ???

//  def contentAsBytes: Array[Byte] = _content

//  def contentAsStr: String = {
//    import upickle.default._
//
//    read[String](_content)
//  }

//  def content(idx: Int): JsonType = ???
//
//  def content(path: String): JsonType = ???

  // TODO MVP improve
//  def contentAsUjson = {
//
////    ByteArrayParser.transform(_content, new BaseRenderer)
////    transform(Readable.fromByteArray(_content), BytesRenderer())
//    read[ujson.Obj](_content)
//  }

//  def contentAs[T]
//  (implicit ev: Conversions.Decodable[T]): Try[T] = {
//    ev.decode(_content, Conversions.JsonDecodeParams)
//  }

  // TODO support
  def fieldAs[T](path: String)
                (implicit ev: Conversions.DecodableField[T]): Try[T] = {
    _content.get(path) match {
      case Some(field) =>
        // TODO check status
        ev.decode(field.value(), Conversions.JsonDecodeParams)
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

//  def contentAs[T](path: String, decoder: Array[Byte] => T): T = ???

  // TODO MVP decide: nope, far too easy to get this wrong, and drops you into GetSelector.  Must do .dyn or something first.
//  def selectDynamic(name: String): GetSelecter = GetSelecter(this, PathElements(List(PathObjectOrField(name))))
//  def applyDynamic(name: String)(index: Int): GetSelecter = GetSelecter(this, PathElements(List(PathArray(name, index))))

//  override def exists(path: PathElements): Boolean = ???

//  override def contentAs[T](path: PathElements): T = ???
}

