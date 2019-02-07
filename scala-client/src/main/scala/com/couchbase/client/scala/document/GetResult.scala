package com.couchbase.client.scala.document

import com.couchbase.client.core.error.CouchbaseException
import com.couchbase.client.scala.codec.Conversions

import scala.util.Try
//import ujson.Obj
//import upickle.default.read

import scala.concurrent.duration.Duration

trait Convertable {
  def contentAs[T](path: PathElements): T
  def exists(path: PathElements): Boolean
}

class PathNotFound extends CouchbaseException

sealed trait PathElement
case class PathObjectOrField(name: String) extends PathElement {
  override def toString: String = name
}
case class PathArray(name: String, index: Int) extends PathElement {
  override def toString: String = name + "[" + index + "]"
}

case class PathElements(paths: List[PathElement]) {
  def add(elem: PathElement) = copy(paths :+ elem)

  override def toString: String = {
    paths.map(_.toString).mkString(".")
  }
}


case class GetResult(id: String,
                private val _content: Array[Byte],
                     private[scala] val flags: Int,
                cas: Long,
                expiration: Option[Duration]) {

  def contentAsBytes: Array[Byte] = _content

  def contentAs[T]
  (implicit ev: Conversions.Decodable[T]): Try[T] = {
    ev.decode(_content, DecodeParams(flags))
  }

  // TODO support
//  def contentAs[T](path: String): Try[T] = {
//
//  }

  // TODO MVP decide: nope, far too easy to get this wrong, and drops you into GetSelector.  Must do .dyn or something first.
//  def selectDynamic(name: String): GetSelecter = GetSelecter(this, PathElements(List(PathObjectOrField(name))))
//  def applyDynamic(name: String)(index: Int): GetSelecter = GetSelecter(this, PathElements(List(PathArray(name, index))))

//  override def exists(path: PathElements): Boolean = ???

//  override def contentAs[T](path: PathElements): T = ???
}

object GetResult {
//  def unapply[T](document: GetResult): Option[(String, JsonObject, Long, Option[Duration])] = {
//    Some(document.id, document.contentAsObject, document.cas, document.expiration)
//  }

  // TODO can get this working along with unapply
//    def unapplySeq(doc: ReadResult): Option[Seq[Any]] = null
}