package com.couchbase.client.scala.document

import com.couchbase.client.core.error.CouchbaseException

import scala.concurrent.duration.Duration
import scala.language.dynamics

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

// TODO much more implementation required
case class GetSelecter(private val result: Convertable, path: PathElements) extends Dynamic {
  def selectDynamic(name: String): GetSelecter = GetSelecter(result, path.add(PathObjectOrField(name)))
  def applyDynamic(name: String)(index: Int): GetSelecter = GetSelecter(result, path.add(PathArray(name, index)))

  def exists: Boolean = result.exists(path)
  def getString: String = result.contentAs[String](path)
  // TODO see what Jackson transcoding produces in terms of ints, longs, floats, doubles
  def getInt: Int = result.contentAs[Int](path)
  def getObject: JsonObject = result.contentAs[JsonObject](path)
  def getAs[T]: T = result.contentAs[T](path)
}

class Document(val id: String,
               val cas: Long,
               private val _content: Array[Byte],
               val expiry: Option[Duration]) extends Dynamic with Convertable {

  def content: JsonObject = contentAs[JsonObject]

  def content(path: String): JsonObject = contentAs[JsonObject](path)

  def contentAs[T]: T = ???

  def contentAs[T](path: String): T = ???

  def contentAs[T](path: String, decoder: Array[Byte] => T): T = ???

  def selectDynamic(name: String): GetSelecter = GetSelecter(this, PathElements(List(PathObjectOrField(name))))
  def applyDynamic(name: String)(index: Int): GetSelecter = GetSelecter(this, PathElements(List(PathArray(name, index))))

  override def exists(path: PathElements): Boolean = ???

  override def contentAs[T](path: PathElements): T = ???
}
