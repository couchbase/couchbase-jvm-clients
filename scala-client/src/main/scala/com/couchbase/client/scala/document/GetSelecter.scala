package com.couchbase.client.scala.document

import scala.language.dynamics

// TODO much more implementation required
case class GetSelecter(private val result: Convertable, path: PathElements) extends Dynamic {
  def selectDynamic(name: String): GetSelecter = GetSelecter(result, path.add(PathObjectOrField(name)))
  def applyDynamic(name: String)(index: Int): GetSelecter = GetSelecter(result, path.add(PathArray(name, index)))

  def exists: Boolean = result.exists(path)
  def getString: String = result.contentAs[String](path)
  // TODO see what Jackson transcoding produces in terms of ints, longs, floats, doubles
  def getInt: Int = result.contentAs[Int](path)
  def getAs[T]: T = result.contentAs[T](path)
}
