package com.couchbase.client.scala.json

import com.couchbase.client.core.error.DecodingFailedException

import scala.language.dynamics
import scala.util.{Failure, Success, Try}

/** A 'safe' version of [[GetSelecter]] whose methods return `Try` rather than throw exceptions.
  *
  * In all other respects it is identical to `GetSelecter`.
  */
// TODO ScalaDocs
case class GetSelecterSafe(private val in: Either[JsonObjectSafe, JsonArraySafe],
                       private val path: Seq[PathElement]) extends Dynamic {
  def selectDynamic(name: String): GetSelecterSafe = GetSelecterSafe(in, path :+ PathObjectOrField(name))

  def applyDynamic(name: String)(index: Int): GetSelecterSafe = GetSelecterSafe(in, path :+ PathArray(name, index))

  private def pathStr = path.toString()

  def str: Try[String] = GetSelecter.eval(in, path).flatMap(v => Try(ValueConvertor.str(v, pathStr)))

  def num: Try[Int] = GetSelecter.eval(in, path).flatMap(v => Try(ValueConvertor.num(v, pathStr)))

  def numDouble: Try[Double] = GetSelecter.eval(in, path).flatMap(v => Try(ValueConvertor.numDouble(v, pathStr)))

  def numFloat: Try[Float] = GetSelecter.eval(in, path).flatMap(v => Try(ValueConvertor.numFloat(v, pathStr)))

  def bool: Try[Boolean] = GetSelecter.eval(in, path).flatMap(v => Try(ValueConvertor.bool(v, pathStr)))

  def obj: Try[JsonObjectSafe] = GetSelecter.eval(in, path).flatMap(v => Try(JsonObjectSafe(ValueConvertor.obj(v, pathStr))))

  def arr: Try[JsonArraySafe] = GetSelecter.eval(in, path).flatMap(v => Try(JsonArraySafe(ValueConvertor.arr(v, pathStr))))
}
