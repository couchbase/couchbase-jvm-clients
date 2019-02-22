package com.couchbase.client.scala.json

import com.couchbase.client.core.error.DecodingFailedException

import scala.language.dynamics
import scala.util.{Failure, Success, Try}





object GetSelecter {
  private def couldNotFindKey(name: String) = new DecodingFailedException(s"Could not find key $name")
  private def expectedObjectButFoundArray(name: String) = new DecodingFailedException(s"Expected object or field for '$name' but found an array")
  private def expectedArrayButFoundObject(name: String) = new DecodingFailedException(s"Expected array for '$name' but found an object")

  def eval(in: Either[JsonObjectSafe, JsonArraySafe], path: Seq[PathElement]): Try[Any] = {
    path match {
      // x is what we're looking for next, in is what out cursor's on

      case x :: Nil =>
        x match {
          case PathObjectOrField(name) =>
            in match {
              case Left(obj) =>
                obj.get(name) match {
                  case Success(o) => Success(o)
                  case _ =>
                    Failure(couldNotFindKey(name))
                }

              case Right(arr) =>
                Failure(expectedObjectButFoundArray(name))
            }

          case PathArray(name, idx) =>
            in match {
              case Left(obj) =>
                Failure(expectedArrayButFoundObject(name))
              case Right(arr) =>
                Success(arr.get(idx))
            }
        }

      case x :: xs =>
        x match {
          case PathObjectOrField(name) =>
            in match {
              case Left(obj) =>
                eval(Left(obj), xs)
              case Right(arr) =>
                Failure(expectedObjectButFoundArray(name))
            }

          case PathArray(name, idx) =>
            in match {
              case Left(obj) =>
                obj.arr(name) match {
                  case Success(arr) =>
                    val atIdx: Try[Any] = arr.get(idx)

                    atIdx match {
                      case Success(o: JsonObjectSafe) => eval(Left(o), xs)
                      case Success(a: JsonArraySafe) => eval(Right(a), xs)
                      case Success(v: Any) => Failure(new DecodingFailedException(s"Needed object or array at $name[$idx], but found '${v}'"))
                      case _ => Failure(new DecodingFailedException(s"Found array $name but nothing at index $idx"))
                    }

                  case _ => Failure(new DecodingFailedException(s"Could not find array '$name'"))
                }
              case Right(arr) =>
                eval(Right(arr), xs)
            }
        }
    }
  }
}

case class GetSelecter(private val in: Either[JsonObject, JsonArray],
                       private val path: Seq[PathElement]) extends Dynamic {
  private val mapped = in.left.map(_.safe).right.map(_.safe)

  def selectDynamic(name: String): GetSelecter = GetSelecter(in, path :+ PathObjectOrField(name))

  def applyDynamic(name: String)(index: Int): GetSelecter = GetSelecter(in, path :+ PathArray(name, index))

  private def pathStr = path.toString()

  def str: String = GetSelecter.eval(mapped, path).map(v => ValueConvertor.str(v, pathStr)).get

  def num: Int = GetSelecter.eval(mapped, path).map(v => ValueConvertor.num(v, pathStr)).get

  def numDouble: Double = GetSelecter.eval(mapped, path).map(v => ValueConvertor.numDouble(v, pathStr)).get

  def numFloat: Float = GetSelecter.eval(mapped, path).map(v => ValueConvertor.numFloat(v, pathStr)).get

  def numLong: Long = GetSelecter.eval(mapped, path).map(v => ValueConvertor.numLong(v, pathStr)).get

  def bool: Boolean = GetSelecter.eval(mapped, path).map(v => ValueConvertor.bool(v, pathStr)).get

  def obj: JsonObject = GetSelecter.eval(mapped, path).map(v => ValueConvertor.obj(v, pathStr)).get

  def arr: JsonArray = GetSelecter.eval(mapped, path).map(v => ValueConvertor.arr(v, pathStr)).get

}
