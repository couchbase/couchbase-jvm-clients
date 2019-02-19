package com.couchbase.client.scala.json

import com.couchbase.client.core.error.DecodingFailedException

import scala.language.dynamics
import scala.util.{Failure, Success, Try}

sealed trait PathElements

case class PathObjectOrField(str: String) extends PathElements

case class PathArray(str: String, idx: Int) extends PathElements


object GetSelecter {
  def eval(in: Either[JsonObject, JsonArray], path: Seq[PathElements]): Try[Any] = {
    path match {
      // x is what we're looking for next, in is what out cursor's on

      case x :: Nil =>
        x match {
          case PathObjectOrField(name) =>
            in match {
              case Left(obj) =>
                obj.getOpt(name) match {
                  case Some(o) => Success(o)
                  case _ =>
                    Failure(new DecodingFailedException(s"Could not find key $name"))
                }

              case Right(arr) =>
                Failure(new DecodingFailedException(s"Expected object or field for '$name' but found an array"))
            }

          case PathArray(name, idx) =>
            in match {
              case Left(obj) =>
                Failure(new DecodingFailedException(s"Expected array for '$name' but found an object"))
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
                Failure(new DecodingFailedException(s"Expected object or field for '$name' but found an array"))
            }

          case PathArray(name, idx) =>
            in match {
              case Left(obj) =>
                obj.arrTry(name) match {
                  case Success(arr) =>
                    val atIdx: Option[Any] = arr.getOpt(idx)

                    atIdx match {
                      case Some(o: JsonObject) => eval(Left(o), xs)
                      case Some(a: JsonArray) => eval(Right(a), xs)
                      case Some(v: Any) => Failure(new DecodingFailedException(s"Needed object or array at $name[$idx], but found '${v}'"))
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
                       private val path: Seq[PathElements]) extends Dynamic {
  def selectDynamic(name: String): GetSelecter = GetSelecter(in, path :+ PathObjectOrField(name))

  def applyDynamic(name: String)(index: Int): GetSelecter = GetSelecter(in, path :+ PathArray(name, index))

  def str: Try[String] = GetSelecter.eval(in, path).map(_.asInstanceOf[String])

  def int: Try[Int] = GetSelecter.eval(in, path).map(_.asInstanceOf[Int])

  def double: Try[Double] = GetSelecter.eval(in, path).map(_.asInstanceOf[Double])

  def float: Try[Float] = GetSelecter.eval(in, path).map(_.asInstanceOf[Float])

  def bool: Try[Boolean] = GetSelecter.eval(in, path).map(_.asInstanceOf[Boolean])

  def obj: Try[JsonObject] = GetSelecter.eval(in, path).map(_.asInstanceOf[JsonObject])

  def arr: Try[JsonArray] = GetSelecter.eval(in, path).map(_.asInstanceOf[JsonArray])

}
