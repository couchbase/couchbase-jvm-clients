package com.couchbase.client.scala.kv

import com.couchbase.client.scala.codec.Conversions
import com.couchbase.client.scala.json._
import com.couchbase.client.core.deps.io.netty.util.CharsetUtil

import scala.annotation.tailrec
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

private[scala] object ProjectionsApplier {
  // Down the line, could support generating any type of Json - ujson, Json4s, etc.  Will need to take an implicit
  // conversion and have that conversion object know how to create and add to arrays and objects in the target AST.
  // For now, implement support for JsonObject only.  E.g. only contentAs[JsonObject] will be supported if projections
  // are used.
  def parseContent[T](content: Array[Byte]): Try[Any] = {
    val first = content(0)
    first match {
      case '{' => Conversions.decode[JsonObject](content)
      case '[' => Conversions.decode[JsonArray](content)
      case '"' =>
        val str = new String(content, CharsetUtil.UTF_8)
        Success(str.substring(1, str.size - 1))
      case 't' => Success(true)
      case 'f' => Success(false)
      case 'n' => Success(null)
      case '0' | '1' | '2' | '3' | '4' | '5' | '6' | '7' | '8' | '9' =>
        val str = new String(content, CharsetUtil.UTF_8)
        val out = try { if (str.contains('.')) str.toDouble else str.toLong }
        catch {
          // Try it as a number and fallback to a string
          case NonFatal(_) => str
        }
        Success(out)
      case _ =>
        Try(new String(content, CharsetUtil.UTF_8))
      //        Failure(new IllegalStateException(s"Could not parse content '${new String(content, CharsetUtil.UTF_8)}'"))
    }
  }

  def parse(in: JsonObject, path: String, content: Array[Byte]): Try[JsonObject] = {
    (for {
      parsed <- JsonPathParser.parse(path)
      content <- parseContent(content)
    } yield (parsed, content)) match {
      case Success((parsedPath, parsedContent)) =>
        parseRec(Left(in), parsedPath.toList, parsedContent).map(_ => in)
      case Failure(exception) => Failure(exception)
    }
  }

  def parse(path: String, content: Array[Byte]): Try[JsonObject] = {
    val out = JsonObject.create
    parse(out, path, content)
  }

  // Will follow `path`, constructing JSON as it does, and inserting `content` at the leaf
  @tailrec
  private def parseRec(in: Either[JsonObject, JsonArray], path: List[PathElement], content: Any): Try[Unit] = {
    path match {
      case Nil => Success()

      case x :: Nil =>
        x match {
          case PathArray(str, idx) =>
            val toInsert = JsonArray.create.add(content)
            in match {
              case Left(obj) => Success(obj.put(str, toInsert))
              case Right(arr) => Success(arr.add(toInsert))
            }
          case PathObjectOrField(str) =>
            in match {
              case Left(obj) => Success(obj.put(str, content))
              case Right(arr) => {
                val toInsert = JsonObject.create
                toInsert.put(str, content)
                Success(arr.add(toInsert))
              }
            }
        }

      case x :: xs =>
        x match {
          case PathArray(str, idx) =>
            val toInsert = JsonArray.create
            in match {
              case Left(obj) =>
                obj.put(str, toInsert)
                parseRec(Right(toInsert), xs, content)
              case Right(arr) =>
                arr.add(toInsert)
                parseRec(Right(toInsert), xs, content)
            }
          case PathObjectOrField(str) =>
            val toCreate = JsonObject.create
            in match {
              case Left(obj) =>
                obj.put(str, toCreate)
                parseRec(Left(toCreate), xs, content)
              case Right(arr) =>
                arr.add(toCreate)
                parseRec(Left(toCreate), xs, content)
            }
        }
    }
  }

}