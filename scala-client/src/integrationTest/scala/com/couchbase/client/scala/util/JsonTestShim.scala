package com.couchbase.client.scala.util

import com.couchbase.client.scala.json.{JsonObject, JsonArray}

/**
  * Lightweight compatibility layer that lets existing integration–test code keep using the familiar
  * `ujson` syntax even though the real ujson library is no longer on the class-path in the Scala 3
  * build.
  *
  * • `ujson.Obj(...)` → JsonObject
  * • `ujson.Arr(...)` → JsonArray
  *
  * It also supplies a handful of extension helpers so that convenient field access like
  * `json("foo").str`, `json("foo").arr.map(_.str)` etc. continue to work.
  */
object ujson {
  type Obj   = JsonObject
  type Value = JsonObject

  def Obj(entries: (String, Any)*): JsonObject = JsonObject(entries: _*)

  object Arr {
    def apply(values: Any*): JsonArray = JsonArray(values: _*)
    def unapplySeq(arr: JsonArray): Option[Seq[Any]] = Some(arr.toSeq)
  }
}

object TestJsonImplicits {
  import scala.language.implicitConversions
  import com.couchbase.client.scala.json.{JsonObject, JsonArray}

  implicit class RichJsonObject(val jo: JsonObject) extends AnyVal {
    def apply(name: String): RichField = new RichField(jo, name)
  }

  final class RichField(private val jo: JsonObject, private val name: String) {
    def str: String        = try { jo.str(name) } catch { case _: Exception => throw new NoSuchElementException(s"Field '$name' not found") }
    def num: Int           = try { jo.num(name) } catch { case _: Exception => throw new NoSuchElementException(s"Field '$name' not found") }
    def numLong: Long      = try { jo.numLong(name) } catch { case _: Exception => throw new NoSuchElementException(s"Field '$name' not found") }
    def numDouble: Double  = try { jo.numDouble(name) } catch { case _: Exception => throw new NoSuchElementException(s"Field '$name' not found") }
    def bool: Boolean      = try { jo.bool(name) } catch { case _: Exception => throw new NoSuchElementException(s"Field '$name' not found") }
    def obj: JsonObject    = try { jo.obj(name) } catch { case _: Exception => throw new NoSuchElementException(s"Field '$name' not found") }
    def arr: JsonArray     = try { jo.arr(name) } catch { case _: Exception => throw new NoSuchElementException(s"Field '$name' not found") }
  }

  implicit class RichJsonArray(val ja: JsonArray) extends AnyVal {
    def map[B](f: Any => B): Seq[B] = ja.toSeq.map(f)
  }

  implicit class RichAny(val v: Any) extends AnyVal {
    def str: String = v.toString
  }
} 
