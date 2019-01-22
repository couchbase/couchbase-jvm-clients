package com.couchbase.client.scala.document

import scala.util.Try

object Conversions {
  trait Convertable[T] {
    def encode(content: T): Try[Array[Byte]]

    def decode(bytes: Array[Byte]): Try[T]
  }

  object Convertable {

    implicit object RawConvert extends Convertable[Array[Byte]] {
      override def encode(content: Array[Byte]): Try[Array[Byte]] = Try(content)

      override def decode(bytes: Array[Byte]): Try[Array[Byte]] = Try(bytes)
    }

    implicit object UjsonConvert extends Convertable[ujson.Obj] {
      override def encode(content: ujson.Obj): Try[Array[Byte]] = {
        Try(ujson.transform(content, ujson.BytesRenderer()).toBytes)
      }

      override def decode(bytes: Array[Byte]): Try[ujson.Obj] = {
        Try(upickle.default.read[ujson.Obj](bytes))
      }
    }
  }
}
