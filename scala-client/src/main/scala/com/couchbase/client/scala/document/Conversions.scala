package com.couchbase.client.scala.document

import scala.concurrent.duration.Duration
import scala.util.Try

case class EncodeParams(flags: Int)
case class DecodeParams(flags: Int)


object DocumentFlags {
  val Reserved = 0
  // TODO
  val Private = 1 << 24
  val Json = 2 << 24
  val Binary = 3 << 24
  val String = 4 << 24
}

object Conversions {
  val JsonEncodeParams = EncodeParams(DocumentFlags.Json) // TODO
  val JsonDecodeParams = DecodeParams(DocumentFlags.Json) // TODO

  trait Encodable[-T] {
    def encode(content: T): Try[(Array[Byte], EncodeParams)]
  }

  trait Decodable[T] {
    def decode(bytes: Array[Byte], params: DecodeParams): Try[T]
  }

  object Encodable {

    implicit object RawConvert extends Encodable[Array[Byte]] {
      override def encode(content: Array[Byte]) = Try((content, JsonEncodeParams))
  }

//    implicit object StringConvert extends Convertable[String] {
//      override def encode(content: String) = Try((content, JsonEncodeParams))
//
//      override def decode(bytes: Array[Byte], params: DecodeParams): Try[String] = Try((bytes, JsonDecodeParams))
//    }

    implicit object UjsonConvert extends Encodable[ujson.Value] {
      override def encode(content: ujson.Value) = {
        Try((ujson.transform(content, ujson.BytesRenderer()).toBytes, JsonEncodeParams))
      }
    }

    implicit object CirceEncode extends Encodable[io.circe.Json] {
      override def encode(content: io.circe.Json) = {
        content.as[Array[Byte]].toTry.map((_, JsonEncodeParams))
      }
    }


  }

  object Decodable {

    implicit object RawConvert extends Decodable[Array[Byte]] {
      override def decode(bytes: Array[Byte], params: DecodeParams): Try[Array[Byte]] = Try(bytes)
    }

    //    implicit object StringConvert extends Convertable[String] {
    //      override def encode(content: String) = Try((content, JsonEncodeParams))
    //
    //      override def decode(bytes: Array[Byte], params: DecodeParams): Try[String] = Try((bytes, JsonDecodeParams))
    //    }

    implicit object UjsonValueConvert extends Decodable[ujson.Value] {
      override def decode(bytes: Array[Byte], params: DecodeParams) = {
        Try(upickle.default.read[ujson.Value](bytes))
      }
    }

    implicit object UjsonObjConvert extends Decodable[ujson.Obj] {
      override def decode(bytes: Array[Byte], params: DecodeParams) = {
        Try(upickle.default.read[ujson.Obj](bytes))
      }
    }

    implicit object UjsonArrConvert extends Decodable[ujson.Arr] {
      override def decode(bytes: Array[Byte], params: DecodeParams) = {
        Try(upickle.default.read[ujson.Arr](bytes))
      }
    }

  }
}
