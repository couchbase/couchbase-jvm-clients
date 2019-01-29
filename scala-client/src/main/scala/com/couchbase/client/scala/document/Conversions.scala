package com.couchbase.client.scala.document

import com.couchbase.client.core.msg.kv.SubdocGetRequest.CommandType
import com.couchbase.client.core.msg.kv.SubdocGetResponse

import scala.concurrent.duration.Duration
import scala.util.{Failure, Try}

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

//        implicit object StringConvert extends Convertable[String] {
//          override def encode(content: String) = Try((content, JsonEncodeParams))
//
//          override def decode(bytes: Array[Byte], params: DecodeParams): Try[String] = Try((bytes, JsonDecodeParams))
//        }

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


  trait DecodableField[T] {
    def decode(in: SubdocGetResponse.ResponseValue, params: DecodeParams): Try[T] = {
      in.`type`() match {
        case CommandType.EXISTS => decodeExists(in, params)
        case _ => decodeGet(in, params)
      }
    }

    def decodeGet(in: SubdocGetResponse.ResponseValue, params: DecodeParams): Try[T]
    
    def decodeExists(in: SubdocGetResponse.ResponseValue, params: DecodeParams): Try[T] = {
      Failure(new IllegalStateException()) // TODO replace with proper exception
    }
  }



  object DecodableField {

        implicit object StringConvert extends DecodableField[String] {
          override def decodeGet(in: SubdocGetResponse.ResponseValue, params: DecodeParams) = {
            // Note this means we actually depend on ujson
            Try(upickle.default.read[String](in.value()))
          }
        }

    implicit object BooleanConvert extends DecodableField[Boolean] {
      override def decodeGet(in: SubdocGetResponse.ResponseValue, params: DecodeParams) = {
        Try(upickle.default.read[Boolean](in.value()))
      }

      override def decodeExists(in: SubdocGetResponse.ResponseValue, params: DecodeParams): Try[Boolean] = {
        Try(in.status().success())
      }
    }


    implicit object IntConvert extends DecodableField[Int] {
      override def decodeGet(in: SubdocGetResponse.ResponseValue, params: DecodeParams) = {
        Try(upickle.default.read[Int](in.value()))
      }
    }


    implicit object DoubleConvert extends DecodableField[Double] {
      override def decodeGet(in: SubdocGetResponse.ResponseValue, params: DecodeParams) = {
        Try(upickle.default.read[Double](in.value()))
      }
    }

    implicit object UsjonValueConvert extends DecodableField[ujson.Value] {
      override def decodeGet(in: SubdocGetResponse.ResponseValue, params: DecodeParams) = {
        Try(upickle.default.read[ujson.Value](in.value()))
      }
    }


    implicit object UsjonObjConvert extends DecodableField[ujson.Obj] {
      override def decodeGet(in: SubdocGetResponse.ResponseValue, params: DecodeParams) = {
        Try(upickle.default.read[ujson.Obj](in.value()))
      }
    }



  }
}
