package com.couchbase.client.scala.document

import java.nio.charset.Charset

import com.couchbase.client.core.error.DecodingFailedException
import com.couchbase.client.core.msg.kv.{SubdocCommandType, SubdocField, SubdocGetResponse}
import io.netty.util.CharsetUtil
import play.api.libs.json.JsValue

import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

trait CodecParams {
  val flags: Int
  def isPrivate = (flags & DocumentFlags.Private) != 0
  def isJson = (flags & DocumentFlags.Json) != 0
  def isBinary = (flags & DocumentFlags.Binary) != 0
  def isString = (flags & DocumentFlags.String) != 0

  override def toString = "CodecParams{" +
    "flags=" + flags +
  "private=" + isPrivate +
  "json=" + isJson +
  "binary=" + isBinary +
  "str=" + isString +
  "}"
}
case class EncodeParams(flags: Int) extends CodecParams
case class DecodeParams(flags: Int) extends CodecParams


object DocumentFlags {
  val Reserved = 0
  val Private = 1 << 24
  val Json = 2 << 24
  val Binary = 3 << 24

  // Non-JSON String, utf-8 encoded, no BOM: "hello world"
  val String = 4 << 24
}

object Conversions {
  val JsonEncodeParams = EncodeParams(DocumentFlags.Json)
  val StringEncodeParams = EncodeParams(DocumentFlags.String)
  val BinaryEncodeParams = EncodeParams(DocumentFlags.Binary)
  val JsonDecodeParams = DecodeParams(DocumentFlags.Json)

  def encode[T](in: T)(implicit ev: Encodable[T]): Try[(Array[Byte], EncodeParams)] = {
    ev.encode(in)
  }

  def decode[T](bytes: Array[Byte], params: DecodeParams = JsonDecodeParams)
               (implicit ev: Decodable[T]): Try[T] = {
    ev.decode(bytes, params)
  }

  trait Encodable[-T] {
    def encode(content: T): Try[(Array[Byte], EncodeParams)]
  }

  trait Decodable[T] {
    def decode(bytes: Array[Byte], params: DecodeParams): Try[T]
  }

  object Encodable {

    // This is the safe Bytes converter: it parses the input, and sets the flags to Json or String as appropriate.
    // TODO add higher-performance unsafe converters that trusts the app
    implicit object BytesConvert extends Encodable[Array[Byte]] {
      override def encode(content: Array[Byte]) = {
        val upickleAttempt = Try(upickle.default.read[ujson.Value](content))

        upickleAttempt match {
          case Success(json) =>
            // TODO can probably get upickle to encode directly to Array[Byte]
            Try((content, JsonEncodeParams))
          case Failure(_) =>
            Try((content, BinaryEncodeParams))
        }
      }
  }

    // This is the safe String converter: it parses the input, and sets the flags to Json or String as appropriate.
    // TODO add higher-performance unsafe converters that trusts the app
    implicit object StringConvert extends Encodable[String] {
      override def encode(content: String) = {
        val upickleAttempt = Try(upickle.default.read[ujson.Value](content))

        upickleAttempt match {
          case Success(json) =>
            // TODO can probably get upickle to encode directly to Array[Byte]
            Try((content.getBytes(CharsetUtil.UTF_8), JsonEncodeParams))
          case Failure(_) =>
            Try((content.getBytes(CharsetUtil.UTF_8), StringEncodeParams))
        }
      }
    }

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

    implicit object PlayEncode extends Encodable[play.api.libs.json.JsValue] {
      override def encode(content: play.api.libs.json.JsValue) = {
        Try(content.as[Array[Byte]]).map((_, JsonEncodeParams))
      }

    }

  }

  object Decodable {

    implicit object RawConvert extends Decodable[Array[Byte]] {
      override def decode(bytes: Array[Byte], params: DecodeParams): Try[Array[Byte]] = Try(bytes)
    }

    implicit object StringConvert extends Decodable[String] {
      override def decode(bytes: Array[Byte], params: DecodeParams) = {
        if (params.isJson || params.isString) {
          Try(new String(bytes, CharsetUtil.UTF_8))
        }
        else {
          Failure(new DecodingFailedException(s"Cannot decode data with flags ${params} as a String"))
        }
      }
    }

    implicit object UjsonValueConvert extends Decodable[ujson.Value] {
      override def decode(bytes: Array[Byte], params: DecodeParams) = {
        val out = Try(upickle.default.read[ujson.Value](bytes))
        out match {
          case Success(_) => out
          case Failure(err) => Failure(new DecodingFailedException(err))
        }
      }
    }

    implicit object UjsonObjConvert extends Decodable[ujson.Obj] {
      override def decode(bytes: Array[Byte], params: DecodeParams) = {
        val out = Try(upickle.default.read[ujson.Obj](bytes))
        out match {
          case Success(_) => out
          case Failure(err) => Failure(new DecodingFailedException(err))
        }
      }
    }

    implicit object UjsonArrConvert extends Decodable[ujson.Arr] {
      override def decode(bytes: Array[Byte], params: DecodeParams) = {
        val out = Try(upickle.default.read[ujson.Arr](bytes))
        out match {
          case Success(_) => out
          case Failure(err) => Failure(new DecodingFailedException(err))
        }
      }
    }

//    implicit object CirceConvert extends Decodable[io.circe.Json] {
//      override def decode(bytes: Array[Byte], params: DecodeParams) = {
//        val out = Try(io.circe.parser.decode[io.circe.Json](bytes))
//        out match {
//          case Success(_) => out
//          case Failure(err) => Failure(new DecodingFailedException(err))
//        }
//      }
//    }

  }



  trait EncodableField[-T] {
    def encode(content: T): Try[(Array[Byte], EncodeParams)]
  }

  object EncodableField {
    implicit object StringConvert extends EncodableField[String] {
      override def encode(content: String) = {
        Try((('"' + content + '"').getBytes(CharsetUtil.UTF_8), JsonEncodeParams))
      }
    }

//    implicit object BoolConvert extends EncodableField[Boolean] {
//      override def encode(content: Boolean) = {
//        Try((content..getBytes(CharsetUtil.UTF_8), JsonEncodeParams))
//      }
//    }
  }

  trait DecodableField[T] {
    def decode(in: SubdocField, params: DecodeParams): Try[T] = {
      in.`type`() match {
        case SubdocCommandType.EXISTS => decodeExists(in, params)
        case _ => decodeGet(in, params)
      }
    }

    def decodeGet(in: SubdocField, params: DecodeParams): Try[T]
    
    def decodeExists(in: SubdocField, params: DecodeParams): Try[T] = {
      Failure(new IllegalStateException()) // TODO replace with proper exception
    }
  }



  object DecodableField {

        implicit object StringConvert extends DecodableField[String] {
          override def decodeGet(in: SubdocField, params: DecodeParams) = {
            // Note this means we actually depend on ujson
            Try(upickle.default.read[String](in.value()))
          }
        }

    implicit object BooleanConvert extends DecodableField[Boolean] {
      override def decodeGet(in: SubdocField, params: DecodeParams) = {
        Try(upickle.default.read[Boolean](in.value()))
      }

      override def decodeExists(in: SubdocField, params: DecodeParams): Try[Boolean] = {
        Try(in.status().success())
      }
    }


    implicit object IntConvert extends DecodableField[Int] {
      override def decodeGet(in: SubdocField, params: DecodeParams) = {
        Try(upickle.default.read[Int](in.value()))
      }
    }


    implicit object DoubleConvert extends DecodableField[Double] {
      override def decodeGet(in: SubdocField, params: DecodeParams) = {
        Try(upickle.default.read[Double](in.value()))
      }
    }

    implicit object UsjonValueConvert extends DecodableField[ujson.Value] {
      override def decodeGet(in: SubdocField, params: DecodeParams) = {
        Try(upickle.default.read[ujson.Value](in.value()))
      }
    }


    implicit object UsjonObjConvert extends DecodableField[ujson.Obj] {
      override def decodeGet(in: SubdocField, params: DecodeParams) = {
        Try(upickle.default.read[ujson.Obj](in.value()))
      }
    }



  }
}
