package com.couchbase.client.scala.codec

import com.couchbase.client.core.error.DecodingFailedException
import com.couchbase.client.core.msg.kv.{SubdocCommandType, SubdocField}
import io.netty.util.CharsetUtil

import scala.util.{Failure, Success, Try}


trait CodecParams {
  val flags: Int

  def isPrivate = (flags & DocumentFlags.Private) != 0

  def isJson = (flags & DocumentFlags.Json) != 0

  def isBinary = (flags & DocumentFlags.Binary) != 0

  def isString = (flags & DocumentFlags.String) != 0

  override def toString = "CodecParams{" +
    "flags=" + flags +
    ",private=" + isPrivate +
    ",json=" + isJson +
    ",binary=" + isBinary +
    ",str=" + isString +
    "}"
}

case class EncodeParams(flags: Int) extends CodecParams

//case class EncodeParams(flags: Int) extends CodecParams


object DocumentFlags {
  val Reserved = 0
  val Private = 1 << 24
  val Json = 2 << 24
  val Binary = 3 << 24

  // Non-JSON String, utf-8 encoded, no BOM: "hello world"
  val String = 4 << 24
}



object Conversions {
  val JsonFlags = EncodeParams(DocumentFlags.Json)
  val StringFlags = EncodeParams(DocumentFlags.String)
  val BinaryFlags = EncodeParams(DocumentFlags.Binary)

  def encode[T](in: T)(implicit ev: Encodable[T]): Try[(Array[Byte], EncodeParams)] = {
    ev.encode(in)
  }

  def decode[T](bytes: Array[Byte], params: EncodeParams = JsonFlags)
               (implicit ev: Decodable[T]): Try[T] = {
    ev.decode(bytes, params)
  }

  trait Encodable[-T] {
    def encode(content: T): Try[(Array[Byte], EncodeParams)]
  }

  trait Decodable[T] {
    def decode(bytes: Array[Byte], params: EncodeParams): Try[T]
  }

  trait Codec[T] extends Encodable[T] with Decodable[T] {
  }

  object Encodable {

    // TODO unify all names here

    // This is the safe Bytes converter: it parses the input, and sets the flags to Json or String as appropriate.
    implicit object BytesConvert extends Encodable[Array[Byte]] {
      override def encode(content: Array[Byte]) = {
        val upickleAttempt = Try(upickle.default.read[ujson.Value](content))

        upickleAttempt match {
          case Success(json) => Try((content, JsonFlags))
          case Failure(_) => Try((content, BinaryFlags))
        }
      }
    }

    // This is the safe String converter: it parses the input, and sets the flags to Json or String as appropriate.
    implicit object StringConvert extends Encodable[String] {
      override def encode(content: String) = {
        val upickleAttempt = Try(upickle.default.read[ujson.Value](content))

        upickleAttempt match {
          case Success(json) =>
            // TODO can probably get upickle to encode directly to Array[Byte]
            Try((content.getBytes(CharsetUtil.UTF_8), JsonFlags))
          case Failure(_) =>
            Try(((content).getBytes(CharsetUtil.UTF_8), StringFlags))
        }
      }
    }

    // These encoders treat the provided values as encoded Json
    object AsJson {
      // This is a high-performance Array[Bytes] converter that trusts that the input is indeed Json and sets the appropriate flag
      implicit object BytesConvert extends Encodable[Array[Byte]] {
        override def encode(content: Array[Byte]) = {
              Try((content, JsonFlags))
        }
      }

      // This is a high-performance String converter that trusts that the input is indeed Json and sets the appropriate flag
      implicit object StringConvert extends Encodable[String] {
        override def encode(content: String) = {
              Try((content.getBytes(CharsetUtil.UTF_8), JsonFlags))
        }
      }
    }

    // These encoders treat the provided values as raw binary or strings
    object AsValue {
      // This is a high-performance Array[Byte] converter that trusts that the input is a binary blob and sets the appropriate flag
      implicit object BytesConvert extends Encodable[Array[Byte]] {
        override def encode(content: Array[Byte]) = {
            Try((content, BinaryFlags))
        }
      }

      // This is a high-performance String converter that trusts that the input is a regular non-Json String and sets the appropriate flag
      implicit object StringConvert extends Encodable[String] {
        override def encode(content: String) = {
              Try(((content).getBytes(CharsetUtil.UTF_8), StringFlags))
        }
      }
    }

    implicit object BooleanConvert extends Encodable[Boolean] {
      override def encode(content: Boolean) = {
        val str = if (content) "true" else "false"
        Try((str.getBytes(CharsetUtil.UTF_8), JsonFlags))
      }
    }

    implicit object IntConvert extends Encodable[Int] {
      override def encode(content: Int) = {
        Try((content.toString.getBytes(CharsetUtil.UTF_8), JsonFlags))
      }
    }

    implicit object DoubleConvert extends Encodable[Double] {
      override def encode(content: Double) = {
        Try((content.toString.getBytes(CharsetUtil.UTF_8), JsonFlags))
      }
    }

    implicit object LongConvert extends Encodable[Long] {
      override def encode(content: Long) = {
        Try((content.toString.getBytes(CharsetUtil.UTF_8), JsonFlags))
      }
    }

    implicit object ShortConvert extends Encodable[Short] {
      override def encode(content: Short) = {
        Try((content.toString.getBytes(CharsetUtil.UTF_8), JsonFlags))
      }
    }

    implicit object UjsonConvert extends Encodable[ujson.Value] {
      override def encode(content: ujson.Value) = {
        Try((ujson.transform(content, ujson.BytesRenderer()).toBytes, JsonFlags))
      }
    }

    implicit object CirceEncode extends Encodable[io.circe.Json] {
      override def encode(content: io.circe.Json) = {
        content.as[Array[Byte]].toTry.map((_, JsonFlags))
      }
    }

    implicit object PlayEncode extends Encodable[play.api.libs.json.JsValue] {
      override def encode(content: play.api.libs.json.JsValue) = {
        Try(play.api.libs.json.Json.stringify(content).getBytes(CharsetUtil.UTF_8)).map((_, JsonFlags))
      }
    }


    implicit object Json4sEncode extends Encodable[org.json4s.JsonAST.JValue] {
      override def encode(content: org.json4s.JsonAST.JValue) = {
        Try(org.json4s.jackson.JsonMethods.compact(content).getBytes(CharsetUtil.UTF_8)).map((_, JsonFlags))
      }
    }


    implicit object JawnConvert extends Encodable[org.typelevel.jawn.ast.JValue] {
      override def encode(content: org.typelevel.jawn.ast.JValue) = {
        Try(content.render().getBytes(CharsetUtil.UTF_8)).map((_, JsonFlags))
      }
    }

  }

  object Decodable {

    implicit object RawConvert extends Decodable[Array[Byte]] {
      override def decode(bytes: Array[Byte], params: EncodeParams): Try[Array[Byte]] = Try(bytes)
    }

    implicit object StringConvert extends Decodable[String] {
      override def decode(bytes: Array[Byte], params: EncodeParams) = {
        if (params.isJson || params.isString) {
          Try(new String(bytes, CharsetUtil.UTF_8))
        }
        else {
          Failure(new DecodingFailedException(s"Cannot decode data with flags ${params} as a String"))
        }
      }
    }

    implicit object UjsonValueConvert extends Decodable[ujson.Value] {
      override def decode(bytes: Array[Byte], params: EncodeParams) = {
        val out = Try(upickle.default.read[ujson.Value](bytes))
        out match {
          case Success(_) => out
          case Failure(err) => Failure(new DecodingFailedException(err))
        }
      }
    }

    implicit object UjsonObjConvert extends Decodable[ujson.Obj] {
      override def decode(bytes: Array[Byte], params: EncodeParams) = {
        val out = Try(upickle.default.read[ujson.Obj](bytes))
        out match {
          case Success(_) => out
          case Failure(err) => Failure(new DecodingFailedException(err))
        }
      }
    }

    implicit object UjsonArrConvert extends Decodable[ujson.Arr] {
      override def decode(bytes: Array[Byte], params: EncodeParams) = {
        tryDecode(upickle.default.read[ujson.Arr](bytes))
      }
    }

    def tryDecode[T](in: => T) = {
      val out = Try(in)
      out match {
        case Success(_) => out
        case Failure(err) => Failure(new DecodingFailedException(err))
      }
    }

    implicit object PlayConvert extends Decodable[play.api.libs.json.JsValue] {
      override def decode(bytes: Array[Byte], params: EncodeParams) = {
        tryDecode(play.api.libs.json.Json.parse(bytes))
      }
    }

    implicit object Play4sConvert extends Decodable[org.json4s.JsonAST.JValue] {
      override def decode(bytes: Array[Byte], params: EncodeParams) = {
        tryDecode(org.json4s.native.JsonMethods.parse(new String(bytes, CharsetUtil.UTF_8)))
      }
    }

    implicit object JawnConvert extends Decodable[org.typelevel.jawn.ast.JValue] {
      override def decode(bytes: Array[Byte], params: EncodeParams) = {
        org.typelevel.jawn.Parser.parseFromString[org.typelevel.jawn.ast.JValue](new String(bytes, CharsetUtil.UTF_8))
      }
    }


    //    implicit object CirceConvert extends Decodable[io.circe.Json] {
    //      override def decode(bytes: Array[Byte], params: EncodeParams) = {
    //        val out = Try(io.circe.parser.decode[io.circe.Json](bytes))
    //        out match {
    //          case Success(_) => out
    //          case Failure(err) => Failure(new DecodingFailedException(err))
    //        }
    //      }
    //    }

  }


  /*
    * Do we need EncodableField vs Encodable?
    * - Encoding String with "" - needed?
    * Could handle with a param instead
    *
    * Do we need DecodableField vs Decoable?
    * - Lets us handle exists separately
    */

  trait EncodableField[-T] extends Encodable[T] {
    def encode(content: T): Try[(Array[Byte], EncodeParams)]
  }

  object EncodableField {

    implicit object StringConvert extends EncodableField[String] {
      override def encode(content: String) = {
        Try((('"' + content + '"').getBytes(CharsetUtil.UTF_8), JsonFlags))
      }
    }

    //    implicit object BoolConvert extends EncodableField[Boolean] {
    //      override def encode(content: Boolean) = {
    //        Try((content..getBytes(CharsetUtil.UTF_8), JsonEncodeParams))
    //      }
    //    }
  }

  trait DecodableField[T] {
    def decode(in: SubdocField, params: EncodeParams): Try[T] = {
      in.`type`() match {
        case SubdocCommandType.EXISTS => decodeExists(in, params)
        case _ => decodeGet(in, params)
      }
    }

    def decodeGet(in: SubdocField, params: EncodeParams): Try[T]

    def decodeExists(in: SubdocField, params: EncodeParams): Try[T] = {
      Failure(new DecodingFailedException("Field cannot be checked with exists"))
    }
  }


  object DecodableField {

    implicit object StringConvert extends DecodableField[String] {
      override def decodeGet(in: SubdocField, params: EncodeParams) = {
        // Note this means we actually depend on ujson
        Try(upickle.default.read[String](in.value()))
      }
    }

    implicit object BooleanConvert extends DecodableField[Boolean] {
      override def decodeGet(in: SubdocField, params: EncodeParams) = {
        Try(upickle.default.read[Boolean](in.value()))
      }

      override def decodeExists(in: SubdocField, params: EncodeParams): Try[Boolean] = {
        Try(in.status().success())
      }
    }


    implicit object IntConvert extends DecodableField[Int] {
      override def decodeGet(in: SubdocField, params: EncodeParams) = {
        Try(upickle.default.read[Int](in.value()))
      }
    }


    implicit object DoubleConvert extends DecodableField[Double] {
      override def decodeGet(in: SubdocField, params: EncodeParams) = {
        Try(upickle.default.read[Double](in.value()))
      }
    }

    implicit object UsjonValueConvert extends DecodableField[ujson.Value] {
      override def decodeGet(in: SubdocField, params: EncodeParams) = {
        Try(upickle.default.read[ujson.Value](in.value()))
      }
    }


    implicit object UsjonObjConvert extends DecodableField[ujson.Obj] {
      override def decodeGet(in: SubdocField, params: EncodeParams) = {
        Try(upickle.default.read[ujson.Obj](in.value()))
      }
    }


  }

}
