package com.couchbase.client.scala.codec

import com.couchbase.client.core.error.DecodingFailedException
import com.couchbase.client.core.msg.kv.{SubdocCommandType, SubdocField}
import com.couchbase.client.scala.document.{DecodeParams, DocumentFlags, EncodeParams}
import com.couchbase.client.scala.json.JsonObject
import io.netty.util.CharsetUtil

import scala.util.{Failure, Success, Try}

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

    // TODO unify all names here

    // This is the safe Bytes converter: it parses the input, and sets the flags to Json or String as appropriate.
    implicit object BytesConvert extends Encodable[Array[Byte]] {
      override def encode(content: Array[Byte]) = {
        val upickleAttempt = Try(upickle.default.read[ujson.Value](content))

        upickleAttempt match {
          case Success(json) => Try((content, JsonEncodeParams))
          case Failure(_) => Try((content, BinaryEncodeParams))
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
            Try((content.getBytes(CharsetUtil.UTF_8), JsonEncodeParams))
          case Failure(_) =>
            Try(((content).getBytes(CharsetUtil.UTF_8), StringEncodeParams))
        }
      }
    }

    // These encoders treat the provided values as encoded Json
    object AsJson {
      // This is a high-performance Array[Bytes] converter that trusts that the input is indeed Json and sets the appropriate flag
      implicit object BytesConvert extends Encodable[Array[Byte]] {
        override def encode(content: Array[Byte]) = {
              Try((content, JsonEncodeParams))
        }
      }

      // This is a high-performance String converter that trusts that the input is indeed Json and sets the appropriate flag
      implicit object StringConvert extends Encodable[String] {
        override def encode(content: String) = {
              Try((content.getBytes(CharsetUtil.UTF_8), JsonEncodeParams))
        }
      }
    }

    // These encoders treat the provided values as raw binary or strings
    object AsValue {
      // This is a high-performance Array[Byte] converter that trusts that the input is a binary blob and sets the appropriate flag
      implicit object BytesConvert extends Encodable[Array[Byte]] {
        override def encode(content: Array[Byte]) = {
            Try((content, BinaryEncodeParams))
        }
      }

      // This is a high-performance String converter that trusts that the input is a regular non-Json String and sets the appropriate flag
      implicit object StringConvert extends Encodable[String] {
        override def encode(content: String) = {
              Try(((content).getBytes(CharsetUtil.UTF_8), StringEncodeParams))
        }
      }
    }

    implicit object BooleanConvert extends Encodable[Boolean] {
      override def encode(content: Boolean) = {
        val str = if (content) "true" else "false"
        Try((str.getBytes(CharsetUtil.UTF_8), JsonEncodeParams))
      }
    }

    implicit object IntConvert extends Encodable[Int] {
      override def encode(content: Int) = {
        Try((content.toString.getBytes(CharsetUtil.UTF_8), JsonEncodeParams))
      }
    }

    implicit object DoubleConvert extends Encodable[Double] {
      override def encode(content: Double) = {
        Try((content.toString.getBytes(CharsetUtil.UTF_8), JsonEncodeParams))
      }
    }

    implicit object LongConvert extends Encodable[Long] {
      override def encode(content: Long) = {
        Try((content.toString.getBytes(CharsetUtil.UTF_8), JsonEncodeParams))
      }
    }

    implicit object ShortConvert extends Encodable[Short] {
      override def encode(content: Short) = {
        Try((content.toString.getBytes(CharsetUtil.UTF_8), JsonEncodeParams))
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
        Try(play.api.libs.json.Json.stringify(content).getBytes(CharsetUtil.UTF_8)).map((_, JsonEncodeParams))
      }
    }


    implicit object Json4sEncode extends Encodable[org.json4s.JsonAST.JValue] {
      override def encode(content: org.json4s.JsonAST.JValue) = {
        Try(org.json4s.jackson.JsonMethods.compact(content).getBytes(CharsetUtil.UTF_8)).map((_, JsonEncodeParams))
      }
    }


    implicit object JawnConvert extends Encodable[org.typelevel.jawn.ast.JValue] {
      override def encode(content: org.typelevel.jawn.ast.JValue) = {
        Try(content.render().getBytes(CharsetUtil.UTF_8)).map((_, JsonEncodeParams))
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
      override def decode(bytes: Array[Byte], params: DecodeParams) = {
        tryDecode(play.api.libs.json.Json.parse(bytes))
      }
    }

    implicit object Play4sConvert extends Decodable[org.json4s.JsonAST.JValue] {
      override def decode(bytes: Array[Byte], params: DecodeParams) = {
        tryDecode(org.json4s.native.JsonMethods.parse(new String(bytes, CharsetUtil.UTF_8)))
      }
    }

    implicit object JawnConvert extends Decodable[org.typelevel.jawn.ast.JValue] {
      override def decode(bytes: Array[Byte], params: DecodeParams) = {
        org.typelevel.jawn.Parser.parseFromString[org.typelevel.jawn.ast.JValue](new String(bytes, CharsetUtil.UTF_8))
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
      Failure(new DecodingFailedException("Field cannot be checked with exists"))
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
