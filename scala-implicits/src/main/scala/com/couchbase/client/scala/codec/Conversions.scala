package com.couchbase.client.scala.codec

import java.nio.ByteBuffer

import com.couchbase.client.core.error.DecodingFailedException
import com.couchbase.client.core.msg.kv.{CodecFlags, SubdocCommandType, SubdocField}
import com.couchbase.client.scala.json.{JacksonTransformers, JsonObject}
import com.couchbase.client.scala.kv.MutateInMacro
import io.netty.util.CharsetUtil

import scala.util.{Failure, Success, Try}


trait CodecParams {
  val flags: Int

  def isJson = (flags & DocumentFlags.Json) != 0

  def isBinary = (flags & DocumentFlags.Binary) != 0

  def isString = (flags & DocumentFlags.String) != 0

  override def toString = "CodecParams{" +
    "flags=" + flags +
    ",json=" + isJson +
    ",binary=" + isBinary +
    ",str=" + isString +
    "}"
}

/** Returned by [[Conversions.Encodable.encode]] to provide additional context about the encoding. */
case class EncodeParams(
                         // Each value is stored on Couchbase along with a flags field indicating the type of the content
                         // @see DocumentFlags
                         flags: Int
                       ) extends CodecParams


object DocumentFlags {
  val Json = CodecFlags.JSON_COMPAT_FLAGS
  val Binary = CodecFlags.BINARY_COMPAT_FLAGS

  // Non-JSON String, utf-8 encoded, no BOM: "hello world"
  // (note "hello world" is still valid Json)
  val String = CodecFlags.STRING_COMPAT_FLAGS
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

    def encodeSubDocumentField(content: T): Try[(Array[Byte], EncodeParams)] = encode(content)
  }

  trait Decodable[T] {
    def decode(bytes: Array[Byte], params: EncodeParams): Try[T]

    def decodeSubDocumentField(in: SubdocField, params: EncodeParams): Try[T] = {
      in.`type`() match {
        case SubdocCommandType.EXISTS => decodeSubDocumentExists(in, params)
        case _ => decodeSubDocumentGet(in, params)
      }
    }

    def decodeSubDocumentGet(in: SubdocField, params: EncodeParams): Try[T] = {
      if (in.error().isPresent) Failure(in.error().get())
      else decode(in.value(), params)
    }

    def decodeSubDocumentExists(in: SubdocField, params: EncodeParams): Try[T] = {
      Failure(new DecodingFailedException("Field cannot be checked with exists"))
    }

  }

  trait Codec[T] extends Encodable[T] with Decodable[T] {
  }

  object Encodable {

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

    // TODO support Serializable

    // Note there are 3 ways of representing strings:
    // {"hello":"world"}  written with flags=Json
    // foobar             written with flags=Json (note that a single string is perfectly valid json)
    // foobar             written with flags=String

    // This is the safe String converter: it parses the input, and sets the flags to Json or String as appropriate.
    implicit object StringConvert extends Encodable[String] {
      // TODO default StringConvert perhaps shouldn't be safe - talk with Matt
      override def encode(content: String) = {
        // TODO this requires upickle, exchange for jsoniter
        val upickleAttempt = Try(upickle.default.read[ujson.Value](content))

        upickleAttempt match {
          case Success(json) =>
            Try((content.getBytes(CharsetUtil.UTF_8), JsonFlags))
          case Failure(_) =>
            Try(((content).getBytes(CharsetUtil.UTF_8), StringFlags))
        }
      }

      override def encodeSubDocumentField(content: String): Try[(Array[Byte], EncodeParams)] = {
        Try((('"' + content + '"').getBytes(CharsetUtil.UTF_8), JsonFlags))
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

    implicit object JsonObjectConvert extends Encodable[JsonObject] {
      override def encode(content: JsonObject) = {
        Try(JacksonTransformers.MAPPER.writeValueAsBytes(content), JsonFlags)
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
        Try(content.noSpaces.getBytes(CharsetUtil.UTF_8)).map((_, JsonFlags))
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

    implicit object MutateInMacroConvert extends Encodable[MutateInMacro] {
      override def encode(content: MutateInMacro) = {
        Try((content.value.getBytes(CharsetUtil.UTF_8), Conversions.StringFlags))
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

      override def decodeSubDocumentGet(in: SubdocField, params: EncodeParams): Try[String] = {
        if (in.error().isPresent) Failure(in.error().get())
        else {
          Try({
            val str = new String(in.value(), CharsetUtil.UTF_8)
            // Strip off the " from start and end
            str.substring(1, str.size - 1)
          })
        }
      }
    }

    implicit object JsonObjectConvert extends Decodable[JsonObject] {
      override def decode(bytes: Array[Byte], params: EncodeParams) = {
        val out = Try(JacksonTransformers.MAPPER.readValue(bytes, classOf[JsonObject]))
        out match {
          case Success(_) => out
          case Failure(err) => Failure(new DecodingFailedException(err))
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

    implicit object CirceConvert extends Decodable[io.circe.Json] {
      override def decode(bytes: Array[Byte], params: EncodeParams) = {
        val str = new String(bytes, CharsetUtil.UTF_8)
        val out = io.circe.parser.decode[io.circe.Json](str).toTry
        out match {
          case Success(_) => out
          case Failure(err) => Failure(new DecodingFailedException(err))
        }
      }
    }

    implicit object BooleanConvert extends Decodable[Boolean] {
      override def decode(bytes: Array[Byte], params: EncodeParams) = {
        val str = new String(bytes, CharsetUtil.UTF_8)
        if (str == "true") Try(true)
        else if (str == "false") Try(false)
        else Failure(new DecodingFailedException(s"Boolean field has invalid value '${str}'"))
      }

      override def decodeSubDocumentGet(in: SubdocField, params: EncodeParams) = {
        Try(upickle.default.read[Boolean](in.value()))
      }

      override def decodeSubDocumentExists(in: SubdocField, params: EncodeParams): Try[Boolean] = {
        Try(in.status().success())
      }
    }

    implicit object IntConvert extends Decodable[Int] {
      override def decode(bytes: Array[Byte], params: EncodeParams) = {
        Try(new String(bytes, CharsetUtil.UTF_8).toInt)
      }
    }

    implicit object DoubleConvert extends Decodable[Double] {
      override def decode(bytes: Array[Byte], params: EncodeParams) = {
        Try(new String(bytes, CharsetUtil.UTF_8).toDouble)
      }
    }

    implicit object LongConvert extends Decodable[Long] {
      override def decode(bytes: Array[Byte], params: EncodeParams) = {
        Try(new String(bytes, CharsetUtil.UTF_8).toLong)
      }
    }

    implicit object ShortConvert extends Decodable[Short] {
      override def decode(bytes: Array[Byte], params: EncodeParams) = {
        Try(new String(bytes, CharsetUtil.UTF_8).toShort)
      }
    }
  }

}
