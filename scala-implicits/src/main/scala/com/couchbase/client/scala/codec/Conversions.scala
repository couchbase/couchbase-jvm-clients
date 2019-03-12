package com.couchbase.client.scala.codec

import java.nio.ByteBuffer

import com.couchbase.client.core.error.DecodingFailedException
import com.couchbase.client.core.msg.kv.{CodecFlags, SubdocCommandType, SubdocField}
import com.couchbase.client.scala.json._
import com.couchbase.client.scala.kv.MutateInMacro
import com.couchbase.client.core.deps.io.netty.util.CharsetUtil
import com.couchbase.client.scala.transformers.JacksonTransformers

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


/** Flags to indicate the content type of a document. */
object DocumentFlags {
  val Json = CodecFlags.JSON_COMPAT_FLAGS
  val Binary = CodecFlags.BINARY_COMPAT_FLAGS

  // Non-JSON String, utf-8 encoded, no BOM: "hello world"
  // (note "hello world" is still valid Json)
  val String = CodecFlags.STRING_COMPAT_FLAGS
}

/** The Scala SDK aims to be agnostic to how the user wants to represent JSON.  All these are supported:
  *
  * - Several external JSON libraries: Circe, upickle, json4s and more.
  * - A simple built-in JSON library, [[JsonObject]]
  * - Scala case classes.
  * - String and Array[Byte]
  *
  * This class contains functions to allow encoding and decoding these values to and from the Couchbase Server.
  *
  * Note there are 3 ways of representing strings on the server:
  *
  *   {"hello":"world"}  written with flags=Json
  *   foobar             written with flags=Json (note that a single string is perfectly valid json)
  *   foobar             written with flags=String
  *
  * @author Graham Pople
  * @since 1.0.0
  */
object Conversions {

  /** Many functions look for an implicit `Encodable[T]`.  These define how to convert a T into an Array[Byte] for sending
    * to the Couchbase Server - plus a flag indicating the type of the content (generally JSON).
    *
    * Encodable for many T are provided 'out-of-the-box', but if you need to create one for a currently unsupported
    * type then this is very simple.  Check out the Encodable in this file for examples.
    */
  trait Encodable[-T] {
    /** Encodes a `T` into an Array[Byte].
      *
      * @param content the content to encode
      * @return a Tuple2 of the content encoded as an Array[Byte], plus [[EncodeParams]] representing the type of the
      *         encoded content (usually JSON)
      */
    def encode(content: T): Try[(Array[Byte], EncodeParams)]

    /** Encodes a `T` into an Array[Byte], which is going to be sent as a SubDocument field.  This will almost always
      * return the same as `encode`, and is implemented purely so that a String can be sent with the required
      * surrounded double-quotes.  New implementations of Encodable should not require this.
      */
    protected[scala] def encodeSubDocumentField(content: T): Try[(Array[Byte], EncodeParams)] = encode(content)
  }

  /** Many functions look for an implicit `Decodable[T]`.  These define how to convert an Array[Byte] received from the
    * Couchbase Server into a T.
    *
    * Decodable for many T are provided 'out-of-the-box', but if you need to create one for a currently unsupported
    * type then this is very simple.  Check out the Decodable in this file for examples.
    */
  trait Decodable[T] {
    /** Decodes an Array[Byte] into a `T`.
      *
      * @param bytes bytes representing a documented, to be decoded
      * @return `Success(T)` if successful, else a `Failure(DecodingFailedException)`
      */
    def decode(bytes: Array[Byte], params: EncodeParams): Try[T]

    /** A hook to allow SubDocument results to be handled slightly differently for special cases.  New implementations of
      * Decodable should not require this. */
    protected[scala] def decodeSubDocumentField(in: SubdocField, params: EncodeParams): Try[T] = {
      in.`type`() match {
        case SubdocCommandType.EXISTS => decodeSubDocumentExists(in, params)
        case _ => decodeSubDocumentGet(in, params)
      }
    }

    /** A hook to allow SubDocument results to be handled slightly differently for special cases.  New implementations of
      * Decodable should not require this. */
    protected[scala] def decodeSubDocumentGet(in: SubdocField, params: EncodeParams): Try[T] = {
      if (in.error().isPresent) Failure(in.error().get())
      else decode(in.value(), params)
    }

    /** A hook to allow SubDocument results to be handled slightly differently for special cases.  New implementations of
      * Decodable should not require this. */
    protected[scala] def decodeSubDocumentExists(in: SubdocField, params: EncodeParams): Try[T] = {
      Failure(new DecodingFailedException("Field cannot be checked with exists"))
    }
  }

  /** A Codec conveniently combines an [[com.couchbase.client.scala.codec.Conversions.Encodable]] and
    * [[Decodable]] so that they can be created by [[com.couchbase.client.scala.implicits.Codecs.codec]] on the same line.
    */
  trait Codec[T] extends Encodable[T] with Decodable[T]

  /** Indicates that a document represents JSON. */
  val JsonFlags = EncodeParams(DocumentFlags.Json)

  /** Indicates that a document is simply a String (e.g. it should not be treated as JSON even if that string is
    * """{"hello":"world"}"""). */
  val StringFlags = EncodeParams(DocumentFlags.String)

  /** Indicates that a document is simply an Array[Byte] (e.g. it should not be treated as JSON). */
  val BinaryFlags = EncodeParams(DocumentFlags.Binary)

  private[scala] def encode[T](in: T)(implicit ev: Encodable[T]): Try[(Array[Byte], EncodeParams)] = {
    ev.encode(in)
  }

  private[scala] def decode[T](bytes: Array[Byte], params: EncodeParams = JsonFlags)
                              (implicit ev: Decodable[T]): Try[T] = {
    ev.decode(bytes, params)
  }

  /** Contains all built-in Encodable, which allow a variety of types to be converted to be stored on Couchbase Server.
    */
  object Encodable {

    /** `Encodable` for `String`.
      *
      * Assumes that the content is JSON.  Use `AsValue.StringConvert` if it's really a non-JSON String.
      */
    implicit object StringConvert extends Encodable[String] {
      override def encode(content: String) = {
        Try((content.getBytes(CharsetUtil.UTF_8), JsonFlags))
      }

      override def encodeSubDocumentField(content: String): Try[(Array[Byte], EncodeParams)] = {
        Try((('"' + content + '"').getBytes(CharsetUtil.UTF_8), JsonFlags))
      }
    }

    /** `Encodable` for `Array[Byte]`.
      *
      * Assumes that the content is JSON.  Use AsValue.BytesConvert if it's really non-JSON
      */
    implicit object BytesConvert extends Encodable[Array[Byte]] {
      override def encode(content: Array[Byte]) = {
        Try((content, JsonFlags))
      }
    }

    // TODO support Serializable


    /** These encoders treat the provided values as raw binary or strings. */
    object AsValue {

      /** An alternative `Encodable` for `Array[Byte]` that treats the input as a binary blob.
        */
      implicit object BytesConvert extends Encodable[Array[Byte]] {
        override def encode(content: Array[Byte]) = {
          Try((content, BinaryFlags))
        }
      }

      /** An alternative `Encodable` for `Array[Byte]` that treats the input as a String.
        */
      implicit object StringConvert extends Encodable[String] {
        override def encode(content: String) = {
          Try(((content).getBytes(CharsetUtil.UTF_8), StringFlags))
        }
      }

    }

    /** `Encodable` that can convert a `JsonObject` into `Array[Byte]` for sending to the server. */
   implicit object JsonObjectConvert extends Encodable[JsonObject] {
      override def encode(content: JsonObject) = {
        Try(JacksonTransformers.MAPPER.writeValueAsBytes(content), JsonFlags)
      }
    }

    /** `Encodable` that can convert a `JsonObjectSafe` into `Array[Byte]` for sending to the server. */
    implicit object JsonObjectSafeConvert extends Encodable[JsonObjectSafe] {
      override def encode(content: JsonObjectSafe) = {
        Try(JacksonTransformers.MAPPER.writeValueAsBytes(content.o), JsonFlags)
      }
    }

    /** `Encodable` that can convert a `JsonArray` into `Array[Byte]` for sending to the server. */
    implicit object JsonArrayConvert extends Encodable[JsonArray] {
      override def encode(content: JsonArray) = {
        Try(JacksonTransformers.MAPPER.writeValueAsBytes(content), JsonFlags)
      }
    }

    /** `Encodable` that can convert a `JsonArraySafe` into `Array[Byte]` for sending to the server. */
    implicit object JsonArraySafeConvert extends Encodable[JsonArraySafe] {
      override def encode(content: JsonArraySafe) = {
        Try(JacksonTransformers.MAPPER.writeValueAsBytes(content.a), JsonFlags)
      }
    }

    /** `Encodable` that can convert a `Boolean` into `Array[Byte]` for sending to the server.
      *
      * What's stored is "true" or "false" in UTF8, as bytes.
      */
    implicit object BooleanConvert extends Encodable[Boolean] {
      override def encode(content: Boolean) = {
        val str = if (content) "true" else "false"
        Try((str.getBytes(CharsetUtil.UTF_8), JsonFlags))
      }
    }

    /** `Encodable` that can convert an `Int` into `Array[Byte]` for sending to the server. */
    implicit object IntConvert extends Encodable[Int] {
      override def encode(content: Int) = {
        Try((content.toString.getBytes(CharsetUtil.UTF_8), JsonFlags))
      }
    }

    /** `Encodable` that can convert a `Double` into `Array[Byte]` for sending to the server. */
    implicit object DoubleConvert extends Encodable[Double] {
      override def encode(content: Double) = {
        Try((content.toString.getBytes(CharsetUtil.UTF_8), JsonFlags))
      }
    }

    /** `Encodable` that can convert a `Long` into `Array[Byte]` for sending to the server. */
    implicit object LongConvert extends Encodable[Long] {
      override def encode(content: Long) = {
        Try((content.toString.getBytes(CharsetUtil.UTF_8), JsonFlags))
      }
    }

    /** `Encodable` that can convert a `Short` into `Array[Byte]` for sending to the server. */
    implicit object ShortConvert extends Encodable[Short] {
      override def encode(content: Short) = {
        Try((content.toString.getBytes(CharsetUtil.UTF_8), JsonFlags))
      }
    }

    /** `Encodable` that can convert a `ujson.Value`, from the external JSON library upickle (and ujson), into
      * `Array[Byte]` for sending to the server.
      *
      * upickle is an optional dependency.
      */
    implicit object UjsonConvert extends Encodable[ujson.Value] {
      override def encode(content: ujson.Value) = {
        Try((ujson.transform(content, ujson.BytesRenderer()).toBytes, JsonFlags))
      }
    }

    /** `Encodable` that can convert a `io.circe.Json`, from the external JSON library Circe, into
      * `Array[Byte]` for sending to the server.
      *
      * Circe is an optional dependency.
      */
    implicit object CirceEncode extends Encodable[io.circe.Json] {
      override def encode(content: io.circe.Json) = {
        Try(content.noSpaces.getBytes(CharsetUtil.UTF_8)).map((_, JsonFlags))
      }
    }

    /** `Encodable` that can convert a `JsValue`, from the external JSON library Play JSON, into
      * `Array[Byte]` for sending to the server.
      *
      * Play JSON is an optional dependency.
      */
    implicit object PlayEncode extends Encodable[play.api.libs.json.JsValue] {
      override def encode(content: play.api.libs.json.JsValue) = {
        Try(play.api.libs.json.Json.stringify(content).getBytes(CharsetUtil.UTF_8)).map((_, JsonFlags))
      }
    }

    /** `Encodable` that can convert a `JValue`, from the external JSON library json4s, into
      * `Array[Byte]` for sending to the server.
      *
      * json4s is an optional dependency.
      */
    implicit object Json4sEncode extends Encodable[org.json4s.JsonAST.JValue] {
      override def encode(content: org.json4s.JsonAST.JValue) = {
        Try(org.json4s.jackson.JsonMethods.compact(content).getBytes(CharsetUtil.UTF_8)).map((_, JsonFlags))
      }
    }

    /** `Encodable` that can convert a `JValue`, from the external JSON library Jawn, into
      * `Array[Byte]` for sending to the server.
      *
      * Jawn is an optional dependency.
      */
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

  /** Contains all built-in Decodable, which allow a variety of types to be converted from what is stored on Couchbase Server.
    */
  object Decodable {

    /** `Decodable` returning a binary representation of a document directly as `Array[Byte]`. */
    implicit object BytesConvert extends Decodable[Array[Byte]] {
      override def decode(bytes: Array[Byte], params: EncodeParams): Try[Array[Byte]] = Try(bytes)
    }

    /** `Decodable` converting a binary representation of a document into a `String`. */
    implicit object StringConvert extends Decodable[String] {
      override def decode(bytes: Array[Byte], params: EncodeParams) = {
        if (params.isJson || params.isString) {
          Try(new String(bytes, CharsetUtil.UTF_8))
        }
        else {
          Failure(new DecodingFailedException(s"Cannot decode data with flags $params as a String"))
        }
      }

      override def decodeSubDocumentGet(in: SubdocField, params: EncodeParams): Try[String] = {
        if (in.error().isPresent) Failure(in.error().get())
        else {
          Try({
            val str = new String(in.value(), CharsetUtil.UTF_8)
            if (str.nonEmpty) {
              str.charAt(0) match {
                case '[' =>
                  // For arrays, do not strip anything
                  str
                case _ =>
                  // Strip off the " from start and end
                  str.substring(1, str.size - 1)
              }
            }
            else str
          })
        }
      }
    }

    /** `Decodable` converting a binary representation of a document into a `JsonObject`. */
    implicit object JsonObjectConvert extends Decodable[JsonObject] {
      override def decode(bytes: Array[Byte], params: EncodeParams) = {
        val out = Try(JacksonTransformers.MAPPER.readValue(bytes, classOf[JsonObject]))
        out match {
          case Success(_) => out
          case Failure(err) => Failure(new DecodingFailedException(err))
        }
      }
    }

    /** `Decodable` converting a binary representation of a document into a `JsonObjectSafe`. */
    implicit object JsonObjectSafeConvert extends Decodable[JsonObjectSafe] {
      override def decode(bytes: Array[Byte], params: EncodeParams) = {
        val out = Try(JacksonTransformers.MAPPER.readValue(bytes, classOf[JsonObject]))
        out match {
          case Success(v) => Success(v.safe)
          case Failure(err) => Failure(new DecodingFailedException(err))
        }
      }
    }

    /** `Decodable` converting a binary representation of a document into a `JsonArray`. */
    implicit object JsonArrayConvert extends Decodable[JsonArray] {
      override def decode(bytes: Array[Byte], params: EncodeParams) = {
        val out = Try(JacksonTransformers.MAPPER.readValue(bytes, classOf[JsonArray]))
        out match {
          case Success(_) => out
          case Failure(err) => Failure(new DecodingFailedException(err))
        }
      }
    }

    /** `Decodable` converting a binary representation of a document into a `JsonArraySafe`. */
    implicit object JsonArraySafeConvert extends Decodable[JsonArraySafe] {
      override def decode(bytes: Array[Byte], params: EncodeParams) = {
        val out = Try(JacksonTransformers.MAPPER.readValue(bytes, classOf[JsonArray]))
        out match {
          case Success(v) => Success(v.safe)
          case Failure(err) => Failure(new DecodingFailedException(err))
        }
      }
    }

    /** `Decodable` converting a binary representation of a document into a `ujson.Value`,
      * from the external JSON library upickle (and ujson).
      *
      * upickle is an optional dependency.
      */
    implicit object UjsonValueConvert extends Decodable[ujson.Value] {
      override def decode(bytes: Array[Byte], params: EncodeParams) = {
        val out = Try(upickle.default.read[ujson.Value](bytes))
        out match {
          case Success(_) => out
          case Failure(err) => Failure(new DecodingFailedException(err))
        }
      }
    }

    /** `Decodable` converting a binary representation of a document into a `ujson.Obj`,
      * from the external JSON library upickle (and ujson).
      *
      * upickle is an optional dependency.
      */
    implicit object UjsonObjConvert extends Decodable[ujson.Obj] {
      override def decode(bytes: Array[Byte], params: EncodeParams) = {
        val out = Try(upickle.default.read[ujson.Obj](bytes))
        out match {
          case Success(_) => out
          case Failure(err) => Failure(new DecodingFailedException(err))
        }
      }
    }

    /** `Decodable` converting a binary representation of a document into a `ujson.Arr`,
      * from the external JSON library upickle (and ujson).
      *
      * upickle is an optional dependency.
      */
    implicit object UjsonArrConvert extends Decodable[ujson.Arr] {
      override def decode(bytes: Array[Byte], params: EncodeParams) = {
        tryDecode(upickle.default.read[ujson.Arr](bytes))
      }
    }

    private[scala] def tryDecode[T](in: => T) = {
      val out = Try(in)
      out match {
        case Success(_) => out
        case Failure(err) => Failure(new DecodingFailedException(err))
      }
    }

    /** `Decodable` converting a binary representation of a document into a `JsValue`,
      * from the external JSON library Play Json.
      *
      * Play Json is an optional dependency.
      */
    implicit object PlayConvert extends Decodable[play.api.libs.json.JsValue] {
      override def decode(bytes: Array[Byte], params: EncodeParams) = {
        tryDecode(play.api.libs.json.Json.parse(bytes))
      }
    }

    /** `Decodable` converting a binary representation of a document into a `JValue`,
      * from the external JSON library Play4s.
      *
      * Play4s is an optional dependency.
      */
    implicit object Play4sConvert extends Decodable[org.json4s.JsonAST.JValue] {
      override def decode(bytes: Array[Byte], params: EncodeParams) = {
        tryDecode(org.json4s.native.JsonMethods.parse(new String(bytes, CharsetUtil.UTF_8)))
      }
    }

    /** `Decodable` converting a binary representation of a document into a `JValue`,
      * from the external JSON library Jawn.
      *
      * Jawn is an optional dependency.
      */
    implicit object JawnConvert extends Decodable[org.typelevel.jawn.ast.JValue] {
      override def decode(bytes: Array[Byte], params: EncodeParams) = {
        org.typelevel.jawn.Parser.parseFromString[org.typelevel.jawn.ast.JValue](new String(bytes, CharsetUtil.UTF_8))
      }
    }

    /** `Decodable` converting a binary representation of a document into a `io.circe.Json`,
      * from the external JSON library Circe.
      *
      * Circe is an optional dependency.
      */
    implicit object CirceConvert extends Decodable[io.circe.Json] {
      override def decode(bytes: Array[Byte], params: EncodeParams) = {
        val str = new String(bytes, CharsetUtil.UTF_8)
        val out = io.circe.parser.decode[io.circe.Json](str)
        out match {
          case Right(result) => Success(result)
          case Left(err) => Failure(new DecodingFailedException(err))
        }
      }
    }

    /** `Decodable` converting a binary representation of a document into a `Boolean`.
      * The document must contain "true" or "false".
      */
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

    /** `Decodable` converting a binary representation of a document into an `Int`. */
    implicit object IntConvert extends Decodable[Int] {
      override def decode(bytes: Array[Byte], params: EncodeParams) = {
        Try(new String(bytes, CharsetUtil.UTF_8).toInt)
      }
    }

    /** `Decodable` converting a binary representation of a document into a `Double`. */
    implicit object DoubleConvert extends Decodable[Double] {
      override def decode(bytes: Array[Byte], params: EncodeParams) = {
        Try(new String(bytes, CharsetUtil.UTF_8).toDouble)
      }
    }

    /** `Decodable` converting a binary representation of a document into a `Long`. */
    implicit object LongConvert extends Decodable[Long] {
      override def decode(bytes: Array[Byte], params: EncodeParams) = {
        Try(new String(bytes, CharsetUtil.UTF_8).toLong)
      }
    }

    /** `Decodable` converting a binary representation of a document into a `Short`. */
    implicit object ShortConvert extends Decodable[Short] {
      override def decode(bytes: Array[Byte], params: EncodeParams) = {
        Try(new String(bytes, CharsetUtil.UTF_8).toShort)
      }
    }
  }
}
