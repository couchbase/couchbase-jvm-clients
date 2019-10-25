/*
 * Copyright (c) 2019 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.scala.codec
import java.nio.charset.StandardCharsets

import com.couchbase.client.core.error.DecodingFailedException
import com.couchbase.client.scala.json.{JsonArray, JsonArraySafe, JsonObject, JsonObjectSafe}
import com.couchbase.client.scala.transformers.JacksonTransformers
import io.circe.Json
import org.json4s.JValue
import org.typelevel.jawn.ast
import play.api.libs.json.JsValue
import ujson.{Arr, Obj, Value}

import scala.util.{Failure, Success, Try}

/** Many functions look for an implicit `JsonDeserializer[T]`.  These define how to convert an Array[Byte] received from the
  * Couchbase Server into a T.
  *
  * JsonDeserializer for many T are provided 'out-of-the-box', but if you need to create one for a currently unsupported
  * type then this is very simple.  Check out the JsonDeserializers in this file for examples.
  */
trait JsonDeserializer[T] {
  /** Decodes an Array[Byte] into a `T`.
    *
    * @param bytes bytes representing a documented, to be decoded
    * @return `Success(T)` if successful, else a `Failure(DecodingFailedException)`
    */
  def deserialize(bytes: Array[Byte]): Try[T]
}

/** Contains all built-in JsonDeserializer, which allow a variety of types to be converted from what is stored on Couchbase Server.
  */
object JsonDeserializer {

  /** `JsonDeserializer` for `Array[Byte]`.
    *
    * As required by the RFC, the default deserializer for String should deserialize the data as JSON.
    */
  implicit object BytesConvert extends JsonDeserializer[Array[Byte]] {
    override def deserialize(bytes: Array[Byte]): Try[Array[Byte]] = Try(bytes)
  }

  /** `JsonDeserializer` for `String`.
    *
    * As required by the RFC, the default deserializer for Array[Byte] should pass through the data unchanged.
    */
  implicit object StringConvert extends JsonDeserializer[String] {
    override def deserialize(bytes: Array[Byte]): Try[String] = {
      tryDecode(JacksonTransformers.MAPPER.readValue(bytes, classOf[String]))
    }
  }

  object Passthrough {
    /** An alternative `JsonDeserializer` for `String`, that simply returns the raw bytes as a String.
      */
    implicit object StringConvert extends JsonDeserializer[String] {
      override def deserialize(bytes: Array[Byte]): Try[String] = {
        Success(new String(bytes, StandardCharsets.UTF_8))
      }
    }
  }

  /** `JsonDeserializer` converting a binary representation of a document into a `JsonObject`. */
  implicit object JsonObjectConvert extends JsonDeserializer[JsonObject] {
    override def deserialize(bytes: Array[Byte]): Try[JsonObject] = {
      val out = Try(JacksonTransformers.MAPPER.readValue(bytes, classOf[JsonObject]))
      out match {
        case Success(_) => out
        case Failure(err) => Failure(new DecodingFailedException(err))
      }
    }
  }

  /** `JsonDeserializer` converting a binary representation of a document into a `JsonObjectSafe`. */
  implicit object JsonObjectSafeConvert extends JsonDeserializer[JsonObjectSafe] {
    override def deserialize(bytes: Array[Byte]): Try[JsonObjectSafe] = {
      val out = Try(JacksonTransformers.MAPPER.readValue(bytes, classOf[JsonObject]))
      out match {
        case Success(v) => Success(v.safe)
        case Failure(err) => Failure(new DecodingFailedException(err))
      }
    }
  }

  /** `JsonDeserializer` converting a binary representation of a document into a `JsonArray`. */
  implicit object JsonArrayConvert extends JsonDeserializer[JsonArray] {
    override def deserialize(bytes: Array[Byte]): Try[JsonArray] = {
      val out = Try(JacksonTransformers.MAPPER.readValue(bytes, classOf[JsonArray]))
      out match {
        case Success(_) => out
        case Failure(err) => Failure(new DecodingFailedException(err))
      }
    }
  }

  /** `JsonDeserializer` converting a binary representation of a document into a `JsonArraySafe`. */
  implicit object JsonArraySafeConvert extends JsonDeserializer[JsonArraySafe] {
    override def deserialize(bytes: Array[Byte]): Try[JsonArraySafe] = {
      val out = Try(JacksonTransformers.MAPPER.readValue(bytes, classOf[JsonArray]))
      out match {
        case Success(v) => Success(v.safe)
        case Failure(err) => Failure(new DecodingFailedException(err))
      }
    }
  }

  /** `JsonDeserializer` converting a binary representation of a document into a `ujson.Value`,
    * from the external JSON library upickle (and ujson).
    *
    * upickle is an optional dependency.
    */
  implicit object UjsonValueConvert extends JsonDeserializer[ujson.Value] {
    override def deserialize(bytes: Array[Byte]): Try[Value] = {
      val out = Try(upickle.default.read[ujson.Value](bytes))
      out match {
        case Success(_) => out
        case Failure(err) => Failure(new DecodingFailedException(err))
      }
    }
  }

  /** `JsonDeserializer` converting a binary representation of a document into a `ujson.Obj`,
    * from the external JSON library upickle (and ujson).
    *
    * upickle is an optional dependency.
    */
  implicit object UjsonObjConvert extends JsonDeserializer[ujson.Obj] {
    override def deserialize(bytes: Array[Byte]): Try[Obj] = {
      val out = Try(upickle.default.read[ujson.Obj](bytes))
      out match {
        case Success(_) => out
        case Failure(err) => Failure(new DecodingFailedException(err))
      }
    }
  }

  /** `JsonDeserializer` converting a binary representation of a document into a `ujson.Arr`,
    * from the external JSON library upickle (and ujson).
    *
    * upickle is an optional dependency.
    */
  implicit object UjsonArrConvert extends JsonDeserializer[ujson.Arr] {
    override def deserialize(bytes: Array[Byte]): Try[Arr] = {
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

  /** `JsonDeserializer` converting a binary representation of a document into a `JsValue`,
    * from the external JSON library Play Json.
    *
    * Play Json is an optional dependency.
    */
  implicit object PlayConvert extends JsonDeserializer[play.api.libs.json.JsValue] {
    override def deserialize(bytes: Array[Byte]): Try[JsValue] = {
      tryDecode(play.api.libs.json.Json.parse(bytes))
    }
  }

  /** `JsonDeserializer` converting a binary representation of a document into a `JValue`,
    * from the external JSON library Json4s.
    *
    * Json4s is an optional dependency.
    */
  implicit object Json4sConvert extends JsonDeserializer[org.json4s.JsonAST.JValue] {
    override def deserialize(bytes: Array[Byte]): Try[JValue] = {
      tryDecode(org.json4s.native.JsonMethods.parse(new String(bytes, StandardCharsets.UTF_8)))
    }
  }

  /** `JsonDeserializer` converting a binary representation of a document into a `JValue`,
    * from the external JSON library Jawn.
    *
    * Jawn is an optional dependency.
    */
  implicit object JawnConvert extends JsonDeserializer[org.typelevel.jawn.ast.JValue] {
    override def deserialize(bytes: Array[Byte]): Try[ast.JValue] = {
      org.typelevel.jawn.Parser.parseFromString[org.typelevel.jawn.ast.JValue](new String(bytes, StandardCharsets.UTF_8))
    }
  }

  /** `JsonDeserializer` converting a binary representation of a document into a `io.circe.Json`,
    * from the external JSON library Circe.
    *
    * Circe is an optional dependency.
    */
  implicit object CirceConvert extends JsonDeserializer[io.circe.Json] {
    override def deserialize(bytes: Array[Byte]): Try[Json] = {
      val str = new String(bytes, StandardCharsets.UTF_8)
      val out = io.circe.parser.decode[io.circe.Json](str)
      out match {
        case Right(result) => Success(result)
        case Left(err) => Failure(new DecodingFailedException(err))
      }
    }
  }

  /** `JsonDeserializer` converting a binary representation of a document into a `Boolean`.
    * The document must contain "true" or "false".
    */
  implicit object BooleanConvert extends JsonDeserializer[Boolean] {
    override def deserialize(bytes: Array[Byte]): Try[Boolean] = {
      val str = new String(bytes, StandardCharsets.UTF_8)
      if (str == "true") Try(true)
      else if (str == "false") Try(false)
      else Failure(new DecodingFailedException(s"Boolean field has invalid value '${str}'"))
    }
  }

  /** `JsonDeserializer` converting a binary representation of a document into an `Int`. */
  implicit object IntConvert extends JsonDeserializer[Int] {
    override def deserialize(bytes: Array[Byte]): Try[Int] = {
      Try(new String(bytes, StandardCharsets.UTF_8).toInt)
    }
  }

  /** `JsonDeserializer` converting a binary representation of a document into a `Double`. */
  implicit object DoubleConvert extends JsonDeserializer[Double] {
    override def deserialize(bytes: Array[Byte]): Try[Double] = {
      Try(new String(bytes, StandardCharsets.UTF_8).toDouble)
    }
  }

  /** `JsonDeserializer` converting a binary representation of a document into a `Long`. */
  implicit object LongConvert extends JsonDeserializer[Long] {
    override def deserialize(bytes: Array[Byte]): Try[Long] = {
      Try(new String(bytes, StandardCharsets.UTF_8).toLong)
    }
  }

  /** `JsonDeserializer` converting a binary representation of a document into a `Short`. */
  implicit object ShortConvert extends JsonDeserializer[Short] {
    override def deserialize(bytes: Array[Byte]): Try[Short] = {
      Try(new String(bytes, StandardCharsets.UTF_8).toShort)
    }
  }
}