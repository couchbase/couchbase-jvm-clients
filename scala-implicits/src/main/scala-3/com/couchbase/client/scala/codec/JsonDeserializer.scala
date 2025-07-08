/*
 * Copyright (c) 2025 Couchbase, Inc.
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

import com.couchbase.client.core.error.DecodingFailureException
import com.couchbase.client.scala.json.{JsonArray, JsonArraySafe, JsonObject, JsonObjectSafe}
import com.couchbase.client.scala.transformers.JacksonTransformers

import scala.util.{Failure, Success, Try}

/** Many functions look for an implicit `JsonDeserializer[T]`.  These define how to convert an Array[Byte] received from the
  * Couchbase Server into a T.
  *
  * JsonDeserializer for many T are provided 'out-of-the-box', but if you need to create one for a currently unsupported
  * type then this is very simple.  Check out the JsonDeserializers in this file for examples.
  */
trait JsonDeserializer[T] extends Serializable {

  /** Decodes an Array[Byte] into a `T`.
    *
    * @param bytes bytes representing a documented, to be decoded
    * @return `Success(T)` if successful, else a `Failure(DecodingFailureException)`
    */
  def deserialize(bytes: Array[Byte]): Try[T]
}

/** Contains all built-in JsonDeserializer, which allow a variety of types to be converted from what is stored on Couchbase Server.
  */
object JsonDeserializer {

  /** `JsonDeserializer` for `Array[Byte]`, which passes through the data unchanged.
    */
  implicit object BytesConvert extends JsonDeserializer[Array[Byte]] {
    override def deserialize(bytes: Array[Byte]): Try[Array[Byte]] = Try(bytes)
  }

  /** `JsonDeserializer` for `String`, which deserializes the data as JSON.
    *
    *  Consider using `Passthrough.StringConvert` - e.g. `result.contentAs[String](Passthrough.StringConvert)` - to get
    *  the data unchanged as a String.
    */
  implicit object StringConvert extends JsonDeserializer[String] {
    override def deserialize(bytes: Array[Byte]): Try[String] = {
      Try(JacksonTransformers.MAPPER.readValue(bytes, classOf[String])) match {
        case Success(value) => Success(value)
        case Failure(err) => Failure(new DecodingFailureException(err))
      }
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
        case Success(_)   => out
        case Failure(err) => Failure(new DecodingFailureException(err))
      }
    }
  }

  /** `JsonDeserializer` converting a binary representation of a document into a `JsonObjectSafe`. */
  implicit object JsonObjectSafeConvert extends JsonDeserializer[JsonObjectSafe] {
    override def deserialize(bytes: Array[Byte]): Try[JsonObjectSafe] = {
      val out = Try(JacksonTransformers.MAPPER.readValue(bytes, classOf[JsonObject]))
      out match {
        case Success(v)   => Success(v.safe)
        case Failure(err) => Failure(new DecodingFailureException(err))
      }
    }
  }

  /** `JsonDeserializer` converting a binary representation of a document into a `JsonArray`. */
  implicit object JsonArrayConvert extends JsonDeserializer[JsonArray] {
    override def deserialize(bytes: Array[Byte]): Try[JsonArray] = {
      val out = Try(JacksonTransformers.MAPPER.readValue(bytes, classOf[JsonArray]))
      out match {
        case Success(_)   => out
        case Failure(err) => Failure(new DecodingFailureException(err))
      }
    }
  }

  /** `JsonDeserializer` converting a binary representation of a document into a `JsonArraySafe`. */
  implicit object JsonArraySafeConvert extends JsonDeserializer[JsonArraySafe] {
    override def deserialize(bytes: Array[Byte]): Try[JsonArraySafe] = {
      val out = Try(JacksonTransformers.MAPPER.readValue(bytes, classOf[JsonArray]))
      out match {
        case Success(v)   => Success(v.safe)
        case Failure(err) => Failure(new DecodingFailureException(err))
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
      else Failure(new DecodingFailureException(s"Boolean field has invalid value '${str}'"))
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

  /** `JsonDeserializer` converting a binary representation of content into a `Float`. */
  implicit object FloatConvert extends JsonDeserializer[Float] {
    override def deserialize(bytes: Array[Byte]): Try[Float] = {
      Try(new String(bytes, StandardCharsets.UTF_8).toFloat)
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
