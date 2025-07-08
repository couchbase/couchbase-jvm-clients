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

import com.couchbase.client.core.deps.io.netty.util.CharsetUtil
import com.couchbase.client.scala.json.{JsonArray, JsonArraySafe, JsonObject, JsonObjectSafe}
import com.couchbase.client.scala.kv.MutateInMacro
import com.couchbase.client.scala.transformers.JacksonTransformers

import scala.util.Try

/** Many functions look for an implicit `JsonSerializer[T]`.  These define how to convert a T into an Array[Byte] for sending
  * to the Couchbase Server.
  *
  * JsonSerializer for many T are provided 'out-of-the-box', but if you need to create one for a currently unsupported
  * type then this is very simple.  Check out the JsonSerializers in this file for examples.
  */
trait JsonSerializer[-T] extends Serializable {

  /** Encodes a `T` into an Array[Byte].
    *
    * @param content the content to encode
    * @return the content encoded as an Array[Byte]
    */
  def serialize(content: T): Try[Array[Byte]]
}

/** Contains all built-in JsonSerializer, which allow a variety of types to be converted to be stored on Couchbase Server.
  */
object JsonSerializer {

  /** `JsonSerializer` for `String`.
    *
    * As required by the RFC, the default serializer for String should serialize the data as JSON.  With the default
    * transcoder, the serialized data will be stored on the server as JSON type.
    *
    * If the String should be not serialized and stored on the server as string type, use [[RawStringTranscoder]].
    * If the String should be not serialized and stored on the server as JSON type, use [[RawJsonTranscoder]].
    */
  implicit object StringConvert extends JsonSerializer[String] {
    override def serialize(content: String): Try[Array[Byte]] = {
      Try(JacksonTransformers.MAPPER.writeValueAsBytes(content))
    }
  }

  /** `JsonSerializer` for `Array[Byte]`.
    *
    * As required by the RFC, the default serializer for Array[Byte] should pass through the data unchanged.  With
    * the default transcoder, the unchanged data will be stored on the server as JSON type.
    *
    * If the content should be stored on the server as binary type, use [[RawBinaryTranscoder]].
    */
  implicit object BytesConvert extends JsonSerializer[Array[Byte]] {
    override def serialize(content: Array[Byte]): Try[Array[Byte]] = {
      Try(content)
    }
  }

  /** `JsonSerializer` that can convert a `JsonObject` into `Array[Byte]` for sending to the server. */
  implicit object JsonObjectConvert extends JsonSerializer[JsonObject] {
    override def serialize(content: JsonObject): Try[Array[Byte]] = {
      Try(JacksonTransformers.MAPPER.writeValueAsBytes(content))
    }
  }

  /** `JsonSerializer` that can convert a `JsonObjectSafe` into `Array[Byte]` for sending to the server. */
  implicit object JsonObjectSafeConvert extends JsonSerializer[JsonObjectSafe] {
    override def serialize(content: JsonObjectSafe): Try[Array[Byte]] = {
      Try(JacksonTransformers.MAPPER.writeValueAsBytes(content.o))
    }
  }

  /** `JsonSerializer` that can convert a `JsonArray` into `Array[Byte]` for sending to the server. */
  implicit object JsonArrayConvert extends JsonSerializer[JsonArray] {
    override def serialize(content: JsonArray): Try[Array[Byte]] = {
      Try(JacksonTransformers.MAPPER.writeValueAsBytes(content))
    }
  }

  /** `JsonSerializer` that can convert a `JsonArraySafe` into `Array[Byte]` for sending to the server. */
  implicit object JsonArraySafeConvert extends JsonSerializer[JsonArraySafe] {
    override def serialize(content: JsonArraySafe): Try[Array[Byte]] = {
      Try(JacksonTransformers.MAPPER.writeValueAsBytes(content.a))
    }
  }

  /** `JsonSerializer` that can convert a `Boolean` into `Array[Byte]` for sending to the server.
    *
    * What's stored is "true" or "false" in UTF8, as bytes.
    */
  implicit object BooleanConvert extends JsonSerializer[Boolean] {
    override def serialize(content: Boolean): Try[Array[Byte]] = {
      val str = if (content) "true" else "false"
      Try(str.getBytes(CharsetUtil.UTF_8))
    }
  }

  /** `JsonSerializer` that can convert an `Int` into `Array[Byte]` for sending to the server. */
  implicit object IntConvert extends JsonSerializer[Int] {
    override def serialize(content: Int): Try[Array[Byte]] = {
      Try(content.toString.getBytes(CharsetUtil.UTF_8))
    }
  }

  /** `JsonSerializer` that can convert a `Double` into `Array[Byte]` for sending to the server. */
  implicit object DoubleConvert extends JsonSerializer[Double] {
    override def serialize(content: Double): Try[Array[Byte]] = {
      Try(content.toString.getBytes(CharsetUtil.UTF_8))
    }
  }

  /** `JsonSerializer` that can convert a `Long` into `Array[Byte]` for sending to the server. */
  implicit object LongConvert extends JsonSerializer[Long] {
    override def serialize(content: Long): Try[Array[Byte]] = {
      Try(content.toString.getBytes(CharsetUtil.UTF_8))
    }
  }

  /** `JsonSerializer` that can convert a `Short` into `Array[Byte]` for sending to the server. */
  implicit object ShortConvert extends JsonSerializer[Short] {
    override def serialize(content: Short): Try[Array[Byte]] = {
      Try(content.toString.getBytes(CharsetUtil.UTF_8))
    }
  }

  implicit object MutateInMacroConvert extends JsonSerializer[MutateInMacro] {
    override def serialize(content: MutateInMacro): Try[Array[Byte]] = {
      Try(content.value.getBytes(CharsetUtil.UTF_8))
    }
  }
}
