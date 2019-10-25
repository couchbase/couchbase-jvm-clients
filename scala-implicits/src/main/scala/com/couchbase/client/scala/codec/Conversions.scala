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

import com.couchbase.client.core.msg.kv.CodecFlags
import com.couchbase.client.scala.json._

import scala.util.Try

/** Flags to indicate the content type of a document. */
object DocumentFlags {
  val Json: Int   = CodecFlags.JSON_COMPAT_FLAGS
  val Binary: Int = CodecFlags.BINARY_COMPAT_FLAGS

  // Non-JSON String, utf-8 encoded, no BOM: "hello world"
  // (note "hello world" is still valid Json)
  val String: Int = CodecFlags.STRING_COMPAT_FLAGS
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

  /** Indicates that a document represents JSON. */
  val JsonFlags: Int = CodecFlags.JSON_COMPAT_FLAGS

  /** Indicates that a document is simply a String (e.g. it should not be treated as JSON even if that string is
    * """{"hello":"world"}"""). */
  val StringFlags: Int = CodecFlags.STRING_COMPAT_FLAGS

  /** Indicates that a document is simply an Array[Byte] (e.g. it should not be treated as JSON). */
  val BinaryFlags: Int = CodecFlags.BINARY_COMPAT_FLAGS

  private[scala] def encode[T](in: T)(implicit serializer: JsonSerializer[T]): Try[Array[Byte]] = {
    serializer.serialize(in)
  }

  private[scala] def decode[T](
      bytes: Array[Byte]
  )(implicit deserializer: JsonDeserializer[T]): Try[T] = {
    deserializer.deserialize(bytes)
  }

}
