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

import scala.reflect.runtime.universe._
import scala.util.{Failure, Try}

class JsonTranscoder() extends TranscoderWithSerializer {

  override def encode[T](value: T, serializer: JsonSerializer[T]): Try[EncodedValue] = {
    value match {
      case x: Array[Byte] =>
        Failure(
          new IllegalArgumentException(
            "byte[] input is not supported for the " +
              "JsonTranscoder! If you want to store already encoded JSON, use the RawJsonTranscoder, otherwise store " +
              "it with the RawBinaryTranscoder!"
          )
        )
      case _ =>
        serializer
          .serialize(value)
          .map(bytes => EncodedValue(bytes, CodecFlags.JSON_COMPAT_FLAGS))
    }
  }

  override def decode[T](input: Array[Byte], flags: Int, serializer: JsonDeserializer[T])(
      implicit tag: TypeTag[T]
  ): Try[T] = {
    if (tag.mirror.runtimeClass(tag.tpe).isAssignableFrom(classOf[Array[Byte]])) {
      Failure(
        new IllegalArgumentException(
          "Array[Byte] input is not supported for the JsonTranscoder!. " +
            "If you want to read already encoded JSON, use the RawJsonTranscoder, otherwise read it " +
            "with the RawBinaryTranscoder!"
        )
      )
    } else {
      // Currently no validation is done on the flags, e.g. this could be a dataformat=string doc being passed to the
      // serializer, which will likely fail.  This may change in future.
      serializer.deserialize(input)
    }
  }
}

object JsonTranscoder {
  val Instance: JsonTranscoder = new JsonTranscoder()
}
