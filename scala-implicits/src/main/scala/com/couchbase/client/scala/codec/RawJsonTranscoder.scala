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

import com.couchbase.client.core.error.DecodingFailureException
import com.couchbase.client.core.msg.kv.CodecFlags

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

class RawJsonTranscoder extends TranscoderWithoutSerializer {
  override def encode[T](value: T): Try[EncodedValue] = {
    value match {
      case x: Array[Byte] => Success(EncodedValue(x, CodecFlags.JSON_COMPAT_FLAGS))
      case x: String      =>
        Success(EncodedValue(x.getBytes(StandardCharsets.UTF_8), CodecFlags.JSON_COMPAT_FLAGS))
      case _ =>
        Failure(
          new IllegalArgumentException(
            "Only Array[Byte] and String types are supported for the RawJsonTranscoder!"
          )
        )
    }
  }

  override def decode[T](value: Array[Byte], flags: Int)(
      implicit tag: ClassTag[T]
  ): Try[T] = {
    if (tag.runtimeClass.isAssignableFrom(classOf[Array[Byte]])) {
      Success(value.asInstanceOf[T])
    } else if (tag.runtimeClass.isAssignableFrom(classOf[String])) {
      Success(new String(value, StandardCharsets.UTF_8).asInstanceOf[T])
    } else
      Failure(
        new DecodingFailureException(
          "RawJsonTranscoder can only decode into Array[Byte] or String!"
        )
      )
  }
}

object RawJsonTranscoder {
  val Instance: RawJsonTranscoder = new RawJsonTranscoder()
}
