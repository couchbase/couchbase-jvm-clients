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

import scala.reflect.runtime.universe
import scala.util.{Failure, Success, Try}

class RawStringTranscoder extends TranscoderWithoutSerializer {
  override def encode[T](value: T): Try[EncodedValue] = {
    value match {
      case x: String =>
        Success(EncodedValue(x.getBytes(StandardCharsets.UTF_8), CodecFlags.STRING_COMPAT_FLAGS))
      case _ =>
        Failure(
          new IllegalArgumentException("Only String is supported for the RawStringTranscoder!")
        )
    }
  }

  override def decode[T](value: Array[Byte], flags: Int)(
      implicit tag: universe.WeakTypeTag[T]
  ): Try[T] = {
    if (tag.mirror.runtimeClass(tag.tpe).isAssignableFrom(classOf[String])) {
      Success(new String(value, StandardCharsets.UTF_8).asInstanceOf[T])
    } else Failure(new DecodingFailureException("RawStringTranscoder can only decode into String!"))
  }
}

object RawStringTranscoder {
  val Instance: RawStringTranscoder = new RawStringTranscoder()
}
