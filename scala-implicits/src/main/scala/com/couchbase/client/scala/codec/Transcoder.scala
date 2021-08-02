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

import scala.language.experimental.macros
import scala.reflect.runtime.universe._
import scala.util.Try

case class EncodedValue(encoded: Array[Byte], flags: Int)

sealed trait Transcoder extends Serializable

trait TranscoderWithoutSerializer extends Transcoder {
  def encode[T](value: T): Try[EncodedValue]

  def decode[T](value: Array[Byte], flags: Int)(implicit tag: WeakTypeTag[T]): Try[T]
}

trait TranscoderWithSerializer extends Transcoder {
  def encode[A](value: A, serializer: JsonSerializer[A]): Try[EncodedValue]

  def decode[A](value: Array[Byte], flags: Int, serializer: JsonDeserializer[A])(
      implicit tag: WeakTypeTag[A]
  ): Try[A]
}
