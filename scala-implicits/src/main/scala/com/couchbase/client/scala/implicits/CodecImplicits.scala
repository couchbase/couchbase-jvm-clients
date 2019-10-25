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
package com.couchbase.client.scala.implicits

import com.couchbase.client.scala.codec.{JsonDeserializer, JsonSerializer}

import scala.language.experimental.macros

/** The Scala SDK allows Scala case classes to be directly encoded and decoded to and from the Couchbase Server.
  *
  * But to do this, it needs to be told how to encode and decode the case class to and from JSON.
  *
  * More technically, if you are dealing with a case class `User`, you need an
  * `JsonSerializer[User]` to send it the SDK, and a `JsonDeserializer[User]` to retrieve it.  Or a `Codec[User]`, which conveniently
  * is both.
  *
  * A `Codec[User]` can easily be created like this:
  *
  * {{{
  *   case class Address(line1: String, line2: String)
  *   case class User(name: String, age: Int, address: Address)
  *
  *   object User {
  *     implicit val codec: Codec[User] = Codecs.codec[User]
  *   }
  * }}}
  *
  * Note that a `Codec` is only needed for the top-level case class: e.g. if you are inserting a User, you do
  * not need a `Codec[Address]`.
  *
  * @author Graham Pople
  * @since 1.0.0
  */
object Codec {

  /** Creates a `Codec` for the given type `T`, which is both a `JsonDeserializer[T]` and `JsonSerializer[T]`.  This is everything
    * required to send a case class directly to the Scala SDK, and retrieve results as it.
    */
  def codec[T]: Codec[T] = macro CodecImplicits.makeCodec[T]
}

/** A Codec conveniently combines an [[com.couchbase.client.scala.codec.JsonSerializer]] and
  * [[JsonDeserializer]] so that they can be created by [[com.couchbase.client.scala.implicits.Codec.codec]] on the same line.
  */
trait CodecWrapper[-A,B] extends JsonSerializer[A] with JsonDeserializer[B]
trait Codec[A] extends CodecWrapper[A,A]

private[scala] object CodecImplicits {
  // Implementation detail: the excellent JSON library Jsoniter, with the extensions from com.github.plokhotnyuk.jsoniter_scala,
  // is currently used to encode and decode case classes.  This is purely an implementation detail and should not be
  // relied upon.
  def makeDeserializer[T](c: scala.reflect.macros.blackbox.Context)
                    (implicit e: c.WeakTypeTag[T]): c.universe.Tree = {
    import c.universe._
    q"""
    new JsonDeserializer[${e}] {
      import com.github.plokhotnyuk.jsoniter_scala.core._
      import com.github.plokhotnyuk.jsoniter_scala.macros._

      implicit val jsonIterDecodeCodec: JsonValueCodec[$e] =
        JsonCodecMaker.make[$e](CodecMakerConfig)

      override def deserialize(bytes: Array[Byte]): scala.util.Try[$e] = {
        scala.util.Try(readFromArray(bytes))
      }
    }
    """
  }

  def makeSerializer[T](c: scala.reflect.macros.blackbox.Context)
                    (implicit e: c.WeakTypeTag[T]): c.universe.Tree = {
    import c.universe._
    q"""
    new JsonSerializer[$e] {
      import com.github.plokhotnyuk.jsoniter_scala.core._
      import com.github.plokhotnyuk.jsoniter_scala.macros._

      implicit val jsonIterEncodeCodec: JsonValueCodec[$e] =
       JsonCodecMaker.make[$e](CodecMakerConfig)

      override def serialize(content: $e): scala.util.Try[Array[Byte]] = {
        scala.util.Try(writeToArray(content))
      }
    }
    """
  }

  def makeCodec[T](c: scala.reflect.macros.blackbox.Context)
                  (implicit e: c.WeakTypeTag[T]): c.universe.Tree = {
    import c.universe._
    q"""
    new Codec[$e] {
      import com.github.plokhotnyuk.jsoniter_scala.core._
      import com.github.plokhotnyuk.jsoniter_scala.macros._
      import scala.reflect.runtime.universe._
      import scala.util.{Failure, Success, Try}

      val jsonIterCodec: JsonValueCodec[$e] =
       JsonCodecMaker.make[$e](CodecMakerConfig)

      override def serialize(input: $e): Try[Array[Byte]] = {
        scala.util.Try(writeToArray(input)(jsonIterCodec))
      }

      override def deserialize(input: Array[Byte]): Try[$e] = {
        scala.util.Try(readFromArray(input)(jsonIterCodec))
      }
    }
    """
  }
}