package com.couchbase.client.scala.implicits

import com.couchbase.client.scala.codec.Conversions.{Codec, Decodable, Encodable}

import scala.language.experimental.macros

/** The Scala SDK allows Scala case classes to be directly encoded and decoded to and from the Couchbase Server.
  *
  * But to do this, it needs to be told how to encode and decode the case class to and from JSON.
  *
  * More technically, if you are dealing with a case class `User`, you need an
  * `Encodable[User]` to send it the SDK, and a `Decodable[User]` to retrieve it.  Or a `Codec[User]`, which conveniently
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
object Codecs {

  /** Creates a `Codec` for the given type `T`, which is both a `Decodable[T]` and `Encodable[T]`.  This is everything
    * required to send a case class directly to the Scala SDK, and retrieve results as it.
    */
  def codec[T]: Codec[T] = macro CodecImplicits.makeCodec[T]
}

private[scala] object CodecImplicits {
  // Implementation detail: the excellent JSON library Jsoniter, with the extensions from com.github.plokhotnyuk.jsoniter_scala,
  // is currently used to encode and decode case classes.  This is purely an implementation detail and should not be
  // relied upon.
  def makeDecoder[T](c: scala.reflect.macros.blackbox.Context)
                    (implicit e: c.WeakTypeTag[T]) = {
    import c.universe._
    q"""
    new Decodable[${e}] {
      import com.github.plokhotnyuk.jsoniter_scala.core._
      import com.github.plokhotnyuk.jsoniter_scala.macros._

      implicit val jsonIterDecodeCodec: JsonValueCodec[$e] =
        JsonCodecMaker.make[$e](CodecMakerConfig())

      override def decode(bytes: Array[Byte], params: codec.EncodeParams): scala.util.Try[$e] = {
        scala.util.Try(readFromArray(bytes))
      }
    }
    """
  }

  def makeEncoder[T](c: scala.reflect.macros.blackbox.Context)
                    (implicit e: c.WeakTypeTag[T]) = {
    import c.universe._
    q"""
    new Encodable[$e] {
      import com.github.plokhotnyuk.jsoniter_scala.core._
      import com.github.plokhotnyuk.jsoniter_scala.macros._

      implicit val jsonIterEncodeCodec: JsonValueCodec[$e] =
       JsonCodecMaker.make[$e](CodecMakerConfig())

      override def encode(content: $e): scala.util.Try[(Array[Byte], com.couchbase.client.scala.codec.EncodeParams)] = {
        scala.util.Try((writeToArray(content), com.couchbase.client.scala.codec.Conversions.JsonFlags))
      }
    }
    """
  }


  def makeCodec[T](c: scala.reflect.macros.blackbox.Context)
                  (implicit e: c.WeakTypeTag[T]) = {
    import c.universe._
    q"""
    new Codec[$e] {
      import com.github.plokhotnyuk.jsoniter_scala.core._
      import com.github.plokhotnyuk.jsoniter_scala.macros._

      val jsonIterCodec: JsonValueCodec[$e] =
       JsonCodecMaker.make[$e](CodecMakerConfig())

      override def decode(bytes: Array[Byte], params: com.couchbase.client.scala.codec.EncodeParams): scala.util.Try[$e] = {
        scala.util.Try(readFromArray(bytes)(jsonIterCodec))
      }

      override def encode(content: $e): scala.util.Try[(Array[Byte], com.couchbase.client.scala.codec.EncodeParams)] = {
        scala.util.Try((writeToArray(content)(jsonIterCodec), com.couchbase.client.scala.codec.Conversions.JsonFlags))
      }
    }
    """
  }
}