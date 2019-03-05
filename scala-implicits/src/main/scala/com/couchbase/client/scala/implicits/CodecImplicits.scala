package com.couchbase.client.scala.implicits

import com.couchbase.client.scala.codec.Conversions.{Codec, Decodable, Encodable}

import scala.language.experimental.macros


object Codecs {
  def decoder[T]: Decodable[T] = macro CodecImplicits.makeDecoder[T]
  def encoder[T]: Encodable[T] = macro CodecImplicits.makeEncoder[T]
  def codec[T]: Codec[T] = macro CodecImplicits.makeCodec[T]
}

object CodecImplicits {
  def makeDecoder[T](c: scala.reflect.macros.blackbox.Context)
                    (implicit e: c.WeakTypeTag[T]) = {
    import c.universe._
    // TODO use the shadowed version of plokhotnyuk
    q"""
    new Decodable[${e}] {
      implicit val jsonIterDecodeCodec: com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec[$e] =
        com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker.make[$e](com.github.plokhotnyuk.jsoniter_scala.macros.CodecMakerConfig())

      override def decode(bytes: Array[Byte], params: codec.EncodeParams): scala.util.Try[$e] = {
        scala.util.Try(com.github.plokhotnyuk.jsoniter_scala.core.readFromArray(bytes))
      }
    }
    """
  }

  def makeEncoder[T](c: scala.reflect.macros.blackbox.Context)
                    (implicit e: c.WeakTypeTag[T]) = {
    import c.universe._
    q"""
    new Encodable[$e] {
      implicit val jsonIterEncodeCodec: com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec[$e] =
       com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker.make[$e](com.github.plokhotnyuk.jsoniter_scala.macros.CodecMakerConfig())

      override def encode(content: $e): scala.util.Try[(Array[Byte], com.couchbase.client.scala.codec.EncodeParams)] = {
        scala.util.Try((com.github.plokhotnyuk.jsoniter_scala.core.writeToArray(content), com.couchbase.client.scala.codec.Conversions.JsonFlags))
      }
    }
    """
  }


  def makeCodec[T](c: scala.reflect.macros.blackbox.Context)
                  (implicit e: c.WeakTypeTag[T]) = {
    import c.universe._
    q"""
    new Codec[$e] {
      val jsonIterCodec: com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec[$e] =
       com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker.make[$e](com.github.plokhotnyuk.jsoniter_scala.macros.CodecMakerConfig())

      override def decode(bytes: Array[Byte], params: com.couchbase.client.scala.codec.EncodeParams): scala.util.Try[$e] = {
        scala.util.Try(com.github.plokhotnyuk.jsoniter_scala.core.readFromArray(bytes)(jsonIterCodec))
      }

      override def encode(content: $e): scala.util.Try[(Array[Byte], com.couchbase.client.scala.codec.EncodeParams)] = {
        scala.util.Try((com.github.plokhotnyuk.jsoniter_scala.core.writeToArray(content)(jsonIterCodec), com.couchbase.client.scala.codec.Conversions.JsonFlags))
      }
    }
    """
  }
}