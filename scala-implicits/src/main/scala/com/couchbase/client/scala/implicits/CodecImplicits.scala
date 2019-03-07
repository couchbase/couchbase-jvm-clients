package com.couchbase.client.scala.implicits

import com.couchbase.client.scala.codec.Conversions.{Codec, Decodable, Encodable}

import scala.language.experimental.macros

// TODO ScalaDocs
object Codecs {
  def decoder[T]: Decodable[T] = macro CodecImplicits.makeDecoder[T]
  def encoder[T]: Encodable[T] = macro CodecImplicits.makeEncoder[T]
  def codec[T]: Codec[T] = macro CodecImplicits.makeCodec[T]
}

object CodecImplicits {
  def makeDecoder[T](c: scala.reflect.macros.blackbox.Context)
                    (implicit e: c.WeakTypeTag[T]) = {
    import c.universe._
    // Note we use a shadowed version of plokhotnyuk.jsoniter_scala
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