package com.couchbase.client.scala.implicits

import com.couchbase.client.scala.codec.Conversions.{Codec, Decodable, Encodable}

import scala.language.experimental.macros

object CodecImplicits {
  def makeDecoder[T](c: scala.reflect.macros.blackbox.Context)
                    (implicit e: c.WeakTypeTag[T]) = {
    import c.universe._
    q"""
    new Decodable[${e}] {
      implicit val jsonIterDecodeCodec: JsonValueCodec[$e] = JsonCodecMaker.make[$e](CodecMakerConfig())

      override def decode(bytes: Array[Byte], params: EncodeParams): Try[$e] = {
        Try(readFromArray(bytes))
      }
    }
    """
  }

  def makeEncoder[T](c: scala.reflect.macros.blackbox.Context)
                    (implicit e: c.WeakTypeTag[T]) = {
    import c.universe._
    q"""
    new Encodable[$e] {
      implicit val jsonIterEncodeCodec: JsonValueCodec[$e] = JsonCodecMaker.make[$e](CodecMakerConfig())

      override def encode(content: $e): Try[(Array[Byte], EncodeParams)] = {
        Try(writeToArray(content), JsonFlags)
      }
    }
    """
  }


  def makeCodec[T](c: scala.reflect.macros.blackbox.Context)
                    (implicit e: c.WeakTypeTag[T]) = {
    import c.universe._
    q"""
    new Codec[$e] {
      val jsonIterCodec: JsonValueCodec[$e] = JsonCodecMaker.make[$e](CodecMakerConfig())

      override def decode(bytes: Array[Byte], params: EncodeParams): Try[$e] = {
        Try(readFromArray(bytes)(jsonIterCodec))
      }

      override def encode(content: $e): Try[(Array[Byte], EncodeParams)] = {
        Try(writeToArray(content)(jsonIterCodec), JsonFlags)
      }
    }
    """
  }
}

object Codecs {
  def decoder[T]: Decodable[T] = macro CodecImplicits.makeDecoder[T]
  def encoder[T]: Encodable[T] = macro CodecImplicits.makeEncoder[T]
  def codec[T]: Codec[T] = macro CodecImplicits.makeCodec[T]
}

