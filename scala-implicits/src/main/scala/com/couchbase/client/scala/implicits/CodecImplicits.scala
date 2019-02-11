package com.couchbase.client.scala.implicits

import com.couchbase.client.scala.codec.Conversions.{Codec, Decodable, Encodable}

import scala.language.experimental.macros


/*
  Converting case classes: a history.

  1. Try implicitly but optionally using upickle if on classpath, else circe if on classpath, etc.

  compile group: 'com.lihaoyi', name: 'upickle_2.12', version: '0.7.1', optional

  (implicit ev: Conversions.Convertable[T], up: upickle.default.ReadWriter[T] = null)

  /home/grahamp/dev/couchbase-scala-client-test-gradle/src/main/scala/test.scala:17: Symbol 'term upickle.default' is missing from the classpath.
  This symbol is required by 'value com.couchbase.client.scala.Collection.up'.
  Make sure that term default is in your classpath and check for conflicting dependencies with `-Ylog-classpath`.


  2. Try using circe's shapeless

  def circe[T](c: T)(implicit tt: TypeTag[T]): Unit = {
    import io.circe._, io.circe.generic.auto._, io.circe.parser._, io.circe.syntax._

    c.asJson
  }

  Nope, compile errors.

  3. Having my own implicit codec, written out using a macro, that calls Jsoniter under the hood.  Works.
 */

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

