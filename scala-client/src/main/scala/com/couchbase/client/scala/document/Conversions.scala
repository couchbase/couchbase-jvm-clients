package com.couchbase.client.scala.document

import java.nio.charset.Charset

import com.couchbase.client.core.error.DecodingFailedException
import com.couchbase.client.core.msg.kv.{SubdocCommandType, SubdocField, SubdocGetResponse}
import io.netty.util.CharsetUtil
import play.api.libs.json.JsValue

import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

trait CodecParams {
  val flags: Int

  def isPrivate = (flags & DocumentFlags.Private) != 0

  def isJson = (flags & DocumentFlags.Json) != 0

  def isBinary = (flags & DocumentFlags.Binary) != 0

  def isString = (flags & DocumentFlags.String) != 0

  override def toString = "CodecParams{" +
    "flags=" + flags +
    ",private=" + isPrivate +
    ",json=" + isJson +
    ",binary=" + isBinary +
    ",str=" + isString +
    "}"
}

case class EncodeParams(flags: Int) extends CodecParams

case class DecodeParams(flags: Int) extends CodecParams


object DocumentFlags {
  val Reserved = 0
  val Private = 1 << 24
  val Json = 2 << 24
  val Binary = 3 << 24

  // Non-JSON String, utf-8 encoded, no BOM: "hello world"
  val String = 4 << 24
}


