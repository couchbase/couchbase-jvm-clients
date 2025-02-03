package com.couchbase.client.performer.scala.util

import com.couchbase.client.protocol.shared.{ContentAs, ContentTypes}
import com.couchbase.client.scala.json.{JsonArray, JsonObject}
import com.google.protobuf.ByteString

import java.nio.charset.StandardCharsets
import scala.util.Try

object ContentAsUtil {
  def contentType(
      contentAs: ContentAs,
      asByteArray: () => Try[Array[Byte]],
      asString: () => Try[String],
      asJsonObject: () => Try[JsonObject],
      asJsonArray: () => Try[JsonArray],
      asBoolean: () => Try[Boolean],
      asInteger: () => Try[Int],
      asFloatingPoint: () => Try[Double]
  ): Try[ContentTypes] = {
    if (contentAs.hasAsString) {
      asString().map(value => ContentTypes.newBuilder.setContentAsString(value).build)
    } else if (contentAs.hasAsByteArray) {
      asByteArray().map(
        value => ContentTypes.newBuilder.setContentAsBytes(ByteString.copyFrom(value)).build
      )
    } else if (contentAs.hasAsJsonObject) {
      asJsonObject().map(
        value =>
          ContentTypes.newBuilder
            .setContentAsBytes(ByteString.copyFrom(value.toString.getBytes(StandardCharsets.UTF_8)))
            .build
      )
    } else if (contentAs.hasAsJsonArray) {
      asJsonArray().map(
        value =>
          ContentTypes.newBuilder
            .setContentAsBytes(ByteString.copyFrom(value.toString.getBytes(StandardCharsets.UTF_8)))
            .build
      )
    } else if (contentAs.hasAsBoolean) {
      asBoolean().map(value => ContentTypes.newBuilder.setContentAsBool(value).build)
    } else if (contentAs.hasAsInteger) {
      asInteger().map(value => ContentTypes.newBuilder.setContentAsInt64(value).build)
    } else if (contentAs.hasAsFloatingPoint) {
      asFloatingPoint().map(value => ContentTypes.newBuilder.setContentAsDouble(value).build)
    } else
      throw new UnsupportedOperationException(
        s"Scala performer cannot handle contentAs ${contentAs}"
      )
  }

  def contentTypeSeq(
      contentAs: ContentAs,
      asByteArray: () => Try[Seq[Array[Byte]]],
      asString: () => Try[Seq[String]],
      asJsonObject: () => Try[Seq[JsonObject]],
      asJsonArray: () => Try[Seq[JsonArray]],
      asBoolean: () => Try[Seq[Boolean]],
      asInteger: () => Try[Seq[Int]],
      asFloatingPoint: () => Try[Seq[Double]]
  ): Try[Seq[ContentTypes]] = {
    if (contentAs.hasAsString) {
      asString().map(
        values => values.map(value => ContentTypes.newBuilder.setContentAsString(value).build)
      )
    } else if (contentAs.hasAsByteArray) {
      asByteArray().map(
        values =>
          values.map(
            value => ContentTypes.newBuilder.setContentAsBytes(ByteString.copyFrom(value)).build
          )
      )
    } else if (contentAs.hasAsJsonObject) {
      asJsonObject().map(
        values =>
          values.map(
            value =>
              ContentTypes.newBuilder
                .setContentAsBytes(
                  ByteString.copyFrom(value.toString.getBytes(StandardCharsets.UTF_8))
                )
                .build
          )
      )
    } else if (contentAs.hasAsJsonArray) {
      asJsonArray().map(
        values =>
          values.map(
            value =>
              ContentTypes.newBuilder
                .setContentAsBytes(
                  ByteString.copyFrom(value.toString.getBytes(StandardCharsets.UTF_8))
                )
                .build
          )
      )
    } else if (contentAs.hasAsBoolean) {
      asBoolean().map(
        values => values.map(value => ContentTypes.newBuilder.setContentAsBool(value).build)
      )
    } else if (contentAs.hasAsInteger) {
      asInteger().map(
        values => values.map(value => ContentTypes.newBuilder.setContentAsInt64(value).build)
      )
    } else if (contentAs.hasAsFloatingPoint) {
      asFloatingPoint().map(
        values => values.map(value => ContentTypes.newBuilder.setContentAsDouble(value).build)
      )
    } else
      throw new UnsupportedOperationException(
        s"Scala performer cannot handle contentAs ${contentAs}"
      )
  }

    def convert(content: ContentTypes): Array[Byte] = if (content.hasContentAsBytes) content.getContentAsBytes.toByteArray
    else if (content.hasContentAsString) content.getContentAsString.getBytes
    else throw new UnsupportedOperationException("Cannot convert " + content)
}
