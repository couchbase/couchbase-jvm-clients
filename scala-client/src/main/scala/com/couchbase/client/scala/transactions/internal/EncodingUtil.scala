/*
 * Copyright 2023 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.scala.transactions.internal;

import com.couchbase.client.core.CoreContext
import com.couchbase.client.core.cnc.{CbTracing, RequestSpan, TracingIdentifiers}
import com.couchbase.client.core.msg.kv.CodecFlags
import com.couchbase.client.scala.codec.{
  EncodedValue,
  JsonSerializer,
  Transcoder,
  TranscoderWithSerializer,
  TranscoderWithoutSerializer
}

import scala.util.{Failure, Success, Try};

private[scala] object EncodingUtil {

  def encode[T](
      content: T,
      span: RequestSpan,
      serializer: JsonSerializer[T],
      transcoder: Option[Transcoder],
      coreContext: CoreContext
  ): Try[EncodedValue] = {
    val encoding = CbTracing.newSpan(coreContext, TracingIdentifiers.SPAN_REQUEST_ENCODING, span);

    val out = transcoder match {
      case Some(tc: TranscoderWithoutSerializer) =>
        tc.encode(content)
      case Some(tc: TranscoderWithSerializer) =>
        tc.encode(content, serializer)
      case None =>
        serializer
          .serialize(content)
          .map(bytes => EncodedValue(bytes, CodecFlags.JSON_COMPAT_FLAGS))
    }

    out match {
      case Failure(err) =>
        encoding.recordException(err)
        encoding.status(RequestSpan.StatusCode.ERROR)
      case Success(_) =>
        encoding.end()
    }
    out
  }

  def encode[T](
      content: T,
      span: RequestSpan,
      serializer: JsonSerializer[T],
      coreContext: CoreContext
  ): Try[Array[Byte]] = {
    encode(content, span, serializer, None, coreContext).map(_.encoded)
  }
}
