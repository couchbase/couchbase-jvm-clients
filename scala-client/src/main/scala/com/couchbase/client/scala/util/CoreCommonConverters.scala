/*
 * Copyright (c) 2023 Couchbase, Inc.
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
package com.couchbase.client.scala.util

import com.couchbase.client.core.api.kv.{CoreAsyncResponse, CoreGetResult}
import com.couchbase.client.core.endpoint.http.CoreCommonOptions
import com.couchbase.client.scala.codec.Transcoder
import com.couchbase.client.scala.env.ClusterEnvironment
import com.couchbase.client.scala.kv.{GetOptions, GetResult}

import scala.concurrent.Future
import scala.concurrent.duration._

private[scala] object CoreCommonConverters {
  def convert(options: GetOptions): CoreCommonOptions = {
    CoreCommonOptions.of(
      if (options.timeout == Duration.MinusInf) null
      else java.time.Duration.ofNanos(options.timeout.toNanos),
      options.retryStrategy.orNull,
      options.parentSpan.orNull
    )
  }

  def convert(
      in: CoreGetResult,
      env: ClusterEnvironment,
      transcoder: Option[Transcoder]
  ): GetResult = {
    GetResult(
      in.key(),
      Left(in.content()),
      in.flags(),
      in.cas(),
      Option(in.expiry()),
      transcoder.getOrElse(env.transcoder)
    )
  }

  def convert[T](in: CoreAsyncResponse[T]): Future[T] = {
    FutureConversions.javaCFToScalaFuture(in.toFuture)
  }
}
