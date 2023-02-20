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

package com.couchbase.client.core.io.netty.search

import com.couchbase.client.core.cnc.metrics.NoopMeter

import java.io.{ByteArrayOutputStream, InputStream}
import java.nio.charset.StandardCharsets
import java.util.Optional
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled
import com.couchbase.client.core.deps.io.netty.channel.embedded.EmbeddedChannel
import com.couchbase.client.core.deps.io.netty.handler.codec.http._
import com.couchbase.client.core.endpoint.{BaseEndpoint, EndpointContext}
import com.couchbase.client.core.env.CoreEnvironment
import com.couchbase.client.core.msg.search.SearchRequest
import com.couchbase.client.core.retry.BestEffortRetryStrategy
import com.couchbase.client.core.util.HostAndPort
import com.couchbase.client.core.{Core, CoreContext}
import com.couchbase.client.scala.AsyncCluster
import com.couchbase.client.scala.env.PasswordAuthenticator
import com.couchbase.client.scala.json.JsonObject
import com.couchbase.client.scala.search.result.SearchResult
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import concurrent.ExecutionContext.Implicits.global

/**
  * Mocks out the search code to allow doing unit tests against the search service.
  *
  * This makes it much easier to test against some hard-to-replicate issues.
  */
object SearchMock {

  private def toByteArray(in: InputStream): Array[Byte] = {
    val os     = new ByteArrayOutputStream
    val buffer = new Array[Byte](1024)
    var len    = 0
    // read bytes from the input stream and store them in buffer
    while (len != -1) { // write bytes from the buffer into output stream
      len = in.read(buffer)
      if (len != -1) os.write(buffer, 0, len)
    }
    os.toByteArray
  }

  /**
    * Given JSON in the form expected, e.g. those from https://github.com/chvck/sdk-testcases which contains the
    * returned JSON from the search service in a field "data", returns the completed SearchResult that the API
    * would return.
    */
  def loadSearchTestCase(json: InputStream): SearchResult = {

    // The idea is to fake packets that have come from the search service.
    // Start by preparing the packets.
    val jo   = JsonObject.fromJson(new String(toByteArray(json)))
    val data = jo.obj("data")

    val b     = data.toString.getBytes(StandardCharsets.UTF_8)
    val bytes = Unpooled.wrappedBuffer(b)

    val content = new DefaultLastHttpContent(bytes)
    val resp    = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)

    // Fake some core stuff
    val mockedCore = mock(classOf[Core])
    val env        = CoreEnvironment.builder.meter(NoopMeter.INSTANCE).build
    val ctx        = new CoreContext(mockedCore, 0, env, PasswordAuthenticator("not", "used"))

    // Our ChunkedSearchMessageHandler needs to be initialised by pretending we've sent an outbound SearchRequest
    // through it
    val req = new SearchRequest(
      java.time.Duration.ofSeconds(10),
      ctx,
      BestEffortRetryStrategy.INSTANCE,
      null,
      null,
      null,
      null,
      null
    )

    // ChunkedSearchMessageHandler will try to encode() the SearchRequest.  Rather than mocking everything required
    // to get that working, just mock the encode method.
    val spiedReq = spy(req)
    doReturn(new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "localhost"), null)
      .when(spiedReq)
      .encode

    doAnswer((v) => {
      val endpointContext = new EndpointContext(
        ctx,
        new HostAndPort("127.0.0.1", 0),
        null,
        null,
        null,
        Optional.of("bucket"),
        null
      )

      val endpoint = mock(classOf[BaseEndpoint])
      when(endpoint.pipelined).thenReturn(false)

      // ChunkedSearchMessageHandler does most of the work in handling responses from the service
      val handler = new ChunkedSearchMessageHandler(endpoint, endpointContext)

      // Netty's EmbeddedChannel lets us test ChannelHandlers like ChunkedSearchMessageHandler.  It's a Netty Channel
      // that doesn't touch the network at all.
      val channel = new EmbeddedChannel(handler)

      // Writing the request is necessary to estabish some initial state inChunkedSearchMessageHandler
      channel.writeAndFlush(spiedReq)

      // Finally we can do the interesting bit of passing our fake FTS service response into
      // ChunkedSearchMessageHandler
      channel.writeInbound(resp)
      channel.writeInbound(content)
      null
    }).when(mockedCore).send(any)

    val future = AsyncCluster.searchQuery(req, mockedCore)
    val result = Await.result(future, Duration.Inf)
    result
  }
}
