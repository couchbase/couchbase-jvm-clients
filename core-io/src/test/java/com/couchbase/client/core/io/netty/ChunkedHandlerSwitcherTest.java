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

package com.couchbase.client.core.io.netty;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.deps.io.netty.channel.embedded.EmbeddedChannel;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpObjectAggregator;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpResponseStatus;
import com.couchbase.client.core.endpoint.BaseEndpoint;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.endpoint.http.CoreHttpRequest;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.io.netty.chunk.ChunkResponseParser;
import com.couchbase.client.core.io.netty.chunk.ChunkedMessageHandler;
import com.couchbase.client.core.msg.HttpRequest;
import com.couchbase.client.core.msg.NonChunkedHttpRequest;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.msg.chunk.ChunkHeader;
import com.couchbase.client.core.msg.chunk.ChunkRow;
import com.couchbase.client.core.msg.chunk.ChunkTrailer;
import com.couchbase.client.core.msg.chunk.ChunkedResponse;
import com.couchbase.client.core.msg.search.ServerSearchRequest;
import com.couchbase.client.core.retry.FailFastRetryStrategy;
import com.couchbase.client.core.service.ServiceType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.couchbase.client.core.util.MockUtil.mockCore;
import static com.couchbase.client.core.util.MockUtil.mockCoreContext;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies the functionality of the {@link ChunkedHandlerSwitcher}.
 *
 * @since 3.0.0
 */
class ChunkedHandlerSwitcherTest {

  private static CoreEnvironment env;

  private EmbeddedChannel channel;
  private EndpointContext endpointContext;
  private BaseEndpoint endpoint;

  @BeforeAll
  static void beforeAll() {
    env = CoreEnvironment.create();
  }

  @AfterAll
  static void afterAll() {
    env.shutdown();
  }

  @BeforeEach
  void setup() {
    Core core = mockCore(env);
    endpointContext = mockCoreContext(core, EndpointContext.class);

    endpoint = mock(BaseEndpoint.class);
    when(endpoint.context()).thenReturn(endpointContext);
    when(endpoint.pipelined()).thenReturn(false);
    channel = setupChannel();
  }

  @AfterEach
  void teardown() {
    channel.finishAndReleaseAll();
  }

  /**
   * When the channel is active, the chunked one must be loaded initially.
   */
  @Test
  void startsWithChunkedHandlerInPipeline() {
    assertChunkedInPipeline(channel);
  }

  /**
   * If chunked is loaded, make sure it switches over to non-chunked.
   */
  @Test
  void switchesToNonChunkIfNeeded() {
    assertChunkedInPipeline(channel);
    CoreHttpRequest upsertRequest = mock(CoreHttpRequest.class);
    when(upsertRequest.retryStrategy()).thenReturn(FailFastRetryStrategy.INSTANCE);
    channel.write(upsertRequest);
    assertNonChunkedInPipeline(channel);
  }

  /**
   * If non-chunked is loaded, make sure it switches back to chunked.
   */
  @Test
  void switchesToChunkIfNeeded() {
    assertChunkedInPipeline(channel);

    for (int i = 0; i < 2; i++) {
      CoreHttpRequest genericSearchRequest = mock(CoreHttpRequest.class);
      when(genericSearchRequest.retryStrategy()).thenReturn(FailFastRetryStrategy.INSTANCE);
      channel.write(genericSearchRequest);
      assertNonChunkedInPipeline(channel);

      ServerSearchRequest searchRequest = mock(ServerSearchRequest.class);
      when(searchRequest.retryStrategy()).thenReturn(FailFastRetryStrategy.INSTANCE);
      channel.write(searchRequest);
      assertChunkedInPipeline(channel);
    }
  }

  /**
   * Helper method to setup the channel with all the needed handlers and switcher.
   *
   * @return the embedded channel to use.
   */
  private EmbeddedChannel setupChannel() {
    EmbeddedChannel channel = new EmbeddedChannel();
    channel.pipeline().addFirst(ChunkedHandlerSwitcher.SWITCHER_IDENTIFIER, new TestChunkedHandlerSwitcher(
      new TestChunkedMessageHandler(), new TestNonChunkedMessageHandler(), ServerSearchRequest.class
    ));

    assertNotNull(channel.pipeline().get(TestChunkedHandlerSwitcher.class));
    assertNull(channel.pipeline().get(ChunkedMessageHandler.class));
    assertNull(channel.pipeline().get(NonChunkedHttpMessageHandler.class));

    channel.pipeline().fireChannelActive();

    return channel;
  }

  /**
   * Asserts the channel is in the right state for chunked requests.
   *
   * @param channel the channel on which to check the pipeline.
   */
  private void assertChunkedInPipeline(final EmbeddedChannel channel) {
    assertNotNull(channel.pipeline().get(TestChunkedHandlerSwitcher.class));
    assertNotNull(channel.pipeline().get(ChunkedMessageHandler.class));
    assertNull(channel.pipeline().get(NonChunkedHttpMessageHandler.class));
    assertNull(channel.pipeline().get(HttpObjectAggregator.class));
  }

  /**
   * Asserts the channel is in the right state for non-chunked requests.
   *
   * @param channel the channel on which to check the pipeline.
   */
  private void assertNonChunkedInPipeline(final EmbeddedChannel channel) {
    assertNotNull(channel.pipeline().get(TestChunkedHandlerSwitcher.class));
    assertNull(channel.pipeline().get(ChunkedMessageHandler.class));
    assertNotNull(channel.pipeline().get(NonChunkedHttpMessageHandler.class));
    assertNotNull(channel.pipeline().get(HttpObjectAggregator.class));
  }


  class TestChunkedHandlerSwitcher extends ChunkedHandlerSwitcher {

    TestChunkedHandlerSwitcher(TestChunkedMessageHandler chunkedHandler, TestNonChunkedMessageHandler nonChunkedHandler,
                               Class<? extends Request> chunkedClass) {
      super(chunkedHandler, nonChunkedHandler, chunkedClass);
    }

  }

  class TestChunkedMessageHandler extends ChunkedMessageHandler<
    ChunkHeader,
    ChunkRow,
    ChunkTrailer,
    ChunkedResponse<ChunkHeader, ChunkRow, ChunkTrailer>,
    HttpRequest<ChunkHeader, ChunkRow, ChunkTrailer, ChunkedResponse<ChunkHeader, ChunkRow, ChunkTrailer>>> {

    @SuppressWarnings({"unchecked"})
    TestChunkedMessageHandler() {
      super(endpoint, endpointContext, mock(ChunkResponseParser.class));
    }
  }

  class TestNonChunkedMessageHandler extends NonChunkedHttpMessageHandler {
    TestNonChunkedMessageHandler() {
      super(endpoint, ServiceType.SEARCH);
    }

    @Override
    protected Exception failRequestWith(HttpResponseStatus status, String content, NonChunkedHttpRequest<Response> request) {
      return new Exception(content);
    }
  }

}
