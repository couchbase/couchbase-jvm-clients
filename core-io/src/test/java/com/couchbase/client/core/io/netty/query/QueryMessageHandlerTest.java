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
package com.couchbase.client.core.io.netty.query;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.api.query.CoreQueryResult;
import com.couchbase.client.core.classic.query.ClassicCoreQueryOps;
import com.couchbase.client.core.config.ConfigurationProvider;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.deps.io.netty.channel.embedded.EmbeddedChannel;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.DefaultHttpContent;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.DefaultHttpResponse;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.DefaultLastHttpContent;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.FullHttpRequest;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpContent;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpResponse;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpResponseStatus;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpVersion;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.LastHttpContent;
import com.couchbase.client.core.deps.io.netty.util.ReferenceCountUtil;
import com.couchbase.client.core.deps.io.netty.util.ResourceLeakDetector;
import com.couchbase.client.core.endpoint.BaseEndpoint;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.endpoint.NoopCircuitBreaker;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.PasswordAuthenticator;
import com.couchbase.client.core.error.RequestCanceledException;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.msg.CancellationReason;
import com.couchbase.client.core.msg.query.QueryRequest;
import com.couchbase.client.core.msg.query.QueryResponse;
import com.couchbase.client.core.retry.FailFastRetryStrategy;
import com.couchbase.client.core.retry.RetryReason;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.util.HostAndPort;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static com.couchbase.client.core.manager.CoreCollectionQueryIndexManager.toQueryOptions;
import static com.couchbase.client.core.util.MockUtil.mockCore;
import static com.couchbase.client.test.Util.readResource;
import static com.couchbase.client.test.Util.waitUntilCondition;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies the functionality of the {@link QueryMessageHandler}.
 */
class QueryMessageHandlerTest {

  static {
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
  }

  private static CoreEnvironment ENV;
  private static CoreContext CORE_CTX;
  private static EndpointContext ENDPOINT_CTX;

  @BeforeAll
  static void setup() {
    ENV = CoreEnvironment.create();
    CORE_CTX = new CoreContext(mockCore(), 1, ENV, PasswordAuthenticator.create("user", "pass"));
    ENDPOINT_CTX = new EndpointContext(CORE_CTX, new HostAndPort("127.0.0.1", 1234),
      NoopCircuitBreaker.INSTANCE, ServiceType.QUERY, Optional.empty(), Optional.empty(), Optional.empty());
  }

  @AfterAll
  static void teardown() {
    ENV.shutdown();
  }

  /**
   * To avoid accidentally pipelining requests, we need to make sure that the handler only reports completion
   * once the streaming is fully completed, and not just the response completable future initially completed.
   *
   * <p>We send a request, make sure it is encoded correctly and then stream back the chunks, making sure the
   * final completion is only signaled once the last chunk arrived.</p>
   */
  @Test
  void reportsCompletionOnlyOnceStreamingEnds() {
    BaseEndpoint endpoint = mock(BaseEndpoint.class);
    EmbeddedChannel channel = new EmbeddedChannel(new QueryMessageHandler(endpoint, ENDPOINT_CTX));

    byte[] query = "doesn'tmatter".getBytes(StandardCharsets.UTF_8);
    QueryRequest request = new QueryRequest(
      ENV.timeoutConfig().queryTimeout(), CORE_CTX, ENV.retryStrategy(), CORE_CTX.authenticator(), "statement", query, false, null, null, null,
    null, null, false);
    channel.writeAndFlush(request);

    FullHttpRequest encodedRequest = channel.readOutbound();
    assertEquals("doesn'tmatter", encodedRequest.content().toString(StandardCharsets.UTF_8));
    ReferenceCountUtil.release(encodedRequest);

    verify(endpoint, never()).markRequestCompletion();
    assertFalse(request.response().isDone());


    ByteBuf fullResponse = Unpooled.copiedBuffer(
      readResource("success_response.json", QueryMessageHandlerTest.class),
      StandardCharsets.UTF_8
    );

    // send the header back first
    HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);

    channel.writeInbound(response);
    assertFalse(request.response().isDone());

    // send first chunk, should complete the future but not mark the endpoint completion
    HttpContent firstChunk = new DefaultHttpContent(fullResponse.readBytes(500));
    channel.writeInbound(firstChunk);

    assertTrue(request.response().isDone());
    verify(endpoint, never()).markRequestCompletion();

    // send the second chunk, no change
    HttpContent secondChunk = new DefaultHttpContent(fullResponse.readBytes(500));
    channel.writeInbound(secondChunk);

    verify(endpoint, never()).markRequestCompletion();

    // send the last chunk, mark for completion finally
    LastHttpContent lastChunk = new DefaultLastHttpContent(fullResponse.readBytes(fullResponse.readableBytes()));
    channel.writeInbound(lastChunk);

    verify(endpoint, times(1)).markRequestCompletion();

    channel.finishAndReleaseAll();
  }

  /**
   * Once the header has arrived, the request is complete, so a dropped connection can't retry it.
   * The rows and the trailer still need to end, or a subscriber waits forever.
   */
  @Test
  void rowsAndTrailerFailWhenConnectionDropsAfterHeader() throws Exception {
    BaseEndpoint endpoint = mock(BaseEndpoint.class);
    EmbeddedChannel channel = new EmbeddedChannel(new QueryMessageHandler(endpoint, ENDPOINT_CTX));
    try {
      byte[] query = "doesn'tmatter".getBytes(StandardCharsets.UTF_8);
      QueryRequest request = new QueryRequest(
        ENV.timeoutConfig().queryTimeout(), CORE_CTX, FailFastRetryStrategy.INSTANCE, CORE_CTX.authenticator(), "statement", query,
        true, null, null, null, null, null, false
      );
      channel.writeAndFlush(request);
      ReferenceCountUtil.release(channel.readOutbound());

      // The header and the first row, then the connection drops.
      channel.writeInbound(new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK));
      channel.writeInbound(new DefaultHttpContent(Unpooled.copiedBuffer(
        "{\"requestID\":\"1234\",\"signature\":{\"*\":\"*\"},\"results\":[{\"a\":1},", StandardCharsets.UTF_8
      )));
      QueryResponse response = request.response().get(1, TimeUnit.SECONDS);

      List<String> rows = new CopyOnWriteArrayList<>();
      AtomicReference<Throwable> rowsError = new AtomicReference<>();
      CountDownLatch rowsEnded = new CountDownLatch(1);
      response.rows().subscribe(
        row -> rows.add(new String(row.data(), StandardCharsets.UTF_8)),
        e -> {
          rowsError.set(e);
          rowsEnded.countDown();
        },
        rowsEnded::countDown
      );

      channel.close();

      assertTrue(rowsEnded.await(5, TimeUnit.SECONDS), "rows should end when the connection drops; got " + rows + " and no end");
      assertEquals(singletonList("{\"a\":1}"), rows);
      // Fails, rather than completes, since the result is incomplete. (Like the OkHttp services.)
      assertConnectionClosed(rowsError.get());

      assertConnectionClosed(assertThrows(RequestCanceledException.class, () -> response.trailer().block(Duration.ofSeconds(5))));
    } finally {
      channel.finishAndReleaseAll();
    }
  }

  /**
   * Like Cluster.query: the blocking API waits for all the rows and the metadata,
   * so if they never end, the calling thread waits forever.
   */
  @Test
  void blockingQueryFailsWhenConnectionDropsAfterHeader() throws Exception {
    Core core = mockCore(ENV);
    ConfigurationProvider configurationProvider = mock(ConfigurationProvider.class);
    when(configurationProvider.configs()).thenReturn(Flux.never());
    when(core.configurationProvider()).thenReturn(configurationProvider);

    // Instead of dispatching the request to a node, hand it to the test.
    BlockingQueue<QueryRequest> sent = new LinkedBlockingQueue<>();
    doAnswer(invocation -> sent.add(invocation.getArgument(0))).when(core).send(any());

    EmbeddedChannel channel = new EmbeddedChannel(new QueryMessageHandler(mock(BaseEndpoint.class), ENDPOINT_CTX));
    ExecutorService caller = Executors.newSingleThreadExecutor();
    try {
      Future<CoreQueryResult> result = caller.submit(() -> new ClassicCoreQueryOps(core).queryBlocking(
        "SELECT 1",
        toQueryOptions(CoreCommonOptions.DEFAULT, true, Mapper.createObjectNode()),
        null,
        null,
        null
      ));

      QueryRequest request = sent.poll(10, TimeUnit.SECONDS);
      assertNotNull(request, "query wasn't sent");
      channel.writeAndFlush(request);
      ReferenceCountUtil.release(channel.readOutbound());

      // The header and the first row, then the connection drops.
      channel.writeInbound(new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK));
      channel.writeInbound(new DefaultHttpContent(Unpooled.copiedBuffer(
        "{\"requestID\":\"1234\",\"signature\":{\"*\":\"*\"},\"results\":[{\"a\":1},", StandardCharsets.UTF_8
      )));
      channel.close();

      try {
        result.get(10, TimeUnit.SECONDS);
        fail("blocking query should fail, since the result is incomplete");
      } catch (TimeoutException e) {
        fail("blocking query is still waiting for the rest of the result, after the connection closed");
      } catch (ExecutionException e) {
        assertConnectionClosed(e.getCause());
      }
    } finally {
      caller.shutdownNow();
      channel.finishAndReleaseAll();
    }
  }

  /**
   * Like a request that can't be retried after its connection closes.
   */
  private static void assertConnectionClosed(Throwable t) {
    RequestCanceledException e = assertInstanceOf(RequestCanceledException.class, t);
    assertEquals(CancellationReason.noMoreRetries(RetryReason.CHANNEL_CLOSED_WHILE_IN_FLIGHT), e.reason());
  }

  @Test
  void handlesConcurrentInFlightRequest() {
    BaseEndpoint endpoint = mock(BaseEndpoint.class);
    EmbeddedChannel channel = new EmbeddedChannel(new QueryMessageHandler(endpoint, ENDPOINT_CTX));

    byte[] query = "doesn'tmatter".getBytes(StandardCharsets.UTF_8);
    QueryRequest request1 = new QueryRequest(
      ENV.timeoutConfig().queryTimeout(), CORE_CTX, FailFastRetryStrategy.INSTANCE, CORE_CTX.authenticator(), "statement", query,
      true, null, null, null, null, null, false
    );
    QueryRequest request2 = new QueryRequest(
      ENV.timeoutConfig().queryTimeout(), CORE_CTX, FailFastRetryStrategy.INSTANCE, CORE_CTX.authenticator(), "statement", query,
      true, null, null, null, null, null, false
    );
    channel.writeAndFlush(request1);
    channel.writeAndFlush(request2);

    waitUntilCondition(() -> request2.context().retryAttempts() > 0);
    verify(endpoint, times(1)).decrementOutstandingRequests();
    channel.finishAndReleaseAll();
  }

}
