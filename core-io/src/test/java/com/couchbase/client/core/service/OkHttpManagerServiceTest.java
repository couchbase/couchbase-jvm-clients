/*
 * Copyright 2026 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.service;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.endpoint.http.CoreHttpPath;
import com.couchbase.client.core.endpoint.http.CoreHttpRequest;
import com.couchbase.client.core.endpoint.http.CoreHttpResponse;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.error.RateLimitedException;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.msg.RequestTarget;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.manager.BucketConfigRequest;
import com.couchbase.client.core.msg.manager.BucketConfigResponse;
import com.couchbase.client.core.msg.manager.BucketConfigStreamingRequest;
import com.couchbase.client.core.msg.manager.BucketConfigStreamingResponse;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.core.service.TestHttpServer.RecordedRequest;
import com.couchbase.client.core.util.HostAndPort;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static com.couchbase.client.core.service.OkHttpTestSupport.newTestOkHttpClient;
import static com.couchbase.client.core.util.MockUtil.mockCore;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

/**
 * Sends manager requests through {@link OkHttpManagerService} to a real HTTP server,
 * and checks the request it sends and how it handles the response.
 */
class OkHttpManagerServiceTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(10);

  private CoreEnvironment env;
  private TestHttpServer server;
  private CouchbaseOkHttpClient okHttpClient;
  private Core core;
  private OkHttpManagerService service;

  private void start(Consumer<CoreEnvironment.Builder<?>> envCustomizer) throws IOException {
    CoreEnvironment.Builder<?> envBuilder = CoreEnvironment.builder();
    envCustomizer.accept(envBuilder);
    env = envBuilder.build();

    server = TestHttpServer.startHttp();

    okHttpClient = newTestOkHttpClient(env);

    core = mockCore(env);
    when(core.okHttpClient()).thenReturn(okHttpClient);

    service = new OkHttpManagerService(core.context(), new HostAndPort(TestHttpServer.HOST_NAME, server.port()));
  }

  private void start() throws IOException {
    start(builder -> {
    });
  }

  @AfterEach
  void teardown() {
    try {
      if (okHttpClient != null) okHttpClient.close();
    } finally {
      try {
        if (server != null) server.close();
      } finally {
        if (env != null) env.shutdown();
      }
    }
  }

  private BucketConfigRequest newBucketConfigRequest() {
    return new BucketConfigRequest(TIMEOUT, core.context(), BestEffortRetryStrategy.INSTANCE, "my-bucket", null);
  }

  private BucketConfigStreamingRequest newStreamingRequest() {
    return new BucketConfigStreamingRequest(TIMEOUT, core.context(), BestEffortRetryStrategy.INSTANCE, "my-bucket");
  }

  private <T extends Response> T send(Request<T> request) throws Exception {
    service.send(request);
    return request.response().get(TIMEOUT.toMillis() * 2, TimeUnit.MILLISECONDS);
  }

  @Test
  void bucketConfig() throws Exception {
    start();
    server.enqueue("{\"rev\":1}");

    BucketConfigResponse response = send(newBucketConfigRequest());

    RecordedRequest sent = server.requests().get(0);
    assertEquals("GET", sent.method);
    assertEquals("/pools/default/b/my-bucket", sent.path);
    assertTrue(sent.header("Authorization").startsWith("Basic "), "should authenticate");
    assertEquals("test-user-agent", sent.header("User-Agent"));

    assertEquals(ResponseStatus.SUCCESS, response.status());
    assertEquals("{\"rev\":1}", new String(response.config(), UTF_8));
  }

  @Test
  void errorStatusCompletesWithResponse() throws Exception {
    // Like the Netty implementation: the caller checks the status.
    start();
    server.enqueue(404, "Requested resource not found.", Duration.ZERO);

    BucketConfigResponse response = send(newBucketConfigRequest());
    assertEquals(ResponseStatus.NOT_FOUND, response.status());
    assertEquals("Requested resource not found.", new String(response.config(), UTF_8));
  }

  private CoreHttpRequest newCoreHttpRequest(boolean failOnErrorStatus) {
    return CoreHttpRequest.builder(CoreCommonOptions.DEFAULT, core.context(), HttpMethod.GET, CoreHttpPath.path("/pools?x=1"), RequestTarget.manager())
      .failOnErrorStatus(failOnErrorStatus)
      .build();
  }

  @Test
  void coreHttpRequestFailsOnErrorStatusByDefault() throws Exception {
    start();
    server.enqueue(429, "num_concurrent_requests exceeded", Duration.ZERO);

    CoreHttpRequest request = newCoreHttpRequest(true);
    service.send(request);
    ExecutionException e = assertThrows(ExecutionException.class, () -> request.response().get(TIMEOUT.toMillis() * 2, TimeUnit.MILLISECONDS));
    assertInstanceOf(RateLimitedException.class, e.getCause()); // translated by NonChunkedManagerMessageHandler
  }

  @Test
  void coreHttpRequestCanCompleteWithErrorStatus() throws Exception {
    // Like the removed GenericManagerRequest: the caller checks the status.
    start();
    server.enqueue(404, "Requested resource not found.", Duration.ZERO);

    CoreHttpResponse response = send(newCoreHttpRequest(false));

    assertEquals("/pools?x=1", server.requests().get(0).path);
    assertEquals(404, response.httpStatus());
    assertEquals(ResponseStatus.NOT_FOUND, response.status());
    assertEquals("Requested resource not found.", new String(response.content(), UTF_8));
  }

  @Test
  void configStream() throws Exception {
    start();
    // Two whole configs, then a partial one that's discarded when the stream ends (like Netty).
    server.enqueue("{\"rev\":1}\n\n\n\n  {\"rev\":2}  \n\n\n\n{\"rev\":3");

    BucketConfigStreamingResponse response = send(newStreamingRequest());

    RecordedRequest sent = server.requests().get(0);
    assertEquals("GET", sent.method);
    assertEquals("/pools/default/bs/my-bucket", sent.path);
    assertTrue(sent.header("Authorization").startsWith("Basic "), "should authenticate");

    assertEquals(ResponseStatus.SUCCESS, response.status());
    assertEquals(TestHttpServer.HOST_NAME, response.address());
    // The response replays only the latest config to a new subscriber (by design), so depending on timing,
    // we might not see the first config. Either way, the stream ends with the second config, trimmed.
    List<String> configs = response.configs().collectList().block(Duration.ofSeconds(30));
    assertTrue(configs.equals(asList("{\"rev\":1}", "{\"rev\":2}")) || configs.equals(singletonList("{\"rev\":2}")), String.valueOf(configs));
  }

  @Test
  void idleConfigStreamIsClosed() throws Exception {
    start(env -> env.ioConfig(io -> io.configIdleRedialTimeout(Duration.ofMillis(500))));
    // Sends one config, then goes quiet for much longer than the idle timeout.
    server.enqueueStreaming("{\"rev\":1}\n\n\n\n", Duration.ofSeconds(60));

    BucketConfigStreamingResponse response = send(newStreamingRequest());

    // Completes (so the refresher can redial) well before the server would have closed the stream.
    List<String> configs = response.configs().collectList().block(Duration.ofSeconds(20));
    assertEquals(singletonList("{\"rev\":1}"), configs);
  }

  @Test
  void configStreamErrorStatusCompletesWithResponse() throws Exception {
    start();
    server.enqueue(404, "Requested resource not found.", Duration.ZERO);

    BucketConfigStreamingResponse response = send(newStreamingRequest());
    assertEquals(ResponseStatus.NOT_FOUND, response.status());
    assertTrue(response.configs().collectList().block(Duration.ofSeconds(30)).isEmpty());
  }
}
