/*
 * Copyright (c) 2016 Couchbase, Inc.
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

package com.couchbase.client.core.config.refresher;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.config.ConfigurationProvider;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.DefaultHttpResponse;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpResponse;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpResponseStatus;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpVersion;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.msg.manager.BucketConfigStreamingRequest;
import com.couchbase.client.core.msg.manager.BucketConfigStreamingResponse;
import com.couchbase.client.util.SimpleEventBus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.couchbase.client.test.Util.waitUntilCondition;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies the functionality of the {@link ClusterManagerBucketRefresher}.
 */
class ClusterManagerBucketRefresherTest {

  private CoreEnvironment env;
  private Core core;
  private ClusterManagerBucketRefresher refresher;

  @BeforeEach
  void beforeEach() {
    SimpleEventBus eventBus = new SimpleEventBus(true);
    env = CoreEnvironment.builder("username", "password").eventBus(eventBus).build();

    CoreContext coreContext = mock(CoreContext.class);
    core = mock(Core.class);
    when(core.context()).thenReturn(coreContext);
    when(coreContext.environment()).thenReturn(env);
    ConfigurationProvider provider = mock(ConfigurationProvider.class);
    refresher = new ClusterManagerBucketRefresher(provider, core);
  }

  @AfterEach
  void afterEach() {
    refresher.shutdown().block();
    env.shutdown();
  }

  /**
   * Unsubscription is driven purely from up the stack, so if the config stream should close for some
   * reason the refresher needs to try to establish a new connection.
   */
  @Test
  void shouldReconnectIfStreamCloses() {
    final AtomicReference<BucketConfigStreamingResponse> responseRef = new AtomicReference<>();
    final AtomicInteger streamingRequestAttempts = new AtomicInteger();
    doAnswer(i -> {
      streamingRequestAttempts.incrementAndGet();
      BucketConfigStreamingRequest request = i.getArgument(0);
      HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
      BucketConfigStreamingResponse response = request.decode(httpResponse, null);
      responseRef.set(response);
      request.succeed(response);
      return null;
    }).when(core).send(any(BucketConfigStreamingRequest.class));

    refresher.register("bucketName").block();

    // Let's pretend the successfully opened stream closes for whatever reason
    responseRef.get().completeStream();

    waitUntilCondition(() -> streamingRequestAttempts.get() >= 2);
  }

  /**
   * Unsubscription is driven purely from up the stack, so if the config stream should error for some
   * reason the refresher needs to try to establish a new connection.
   */
  @Test
  void shouldReconnectIfStreamErrors() {
    final AtomicReference<BucketConfigStreamingResponse> responseRef = new AtomicReference<>();
    final AtomicInteger streamingRequestAttempts = new AtomicInteger();
    doAnswer(i -> {
      streamingRequestAttempts.incrementAndGet();
      BucketConfigStreamingRequest request = i.getArgument(0);
      HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
      BucketConfigStreamingResponse response = request.decode(httpResponse, null);
      responseRef.set(response);
      request.succeed(response);
      return null;
    }).when(core).send(any(BucketConfigStreamingRequest.class));

    refresher.register("bucketName").block();

    // Let's pretend the successfully opened stream fails for whatever reason
    responseRef.get().failStream(new RuntimeException("Something Happened"));

    waitUntilCondition(() -> streamingRequestAttempts.get() >= 2);
  }

  /**
   * If a connection cannot be established, the client should keep trying until it finds one.
   */
  @Test
  void shouldKeepTryingIfHttpResponseHasNonSuccessStatus() {
    final AtomicInteger streamingRequestAttempts = new AtomicInteger();
    doAnswer(i -> {
      streamingRequestAttempts.incrementAndGet();
      BucketConfigStreamingRequest request = i.getArgument(0);
      HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR);
      BucketConfigStreamingResponse response = request.decode(httpResponse, null);
      request.succeed(response);
      return null;
    }).when(core).send(any(BucketConfigStreamingRequest.class));

    refresher.register("bucketName").block();

    waitUntilCondition(() -> streamingRequestAttempts.get() >= 2);
  }

  @Test
  void shouldKeepTryingIfRequestFailsCompletely() {
    final AtomicInteger streamingRequestAttempts = new AtomicInteger();
    doAnswer(i -> {
      streamingRequestAttempts.incrementAndGet();
      BucketConfigStreamingRequest request = i.getArgument(0);
      request.fail(new RuntimeException());
      return null;
    }).when(core).send(any(BucketConfigStreamingRequest.class));

    refresher.register("bucketName").block();

    waitUntilCondition(() -> streamingRequestAttempts.get() >= 2);
  }

}
