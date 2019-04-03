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

import com.couchbase.client.core.deps.io.netty.channel.embedded.EmbeddedChannel;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpObjectAggregator;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.service.ServiceType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class NonChunkedHttpMessageHandlerTest {

  private EmbeddedChannel channel;
  private EndpointContext endpointContext;
  private CoreEnvironment env;

  @BeforeEach
  private void setup() {
    env = CoreEnvironment.create("user", "pass");
    endpointContext = mock(EndpointContext.class);
    when(endpointContext.environment()).thenReturn(env);
    channel = new EmbeddedChannel();
  }

  @AfterEach
  private void teardown() {
    channel.finishAndReleaseAll();
    env.shutdown();
  }

  @Test
  void addsAggregatorWhenAdded() {
    assertNull(channel.pipeline().get(TestNonChunkedHttpMessageHandler.class));
    assertNull(channel.pipeline().get(HttpObjectAggregator.class));

    channel.pipeline().addFirst(
      NonChunkedHttpMessageHandler.IDENTIFIER,
      new TestNonChunkedHttpMessageHandler(endpointContext)
    );

    assertNotNull(channel.pipeline().get(TestNonChunkedHttpMessageHandler.class));
    assertNotNull(channel.pipeline().get(HttpObjectAggregator.class));
  }

  @Test
  void removesAggregatorWhenRemoved() {
    channel.pipeline().addFirst(
      NonChunkedHttpMessageHandler.IDENTIFIER,
      new TestNonChunkedHttpMessageHandler(endpointContext)
    );

    assertNotNull(channel.pipeline().get(TestNonChunkedHttpMessageHandler.class));
    assertNotNull(channel.pipeline().get(HttpObjectAggregator.class));

    channel.pipeline().remove(NonChunkedHttpMessageHandler.IDENTIFIER);

    assertNull(channel.pipeline().get(TestNonChunkedHttpMessageHandler.class));
    assertNull(channel.pipeline().get(HttpObjectAggregator.class));
  }

  class TestNonChunkedHttpMessageHandler extends NonChunkedHttpMessageHandler {

    TestNonChunkedHttpMessageHandler(EndpointContext ctx) {
      super(ctx, ServiceType.SEARCH);
    }

    @Override
    protected Exception failRequestWith(String content) {
      return new Exception(content);
    }

  }

}