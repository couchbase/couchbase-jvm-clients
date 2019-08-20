/*
 * Copyright (c) 2018 Couchbase, Inc.
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

package com.couchbase.client.core.io.netty.kv;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.config.ConfigurationProvider;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.deps.io.netty.util.ReferenceCountUtil;
import com.couchbase.client.core.deps.io.netty.util.ResourceLeakDetector;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.error.RequestCanceledException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.CollectionMap;
import com.couchbase.client.core.msg.kv.GetRequest;
import com.couchbase.client.core.msg.kv.GetResponse;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.core.retry.FailFastRetryStrategy;
import com.couchbase.client.core.retry.RetryReason;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class KeyValueMessageHandlerTest {

  static {
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
  }

  private static CoreEnvironment ENV;
  private static EndpointContext CTX;
  private static String BUCKET = "bucket";
  private static CollectionIdentifier CID = CollectionIdentifier.fromDefault(BUCKET);

  @BeforeAll
  static void setup() {
    ENV = CoreEnvironment.create("foo", "bar");
    Core core = mock(Core.class);
    CoreContext coreContext = new CoreContext(core, 1, ENV);
    ConfigurationProvider configurationProvider = mock(ConfigurationProvider.class);
    when(configurationProvider.collectionMap()).thenReturn(new CollectionMap());
    when(core.configurationProvider()).thenReturn(configurationProvider);
    CTX = new EndpointContext(coreContext, "127.0.0.1", 1234,
      null, ServiceType.KV, Optional.empty(), Optional.empty(), Optional.empty());
  }

  @AfterAll
  static void teardown() {
    ENV.shutdown();
  }

  /**
   * This test sends two get requests and makes sure the latter one has a higher opaque then the
   * former one.
   */
  @Test
  void opaqueIsIncreasing() {
    EmbeddedChannel channel = new EmbeddedChannel(new KeyValueMessageHandler(null, CTX, Optional.of(BUCKET)));

    try {
      channel.writeOutbound(new GetRequest("key", Duration.ofSeconds(1),
        CTX, CID, null));
      channel.flushOutbound();

      ByteBuf request = channel.readOutbound();
      assertEquals(1, MemcacheProtocol.opaque(request));
      ReferenceCountUtil.release(request);

      channel.writeOutbound(new GetRequest("key", Duration.ofSeconds(1),
        CTX, CID, null));
      channel.flushOutbound();

      request = channel.readOutbound();
      assertEquals(2, MemcacheProtocol.opaque(request));
      ReferenceCountUtil.release(request);
    } finally {
      channel.finishAndReleaseAll();
    }

  }

  /**
   * To make it easier to compare full streams of requests and responses, the opaque is per
   * channel and not global.
   */
  @Test
  void opaqueIsPerChannel() {
    EmbeddedChannel channel1 = new EmbeddedChannel(new KeyValueMessageHandler(null, CTX, Optional.of(BUCKET)));
    EmbeddedChannel channel2 = new EmbeddedChannel(new KeyValueMessageHandler(null, CTX, Optional.of(BUCKET)));

    try {
      channel1.writeOutbound(new GetRequest("key", Duration.ofSeconds(1),
        CTX, CID, null));
      channel1.flushOutbound();

      ByteBuf request = channel1.readOutbound();
      assertEquals(1, MemcacheProtocol.opaque(request));
      ReferenceCountUtil.release(request);

      channel2.writeOutbound(new GetRequest("key", Duration.ofSeconds(1),
        CTX, CID, null));
      channel2.flushOutbound();

      request = channel2.readOutbound();
      assertEquals(1, MemcacheProtocol.opaque(request));
      ReferenceCountUtil.release(request);
    } finally {
      channel1.finishAndReleaseAll();
      channel2.finishAndReleaseAll();
    }
  }

  /**
   * If an unknown response code is returned and the consulted error map indicates a retry, it should be passed to
   * the retry orchestrator for correct handling.
   */
  @Test
  void shouldAttemptRetryIfInstructedByErrorMap() {
    EmbeddedChannel channel = new EmbeddedChannel(new KeyValueMessageHandler(null, CTX, Optional.of(BUCKET)));

    ErrorMap errorMap = mock(ErrorMap.class);
    Map<Short, ErrorMap.ErrorCode> errors = new HashMap<>();
    ErrorMap.ErrorCode code = mock(ErrorMap.ErrorCode.class);
    errors.put((short) 0xFF, code);
    Set<ErrorMap.ErrorAttribute> attributes = new HashSet<>();
    attributes.add(ErrorMap.ErrorAttribute.RETRY_NOW);
    when(code.attributes()).thenReturn(attributes);
    when(errorMap.errors()).thenReturn(errors);
    channel.attr(ChannelAttributes.ERROR_MAP_KEY).set(errorMap);

    channel.pipeline().fireChannelActive();

    try {
      GetRequest request = new GetRequest("key", Duration.ofSeconds(1), CTX, CID, FailFastRetryStrategy.INSTANCE);
      channel.writeOutbound(request);

      ByteBuf getResponse = MemcacheProtocol.response(channel.alloc(), MemcacheProtocol.Opcode.GET, (byte) 0,
        (short) 0xFF, 1, 0, Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER);
      channel.writeInbound(getResponse);

      RequestCanceledException reason = assertThrows(RequestCanceledException.class, () -> request.response().get());
      assertEquals("NO_MORE_RETRIES", request.cancellationReason().identifier());
      assertEquals(RetryReason.KV_ERROR_MAP_INDICATED, request.cancellationReason().innerReason());
    } finally {
      channel.finishAndReleaseAll();
    }
  }

}