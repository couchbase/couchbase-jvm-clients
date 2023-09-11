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
import com.couchbase.client.core.env.PasswordAuthenticator;
import com.couchbase.client.core.error.RequestCanceledException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.CollectionMap;
import com.couchbase.client.core.msg.CancellationReason;
import com.couchbase.client.core.msg.kv.GetRequest;
import com.couchbase.client.core.retry.FailFastRetryStrategy;
import com.couchbase.client.core.retry.RetryReason;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.channel.embedded.EmbeddedChannel;
import com.couchbase.client.core.util.HostAndPort;
import com.couchbase.client.core.cnc.SimpleEventBus;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies the basic functionality of the {@link KeyValueMessageHandler}.
 */
class KeyValueMessageHandlerTest {

  static {
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
  }

  private static CoreEnvironment ENV;
  private static EndpointContext CTX;
  private static final String BUCKET = "bucket";
  private static final CollectionIdentifier CID = CollectionIdentifier.fromDefault(BUCKET);

  @BeforeAll
  static void setup() {
    ENV = CoreEnvironment.builder().eventBus(new SimpleEventBus(true)).build();
    Core core = mock(Core.class);
    CoreContext coreContext = new CoreContext(core, 1, ENV, PasswordAuthenticator.create("foo", "bar"));
    ConfigurationProvider configurationProvider = mock(ConfigurationProvider.class);
    when(configurationProvider.collectionMap()).thenReturn(new CollectionMap());
    when(core.configurationProvider()).thenReturn(configurationProvider);
    CTX = new EndpointContext(coreContext, new HostAndPort("127.0.0.1", 1234),
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
        CTX, CID, null, null));
      channel.flushOutbound();

      ByteBuf request = channel.readOutbound();
      int firstOpaque = MemcacheProtocol.opaque(request);
      assertTrue(firstOpaque > 0);
      ReferenceCountUtil.release(request);

      channel.writeOutbound(new GetRequest("key", Duration.ofSeconds(1),
        CTX, CID, null, null));
      channel.flushOutbound();

      request = channel.readOutbound();
      int secondOpaque = MemcacheProtocol.opaque(request);
      assertTrue(secondOpaque > firstOpaque);
      ReferenceCountUtil.release(request);
    } finally {
      channel.finishAndReleaseAll();
    }

  }

  /**
   * If an unknown response code is returned and the consulted error map indicates a retry, it should be passed to
   * the retry orchestrator for correct handling.
   */
  @Test
  void attemptsRetryIfInstructedByErrorMap() {
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
      GetRequest request = new GetRequest("key", Duration.ofSeconds(1), CTX, CID, FailFastRetryStrategy.INSTANCE, null);
      channel.writeOutbound(request);

      ByteBuf getResponse = MemcacheProtocol.response(channel.alloc(), MemcacheProtocol.Opcode.GET, (byte) 0,
        (short) 0xFF, request.opaque(), 0, Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER);
      channel.writeInbound(getResponse);

      ExecutionException exception = assertThrows(ExecutionException.class, () -> request.response().get());
      assertInstanceOf(RequestCanceledException.class, exception.getCause());
      assertEquals("NO_MORE_RETRIES", request.cancellationReason().identifier());
      assertEquals(RetryReason.KV_ERROR_MAP_INDICATED, request.cancellationReason().innerReason());
      assertEquals(0, getResponse.refCnt());
    } finally {
      channel.finishAndReleaseAll();
    }
  }

  /**
   * If a response is received with an invalid opaque that the channel knows nothing about, it should be
   * closed to bring it back to a valid state eventually.
   */
  @Test
  void closesChannelWithInvalidOpaque() {
    EmbeddedChannel channel = new EmbeddedChannel(new KeyValueMessageHandler(null, CTX, Optional.of(BUCKET)));

    try {
      GetRequest request1 = new GetRequest("key", Duration.ofSeconds(1), CTX, CID, FailFastRetryStrategy.INSTANCE, null);
      GetRequest request2 = new GetRequest("key", Duration.ofSeconds(1), CTX, CID, FailFastRetryStrategy.INSTANCE, null);
      channel.writeOutbound(request1, request2);

      assertTrue(channel.isOpen());

      ByteBuf getResponse = MemcacheProtocol.response(channel.alloc(), MemcacheProtocol.Opcode.GET, (byte) 0,
        (short) 0xFF, 1234, 0, Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER);
      channel.writeInbound(getResponse);

      assertThrows(ExecutionException.class, () -> request1.response().get());
      assertEquals(RetryReason.CHANNEL_CLOSED_WHILE_IN_FLIGHT, request1.cancellationReason().innerReason());
      assertThrows(ExecutionException.class, () -> request2.response().get());
      assertEquals(RetryReason.CHANNEL_CLOSED_WHILE_IN_FLIGHT, request2.cancellationReason().innerReason());

      assertFalse(channel.isOpen());
      assertEquals(0, getResponse.refCnt());
    } finally {
      channel.finishAndReleaseAll();
    }
  }

  /**
   * As part of the KV error map, certain status codes have been identified as "must close" on the channel
   * to avoid further problems.
   *
   * <p>This test makes sure that on all of those codes, the channel gets closed accordingly.</p>
   */
  @Test
  void closesChannelOnCertainStatusCodes() {
    Set<MemcacheProtocol.Status> closeOnThese = EnumSet.of(
      MemcacheProtocol.Status.INTERNAL_SERVER_ERROR,
      MemcacheProtocol.Status.NO_BUCKET,
      MemcacheProtocol.Status.NOT_INITIALIZED
    );

    for (MemcacheProtocol.Status status : closeOnThese) {
      EmbeddedChannel channel = new EmbeddedChannel(new KeyValueMessageHandler(null, CTX, Optional.of(BUCKET)));
      try {
        GetRequest request1 = new GetRequest("key", Duration.ofSeconds(1), CTX, CID, FailFastRetryStrategy.INSTANCE, null);
        channel.writeOutbound(request1);

        ByteBuf getResponse = MemcacheProtocol.response(channel.alloc(), MemcacheProtocol.Opcode.GET, (byte) 0,
          status.status(), request1.opaque(), 0, Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER);
        channel.writeInbound(getResponse);

        assertFalse(channel.isOpen());
        assertEquals(0, getResponse.refCnt());
      } finally {
        channel.finishAndReleaseAll();
      }
    }
  }

  /**
   * Certain status codes are identified that should be passed to the retry orchestrator rathen than complete
   * the response right away.
   */
  @Test
  void retriesCertainResponseStatusCodes() {
    List<MemcacheProtocol.Status> retryOnThese = Arrays.asList(
      MemcacheProtocol.Status.LOCKED,
      MemcacheProtocol.Status.TEMPORARY_FAILURE,
      MemcacheProtocol.Status.SYNC_WRITE_IN_PROGRESS,
      MemcacheProtocol.Status.SYNC_WRITE_RE_COMMIT_IN_PROGRESS
    );

    List<RetryReason> retryReasons = Arrays.asList(
      RetryReason.KV_LOCKED,
      RetryReason.KV_TEMPORARY_FAILURE,
      RetryReason.KV_SYNC_WRITE_IN_PROGRESS,
      RetryReason.KV_SYNC_WRITE_RE_COMMIT_IN_PROGRESS
    );

    int i = 0;
    for (MemcacheProtocol.Status status : retryOnThese) {
      EmbeddedChannel channel = new EmbeddedChannel(new KeyValueMessageHandler(null, CTX, Optional.of(BUCKET)));
      try {
        GetRequest request = new GetRequest("key", Duration.ofSeconds(1), CTX, CID, FailFastRetryStrategy.INSTANCE, null);
        channel.writeOutbound(request);

        ByteBuf getResponse = MemcacheProtocol.response(channel.alloc(), MemcacheProtocol.Opcode.GET, (byte) 0,
          status.status(), request.opaque(), 0, Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER);
        channel.writeInbound(getResponse);

        assertEquals(CancellationReason.noMoreRetries(retryReasons.get(i)), request.cancellationReason());
        assertEquals(0, getResponse.refCnt());
        i++;
      } finally {
        channel.finishAndReleaseAll();
      }
    }
  }

  @Test
  void incrementsNotMyVbucketIndicator() {
    EmbeddedChannel channel = new EmbeddedChannel(new KeyValueMessageHandler(null, CTX, Optional.of(BUCKET)));

    try {
      GetRequest request = new GetRequest("key", Duration.ofSeconds(1), CTX, CID, FailFastRetryStrategy.INSTANCE, null);
      channel.writeOutbound(request);

      ByteBuf getResponse = MemcacheProtocol.response(channel.alloc(), MemcacheProtocol.Opcode.GET, (byte) 0,
        MemcacheProtocol.Status.NOT_MY_VBUCKET.status(), request.opaque(), 0, Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER);
      channel.writeInbound(getResponse);

      assertEquals(1, request.rejectedWithNotMyVbucket());
    } finally {
      channel.finishAndReleaseAll();
    }
  }

}