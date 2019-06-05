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
import com.couchbase.client.core.deps.io.netty.util.ReferenceCountUtil;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.CollectionMap;
import com.couchbase.client.core.msg.kv.GetRequest;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class KeyValueMessageHandlerTest {

  private static CoreEnvironment ENV;
  private static EndpointContext CTX;
  private static String BUCKET = "bucket";
  private static CollectionIdentifier CID = CollectionIdentifier.fromDefault(BUCKET);

  @BeforeAll
  private static void setup() {
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
  private static void teardown() {
    ENV.shutdown();
  }

  /**
   * This test sends two get requests and makes sure the latter one has a higher opaque then the
   * former one.
   */
  @Test
  void opaqueIsIncreasing() {
    EmbeddedChannel channel = new EmbeddedChannel(new KeyValueMessageHandler(null, CTX, BUCKET));

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
    EmbeddedChannel channel1 = new EmbeddedChannel(new KeyValueMessageHandler(null, CTX, BUCKET));
    EmbeddedChannel channel2 = new EmbeddedChannel(new KeyValueMessageHandler(null, CTX, BUCKET));

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

}