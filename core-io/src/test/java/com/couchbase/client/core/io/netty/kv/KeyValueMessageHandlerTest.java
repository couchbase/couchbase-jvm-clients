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
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.msg.kv.GetRequest;
import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

class KeyValueMessageHandlerTest {

  private static CoreEnvironment ENV;
  private static CoreContext CTX;
  private static String BUCKET = "bucket";

  @BeforeAll
  private static void setup() {
    ENV = CoreEnvironment.create("foo", "bar");
    CTX = new CoreContext(mock(Core.class), 1, ENV);
  }

  @AfterAll
  private static void teardown() {
    ENV.shutdown(Duration.ofSeconds(1));
  }

  /**
   * This test sends two get requests and makes sure the latter one has a higher opaque then the
   * former one.
   */
  @Test
  void opaqueIsIncreasing() {
    EmbeddedChannel channel = new EmbeddedChannel(new KeyValueMessageHandler(CTX, BUCKET));

    try {
      channel.writeOutbound(new GetRequest("key", null, Duration.ofSeconds(1),
        CTX, BUCKET, null));
      channel.flushOutbound();

      ByteBuf request = channel.readOutbound();
      assertEquals(1, MemcacheProtocol.opaque(request));

      channel.writeOutbound(new GetRequest("key", null, Duration.ofSeconds(1),
        CTX, BUCKET, null));
      channel.flushOutbound();

      request = channel.readOutbound();
      assertEquals(2, MemcacheProtocol.opaque(request));
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
    EmbeddedChannel channel1 = new EmbeddedChannel(new KeyValueMessageHandler(CTX, BUCKET));
    EmbeddedChannel channel2 = new EmbeddedChannel(new KeyValueMessageHandler(CTX, BUCKET));

    try {
      channel1.writeOutbound(new GetRequest("key", null, Duration.ofSeconds(1),
        CTX, BUCKET, null));
      channel1.flushOutbound();

      ByteBuf request = channel1.readOutbound();
      assertEquals(1, MemcacheProtocol.opaque(request));

      channel2.writeOutbound(new GetRequest("key", null, Duration.ofSeconds(1),
        CTX, BUCKET, null));
      channel2.flushOutbound();

      request = channel2.readOutbound();
      assertEquals(1, MemcacheProtocol.opaque(request));
    } finally {
      channel1.finishAndReleaseAll();
      channel2.finishAndReleaseAll();
    }
  }

}