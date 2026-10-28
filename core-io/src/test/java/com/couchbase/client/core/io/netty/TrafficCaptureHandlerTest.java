/*
 * Copyright (c) 2026 Couchbase, Inc.
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

import com.couchbase.client.core.cnc.Event;
import com.couchbase.client.core.cnc.SimpleEventBus;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.deps.io.netty.channel.embedded.EmbeddedChannel;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.logging.LogRedaction;
import com.couchbase.client.core.logging.RedactionLevel;
import com.couchbase.client.core.service.ServiceType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.Opcode.GET;
import static com.couchbase.client.core.io.netty.kv.ProtocolVerifier.decodeHexDump;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.Opcode.SASL_AUTH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TrafficCaptureHandlerTest {

  private static final String PASSWORD = "s3cretPassw0rd";

  private RedactionLevel origRedactionLevel;
  private SimpleEventBus eventBus;

  @BeforeEach
  void setUp() {
    origRedactionLevel = LogRedaction.getRedactionLevel();
    eventBus = new SimpleEventBus(true);
  }

  @AfterEach
  void tearDown() {
    LogRedaction.setRedactionLevel(origRedactionLevel);
  }

  @Test
  void masksSaslPlainBody() {
    ByteBuf body = Unpooled.copiedBuffer("\0Administrator\0" + PASSWORD, StandardCharsets.UTF_8);
    ByteBuf request = MemcacheProtocol.request(
      Unpooled.buffer().alloc(), SASL_AUTH, (byte) 0, (short) 0, 1, 0,
      Unpooled.EMPTY_BUFFER, Unpooled.copiedBuffer("PLAIN", StandardCharsets.UTF_8), body
    );
    int totalBodyLength = MemcacheProtocol.totalBodyLength(request);

    String captured = captureWrite(ServiceType.KV, request);

    assertFalse(captured.contains(PASSWORD));
    assertFalse(captured.contains("|00000000|"), "SASL packet must not be hex dumped");
    assertFalse(captured.contains("Administrator"));
    assertFalse(captured.contains(String.format("%08X", totalBodyLength)), "body length must not be logged");
    assertFalse(captured.contains("(" + totalBodyLength + ")"), "body length must not be logged");
    assertTrue(captured.contains(">> Mechanism: PLAIN"));
    assertTrue(captured.contains(">> Body: <masked>"));
    assertTrue(captured.contains("SASL_AUTH"));
  }

  @Test
  void redactsDocumentKeyAndHexDump() {
    LogRedaction.setRedactionLevel(RedactionLevel.PARTIAL);

    ByteBuf request = MemcacheProtocol.request(
      Unpooled.buffer().alloc(), GET, (byte) 0, (short) 0, 1, 0,
      Unpooled.EMPTY_BUFFER, Unpooled.copiedBuffer("\u0000my-doc", StandardCharsets.UTF_8), Unpooled.EMPTY_BUFFER // default collection
    );

    String captured = captureWrite(ServiceType.KV, request);

    assertTrue(captured.contains(">> Key: <ud>my-doc</ud> (Collection ID: 0x0)"));
    assertTrue(captured.startsWith("\n<ud>"), "hex dump should be tagged as user data");
    assertTrue(captured.contains("Total Body Length  | 8-11   | 0x00000007 (7)"));
  }

  @Test
  void masksHttpAuthorizationHeader() {
    String credential = Base64.getEncoder().encodeToString(("Administrator:" + PASSWORD).getBytes(StandardCharsets.UTF_8));
    String request = "GET /pools HTTP/1.1\r\n"
      + "Host: localhost:8091\r\n"
      + "authorization: Basic " + credential + "\r\n"
      + "Accept: */*\r\n"
      + "\r\n"
      + "body";
    ByteBuf buf = Unpooled.copiedBuffer(request, StandardCharsets.ISO_8859_1);

    EmbeddedChannel channel = newChannel(ServiceType.MANAGER);
    try {
      channel.writeOutbound(buf);
      ByteBuf written = channel.readOutbound();
      try {
        assertEquals(request, written.toString(StandardCharsets.ISO_8859_1), "wire bytes must be unchanged");
      } finally {
        written.release();
      }
    } finally {
      channel.finishAndReleaseAll();
    }

    String captured = decode(lastEventDescription());
    assertFalse(captured.contains(credential));
    assertEquals(request.replace("Basic " + credential, "Basic <masked>"), captured);
  }

  private String captureWrite(ServiceType serviceType, ByteBuf msg) {
    EmbeddedChannel channel = newChannel(serviceType);
    try {
      channel.writeOutbound(msg);
    } finally {
      channel.finishAndReleaseAll();
    }
    return lastEventDescription();
  }

  private EmbeddedChannel newChannel(ServiceType serviceType) {
    CoreEnvironment env = mock(CoreEnvironment.class);
    when(env.eventBus()).thenReturn(eventBus);
    EndpointContext ctx = mock(EndpointContext.class);
    when(ctx.environment()).thenReturn(env);
    when(ctx.serviceType()).thenReturn(serviceType);
    return new EmbeddedChannel(new TrafficCaptureHandler(ctx));
  }

  private String lastEventDescription() {
    Event event = eventBus.publishedEvents().get(eventBus.publishedEvents().size() - 1);
    return event.description();
  }

  /**
   * Converts a hex dump back to the string it represents.
   */
  private static String decode(String hexDump) {
    return decodeHexDump(hexDump).toString(StandardCharsets.ISO_8859_1);
  }
}
