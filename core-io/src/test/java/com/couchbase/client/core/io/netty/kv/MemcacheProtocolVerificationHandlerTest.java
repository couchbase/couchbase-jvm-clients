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

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.cnc.Event;
import com.couchbase.client.core.cnc.events.io.InvalidPacketDetectedEvent;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.utils.SimpleEventBus;
import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ResourceLeakDetector;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static com.couchbase.client.core.io.netty.kv.ProtocolVerifier.decodeHexDump;
import static com.couchbase.client.utils.Utils.readResource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies the functionality of the {@link MemcacheProtocolVerificationHandler}.
 *
 * @since 2.0.0
 */
class MemcacheProtocolVerificationHandlerTest {

  static {
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
  }

  /**
   * Verifies good responses are passed through.
   *
   * @param inputHolder the good input packets.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource
  void shouldVerifyCorrectResponses(final InputHolder inputHolder) {
    CoreContext ctx = mock(CoreContext.class);
    final EmbeddedChannel channel = new EmbeddedChannel(new MemcacheProtocolVerificationHandler(ctx));
    try {
      channel.writeInbound(inputHolder.input);
      ByteBuf written = channel.readInbound();
      assertEquals(inputHolder.input, written);
    } finally {
      channel.finishAndReleaseAll();
    }
  }

  static Stream<InputHolder> shouldVerifyCorrectResponses() {
    return streamPackets(
      "response_extras_and_value",
      "success_hello_response",
      "success_errormap_response",
      "error_hello_response"
    );
  }

  /**
   * Verifies that invalid responses are not let through.
   *
   * @param inputHolder the bad input packets.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource
  void shouldCloseOnInvalidResponses(final InputHolder inputHolder) {
    SimpleEventBus eventBus = new SimpleEventBus();
    CoreEnvironment env = mock(CoreEnvironment.class);
    CoreContext ctx = mock(CoreContext.class);
    when(ctx.environment()).thenReturn(env);
    when(env.eventBus()).thenReturn(eventBus);

    final EmbeddedChannel channel = new EmbeddedChannel(new MemcacheProtocolVerificationHandler(ctx));
    try {
      channel.writeInbound(inputHolder.input);
      assertFalse(channel.isOpen());

      InvalidPacketDetectedEvent event = (InvalidPacketDetectedEvent)
        eventBus.publishedEvents().get(0);

      assertEquals(Event.Severity.ERROR, event.severity());
      assertEquals(Event.Category.IO, event.category());
      assertTrue(event.description().contains("Invalid Packet detected:"));
    } finally {
      channel.finishAndReleaseAll();
    }
  }

  static Stream<InputHolder> shouldCloseOnInvalidResponses() {
    return streamPackets(
      // a request is not allowed to go through on the response path
      "request_key_only"
    );
  }

  /**
   * Verifies good requests are passed through.
   *
   * @param inputHolder the good input packets.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource
  void shouldVerifyCorrectRequests(final InputHolder inputHolder) {
    CoreContext ctx = mock(CoreContext.class);
    final EmbeddedChannel channel = new EmbeddedChannel(new MemcacheProtocolVerificationHandler(ctx));
    try {
      channel.writeOutbound(inputHolder.input);
      ByteBuf written = channel.readOutbound();
      assertEquals(inputHolder.input, written);
    } finally {
      channel.finishAndReleaseAll();
    }
  }

  static Stream<InputHolder> shouldVerifyCorrectRequests() {
    return streamPackets("request_key_only");
  }

  /**
   * Provides the individual input byte buffers one by one to the tests.
   *
   * <p>Most of those dumps are taken from real applications, that's why
   * this format comes in handy.</p>
   *
   * @return a stream of buffers to test.
   */
  private static Stream<InputHolder> streamPackets(String... paths) {
    final Class<?> clazz = MemcacheProtocolDecodeHandlerTest.class;
    return Stream.of(paths).map(name -> new InputHolder(
      name,
      decodeHexDump(readResource(name + ".txt", clazz))
    ));
  }

  /**
   * Simple holder which allows us to give a name to the parameterized
   * function.
   */
  private static class InputHolder {
    final String name;
    final ByteBuf input;

    InputHolder(final String name, final ByteBuf input) {
      this.name = name;
      this.input = input;
    }

    @Override
    public String toString() {
      return name;
    }
  }



}
