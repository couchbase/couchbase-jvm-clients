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

import static com.couchbase.client.core.io.netty.kv.ProtocolVerifier.decodeHexDump;
import static com.couchbase.client.utils.Utils.readResource;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ResourceLeakDetector;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Random;
import java.util.stream.Stream;

/**
 * Verifies the functionality of the {@link MemcacheProtocolDecodeHandler}.
 *
 * <p>If you want to add more tests, just add them to the {@link #inputProvider()} as
 * filenames and place them into the right directory (see other files in the
 * resources folder).</p>
 *
 * @since 2.0.0
 */
class MemcacheProtocolDecodeHandlerTest {

  static {
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
  }

  /**
   * Runs through all provided inputs and makes sure even if they are sliced
   * and diced they come out whole at the end.
   *
   * <p>For now each input is tried 100 times with different chunk sizes to increase
   * the chance of finding a problem, this can be increased or decreased as
   * needed.</p>
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("inputProvider")
  void shouldDecodeInput(final InputHolder inputHolder) {
    final EmbeddedChannel channel = new EmbeddedChannel(new MemcacheProtocolDecodeHandler());
    try {
      for (int i = 0; i < 100; i++) {
        ByteBuf copy = inputHolder.input.copy();
        writeAsRandomChunks(channel, copy);

        ByteBuf read = channel.readInbound();
        copy.resetReaderIndex();
        assertEquals(inputHolder.input, read);

        ReferenceCountUtil.release(copy);
        ReferenceCountUtil.release(read);
      }

      ReferenceCountUtil.release(inputHolder.input);
    } finally {
      channel.finishAndReleaseAll();
    }
  }

  /**
   * Provides the individual input byte buffers one by one to the
   * {@link #shouldDecodeInput(InputHolder)} test.
   *
   * <p>Most of those dumps are taken from real applications, that's why
   * this format comes in handy.</p>
   *
   * @return a stream of buffers to test.
   */
  private static Stream<InputHolder> inputProvider() {
    final Class<?> clazz = MemcacheProtocolDecodeHandlerTest.class;
    return Stream.of(
      "request_key_only",
      "response_extras_and_value"
    ).map(name -> new InputHolder(
      name,
      decodeHexDump(readResource(name + ".txt", clazz))
    ));
  }

  /**
   * Simple holder which alles us to give a name to the parameterized
   * function in {@link #shouldDecodeInput(InputHolder)}.
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

  /**
   * Helper method to randomly chunk up the source until the channel emits a new message.
   *
   * @param channel the channel to write into.
   * @param source  the source buffer.
   */
  private static void writeAsRandomChunks(final EmbeddedChannel channel, final ByteBuf source) {
    while (channel.inboundMessages().isEmpty()) {
      int chunkSize = new Random().nextInt(source.readableBytes() + 1);
      ByteBuf toWrite = source.readBytes(chunkSize);
      channel.writeOneInbound(toWrite);
    }
  }

}