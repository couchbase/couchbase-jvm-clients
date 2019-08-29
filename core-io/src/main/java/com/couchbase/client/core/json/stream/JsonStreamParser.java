/*
 * Copyright 2019 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.json.stream;

import com.couchbase.client.core.deps.com.fasterxml.jackson.core.JsonFactory;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.JsonParser;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.JsonToken;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.async.ByteArrayFeeder;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.deps.io.netty.buffer.UnpooledByteBufAllocator;
import com.couchbase.client.core.error.DecodingFailedException;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.Consumer;

import static com.couchbase.client.core.util.CbObjects.defaultIfNull;
import static java.util.Objects.requireNonNull;

/**
 * Create an instance with {@link #builder()}. Use the builder to register
 * JSON pointers and associated callbacks.
 * <p>
 * Supply the input JSON by calling {@link #feed(ByteBuf)} repeatedly.
 * Close the parser after feeding the last of the data.
 * <p>
 * Not thread safe.
 */
public class JsonStreamParser implements Closeable {
  private static final JsonFactory jsonFactory = new JsonFactory();

  /**
   * Jackson non-blocking parser tokenizes the input sent to the feeder.
   */
  private final JsonParser parser;
  private final ByteArrayFeeder feeder;

  /**
   * KLUDGE: An unpooled heap buffer used for feeding Jackson. As of Jackson 2.9.8,
   * the non-blocking parser can only be fed from offset zero of a byte array.
   * Input is copied to this buffer's backing array before being fed to Jackson.
   */
  private final ByteBuf scratchBuffer;

  /**
   * Recent history of the input stream retained in memory.
   */
  private final StreamWindow window;

  /**
   * Offset from beginning of stream where the current capture starts.
   */
  private long captureStartOffset = -1;

  /**
   * Handles traversal of the JSON document structure.
   */
  private final StructureNavigator navigator;

  /**
   * Remember whether the parser has been closed so close() may be called repeatedly.
   */
  private boolean closed;

  /**
   * Construct new parser instances using the builder returned by this method.
   */
  public static Builder builder() {
    return new Builder();
  }

  private JsonStreamParser(PathTree pathTree, ByteBuf scratchBuffer, StreamWindow window) {
    this.scratchBuffer = checkScratchBuffer(scratchBuffer);
    this.window = requireNonNull(window);
    this.navigator = new StructureNavigator(this, pathTree);

    try {
      this.parser = jsonFactory.createNonBlockingByteArrayParser();
      this.feeder = (ByteArrayFeeder) parser.getNonBlockingInputFeeder();
    } catch (IOException impossible) {
      throw new AssertionError(impossible);
    }
  }

  private static ByteBuf checkScratchBuffer(ByteBuf buf) {
    // Must have backing array because Jackson 2.x can only be fed from array.
    // Must have offset 0 due to https://github.com/FasterXML/jackson-core/issues/531
    // Must have unlimited capacity because we don't know how big the feeding buffers will be.
    if (buf.hasArray() && buf.arrayOffset() == 0 && buf.maxCapacity() == Integer.MAX_VALUE) {
      return buf;
    }
    throw new IllegalArgumentException("Expected uncapped unpooled heap buffer but got " + buf);
  }

  /**
   * Consumes all readable bytes from the given buffer. Searches for values matching
   * the configured JSON pointers and invokes callbacks for any matches.
   * <p>
   * Call this method repeatedly as more input becomes available.
   *
   * @throws DecodingFailedException if malformed JSON is detected in this chunk of input
   *                                 or if a value consumer throws an exception.
   */
  public void feed(ByteBuf input) throws DecodingFailedException {
    try {
      feedJackson(input);
      processTokens();
      collectGarbage();

    } catch (Throwable t) {
      throw new DecodingFailedException(t);
    }
  }

  /**
   * Should be called after last chunk of data to parse has been fed.
   * After calling this method no more data can be fed and parser assumes
   * no more data will be available.
   *
   * @throws DecodingFailedException if malformed JSON is detected in this chunk of input.
   */
  public void endOfInput() {
    try {
      feeder.endOfInput();
      processTokens();

    } catch (Throwable t) {
      throw new DecodingFailedException(t);
    }
  }

  private void feedJackson(ByteBuf input) throws IOException {
    // Until a ByteBufferFeeder implementation arrives in Jackson 3, must copy input
    // to a heap buffer and feed from the backing array.
    input.markReaderIndex();
    scratchBuffer.clear();
    scratchBuffer.writeBytes(input);
    input.resetReaderIndex();

    // Do this after copying into the feeder buffer because the input buffer is
    // not guaranteed to be accessible after it's added to the history window.
    // Do this before calling feedInput because that may throw an exception and we need
    // to make sure the input buffer is released when parser is closed.
    window.add(input);

    feeder.feedInput(scratchBuffer.array(), scratchBuffer.arrayOffset(), scratchBuffer.writerIndex());
  }

  private void processTokens() throws IOException {
    while (true) {
      final JsonToken token = parser.nextToken();
      if (token == JsonToken.NOT_AVAILABLE || token == null) {
        return;
      }
      //dumpToken(token);
      navigator.accept(token);
    }
  }

  private void dumpToken(JsonToken token) throws IOException {
    String location = "[" + tokenStartOffset() + "," + tokenEndOffset() + "]";
    System.out.println(token + " (" + parser.getText() + ")  location=" + location);
  }

  /**
   * Advances the stream window past data we no longer need.
   */
  private void collectGarbage() {
    if (navigator.isCapturing()) {
      window.releaseBefore(captureStartOffset);
    } else {
      // Don't actually care about the current token, but this is one way to ensure
      // we're not skipping an incomplete token after it.
      window.releaseBefore(tokenStartOffset());
    }
  }

  String getCurrentName() throws IOException {
    return parser.getCurrentName();
  }

  void beginCapture() {
    captureStartOffset = tokenStartOffset();
  }

  private long tokenStartOffset() {
    // Jackson treats this offset as one-based. We want zero-based, so subtract 1.
    return parser.getTokenLocation().getByteOffset() - 1;
  }

  private long tokenEndOffset() {
    return parser.getCurrentLocation().getByteOffset();
  }

  void emitCapturedValue(String jsonPointer, Consumer<MatchedValue> consumer) {
    final byte[] capturedValue = window.getBytes(captureStartOffset, tokenEndOffset());
    consumer.accept(new MatchedValue(jsonPointer, capturedValue));
  }

  /**
   * Releases resources managed by the parser.
   */
  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;
    scratchBuffer.release();
    window.close();

    try {
      parser.close();
    } catch (IOException inconceivable) {
      throw new AssertionError("non-blocking parser should not have thrown exception on close", inconceivable);
    }
  }

  /**
   * Builder instances are reusable provided that ALL configuration of the instance
   * occurs before any call to {@link #build()}.
   * <p>
   * Not thread safe.
   */
  public static class Builder {
    private final PathTree tree = PathTree.createRoot();
    private boolean frozen;

    /**
     * Register a callback to invoke when the target of the JSON pointer is found.
     * <p>
     * The JSON pointer path component "-" (which normally refers to the non-existent
     * array element after the end) is interpreted as a wildcard that matches every element.
     */
    public Builder doOnValue(String jsonPointer, Consumer<MatchedValue> callback) {
      checkNotFrozen();
      tree.add(jsonPointer, callback);
      return this;
    }

    /**
     * Return a new parser using the builder's configuration. May be called repeatedly
     * to get fresh parsers with the same configuration.
     */
    public JsonStreamParser build() {
      return build(null, null);
    }

    /**
     * Return a new parser that uses the given scratch buffer and stream window.
     * May be called repeatedly to get fresh parsers with the same configuration, but care must
     * be taken to ensure only one parser is using the scratch buffer at a time.
     *
     * @param scratchBuffer for reading
     * @param window for allocating the stream window's composite buffer.
     */
    public JsonStreamParser build(ByteBuf scratchBuffer, StreamWindow window) {
      frozen = true;
      return new JsonStreamParser(tree,
        defaultIfNull(scratchBuffer, Unpooled::buffer),
        defaultIfNull(window, () -> new CopyingStreamWindow(UnpooledByteBufAllocator.DEFAULT)));
    }

    private void checkNotFrozen() {
      if (frozen) {
        // path tree is mutable and shared by all parsers from this builder, so...
        throw new IllegalStateException("Can't reconfigure builder after first parser is built.");
      }
    }
  }

}
