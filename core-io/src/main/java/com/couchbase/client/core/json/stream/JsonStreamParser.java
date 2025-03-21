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
import org.jspecify.annotations.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.function.Consumer;

/**
 * Create an instance with {@link #builder()}. Use the builder to register
 * JSON pointers and associated callbacks.
 * <p>
 * Supply the input JSON by calling {@link #feed} repeatedly.
 * When you're done feeding the parser JSON, call {@link #endOfInput()}
 * to validate the input was well-formed.
 * <p>
 * Finally, call {@link #close()} to release resources used by the parser.
 * <p>
 * Not thread safe.
 */
public final class JsonStreamParser implements Closeable {
  static final JsonFactory jsonFactory = new JsonFactory();

  private final JsonParser parser;
  private final ByteArrayFeeder feeder;

  private final Buffer scratchBuffer = new Buffer();

  /**
   * Recent history of the input stream retained in memory.
   */
  private final StreamWindow window = new CopyingStreamWindow();

  /**
   * Offset from beginning of stream where the current capture starts.
   */
  private long captureStartOffset = -1;

  /**
   * Handles traversal of the JSON document structure.
   */
  private final StructureNavigator navigator;

  /**
   * Construct new parser instances using the builder returned by this method.
   */
  public static Builder builder() {
    return new Builder();
  }

  private JsonStreamParser(PathTree pathTree) {
    this.navigator = new StructureNavigator(this, pathTree);

    try {
      this.parser = jsonFactory.createNonBlockingByteArrayParser();
      this.feeder = (ByteArrayFeeder) parser.getNonBlockingInputFeeder();
    } catch (IOException shouldNotHappen) {
      throw new UncheckedIOException(shouldNotHappen);
    }
  }

  /**
   * Consumes all readable bytes from the given buffer. Searches for values matching
   * the configured JSON pointers and invokes callbacks for any matches.
   * <p>
   * Call this method repeatedly as more input becomes available.
   *
   * @throws UncheckedIOException if malformed JSON is detected in this chunk of input
   * @throws RuntimeException if a value consumer throws an exception
   */
  public void feed(byte[] array, int offset, int len) {
    try {
      feedJackson(array, offset, len);
      processTokens();
      collectGarbage();

    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public void feed(ByteBuffer input) {
    scratchBuffer.clear();
    scratchBuffer.writeBytes(input);
    feed(scratchBuffer.array(), 0, scratchBuffer.readableBytes());
  }

  /**
   * Should be called after last chunk of data to parse has been fed.
   * After calling this method no more data can be fed and parser assumes
   * no more data will be available.
   *
   * @throws UncheckedIOException if malformed JSON is detected in this chunk of input.
   * @throws RuntimeException if a value consumer throws an exception
   */
  public void endOfInput() {
    try {
      feeder.endOfInput();
      processTokens();

    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private void feedJackson(byte[] bytes, int offset, int len) throws IOException {
    // Remember the JSON so we can retrieve matched values later.
    window.add(bytes, offset, len);

    // Workaround for https://github.com/FasterXML/jackson-core/issues/1412
    if (offset != 0) {
      scratchBuffer.clear();
      scratchBuffer.writeBytes(bytes, offset, len);
      bytes = scratchBuffer.array();
      offset = 0;
    }

    feeder.feedInput(bytes, offset, offset + len); // Watch out! Second argument is an offset, not necessarily length.
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

  @Nullable
  String getCurrentName() throws IOException {
    return parser.currentName();
  }

  void beginCapture() {
    captureStartOffset = tokenStartOffset();
  }

  private long tokenStartOffset() {
    // Jackson treats this offset as one-based. We want zero-based, so subtract 1.
    return parser.currentTokenLocation().getByteOffset() - 1;
  }

  private long tokenEndOffset() {
    return parser.currentLocation().getByteOffset();
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
    try {
      parser.close();
    } catch (IOException shouldNotHappen) {
      throw new UncheckedIOException("Jackson non-blocking JsonParser threw an exception on close, which is totally unexpected.", shouldNotHappen);
    }
  }

  /**
   * Builder instances are reusable provided that ALL configuration of the instance
   * occurs before any call to {@link #build()}.
   * <p>
   * Not thread safe.
   */
  public static final class Builder {
    private final PathTree tree = PathTree.createRoot();
    private volatile boolean frozen;

    Builder() {
    }

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
      frozen = true;
      return new JsonStreamParser(tree);
    }

    private void checkNotFrozen() {
      if (frozen) {
        // path tree is mutable and shared by all parsers from this builder, so...
        throw new IllegalStateException("Can't reconfigure builder after first parser is built.");
      }
    }
  }

}
