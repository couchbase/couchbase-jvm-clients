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

import com.couchbase.client.core.deps.com.fasterxml.jackson.core.JsonParser;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.JsonToken;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;

import static com.couchbase.client.core.json.stream.JsonStreamParser.jsonFactory;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public final class MatchedValue {
  private final String jsonPointer;
  private final byte[] json;

  MatchedValue(String jsonPointer, byte[] json) {
    this.jsonPointer = requireNonNull(jsonPointer);
    this.json = requireNonNull(json);
  }

  public byte[] bytes() {
    return json;
  }

  public boolean isNull() {
    return json[0] == 'n';
  }

  /**
   * @deprecated In favor of {@link #bytes()}
   */
  @Deprecated
  public byte[] readBytes() {
    return bytes();
  }

  public String readString() {
    return parseOneToken(JsonParser::getValueAsString);
  }

  public double readDouble() {
    return parseOneToken(JsonParser::getDoubleValue);
  }

  public long readLong() {
    return parseOneToken(JsonParser::getLongValue);
  }

  public boolean readBoolean() {
    return parseOneToken(JsonParser::getBooleanValue);
  }

  private interface IoFunction<T, R> {
    R apply(T t) throws IOException;
  }

  private <T> T parseOneToken(IoFunction<JsonParser, T> reader) {
    try (JsonParser parser = jsonFactory.createParser(json)) {
      JsonToken token = parser.nextToken();
      if (token == null) throw new UncheckedIOException(new EOFException("Unexpected end of stream for value at " + jsonPointer));
      if (token == JsonToken.VALUE_NULL) throw new NullPointerException("Value at " + jsonPointer + " is null.");
      return reader.apply(parser);

    } catch (IOException e) {
      throw new ClassCastException("Value at " + jsonPointer + " does not match requested type; " + e);
    }
  }

  @Override
  public String toString() {
    return "MatchedValue{" +
      "jsonPointer='" + jsonPointer + '\'' +
      ", json=" + new String(json, UTF_8) +
      '}';
  }
}
