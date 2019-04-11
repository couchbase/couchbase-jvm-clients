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

import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.error.DecodingFailedException;
import com.couchbase.client.core.json.Mapper;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class MatchedValue {
  private final String jsonPointer;
  private final byte[] json;

  MatchedValue(String jsonPointer, byte[] json) {
    this.jsonPointer = jsonPointer;
    this.json = requireNonNull(json);
  }

  public boolean isNull() {
    return json[0] == 'n';
  }

  public byte[] readBytes() {
    return json;
  }

  public JsonNode readTree() {
    try {
      return requireNonNull(Mapper.decodeIntoTree(json));
    } catch (Exception shouldNeverHappen) {
      throw new AssertionError("Value at " + jsonPointer + " is not JSON.", shouldNeverHappen);
    }
  }

  public String readString() {
    return read(String.class);
  }

  public double readDouble() {
    return read(Double.class);
  }

  public long readLong() {
    return read(Long.class);
  }

  public boolean readBoolean() {
    return read(Boolean.class);
  }

  private <T> T read(Class<T> type) {
    try {
      return requireNonNull(Mapper.decodeInto(json, type));
    } catch (Exception e) {
      throw new DecodingFailedException("Value at " + jsonPointer + " is not a " + type.getSimpleName(), e);
    }
  }

  @Override
  public String toString() {
    return "JsonValue{" +
      "jsonPointer='" + jsonPointer + '\'' +
      ", json=" + new String(json, UTF_8) +
      '}';
  }
}
