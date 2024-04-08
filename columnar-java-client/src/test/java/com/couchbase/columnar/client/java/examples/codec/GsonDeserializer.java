/*
 * Copyright 2024 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.columnar.client.java.examples.codec;

import com.couchbase.columnar.client.java.codec.Deserializer;
import com.couchbase.columnar.client.java.codec.TypeRef;
import com.google.gson.Gson;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * A JSON deserializer backed by <a href="https://github.com/google/gson">Gson</a>.
 * <p>
 * Requires adding Gson as a dependency of your project.
 */
public class GsonDeserializer implements Deserializer {
  private final Gson gson;

  public GsonDeserializer(Gson gson) {
    this.gson = requireNonNull(gson);
  }

  @Override
  public <T> T deserialize(Class<T> target, byte[] input) {
    return gson.fromJson(toReader(input), target);
  }

  @Override
  public <T> T deserialize(TypeRef<T> target, byte[] input) {
    return gson.fromJson(toReader(input), target.type());
  }

  private static Reader toReader(byte[] input) {
    return new InputStreamReader(new ByteArrayInputStream(input), UTF_8);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }
}
