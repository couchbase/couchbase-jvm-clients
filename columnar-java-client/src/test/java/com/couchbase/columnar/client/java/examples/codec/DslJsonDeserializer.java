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
import com.dslplatform.json.DslJson;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static java.util.Objects.requireNonNull;

/**
 * A JSON deserializer backed by <a href="https://github.com/ngs-doo/dsl-json">DSL-JSON</a>.
 * <p>
 * Requires adding DSL-JSON as a dependency of your project.
 */
public class DslJsonDeserializer implements Deserializer {
  private final DslJson<?> dslJson;

  public DslJsonDeserializer(DslJson<?> dslJson) {
    this.dslJson = requireNonNull(dslJson);
  }

  @Override
  public <T> T deserialize(Class<T> target, byte[] input) throws IOException {
    return dslJson.deserialize(target, new ByteArrayInputStream(input));
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T deserialize(TypeRef<T> target, byte[] input) throws IOException {
    return (T) dslJson.deserialize(target.type(), new ByteArrayInputStream(input));
  }
}
