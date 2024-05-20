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

package com.couchbase.client.core.api.search.vector;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ArrayNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.util.Either;
import reactor.util.annotation.Nullable;

import static com.couchbase.client.core.util.Validators.notNullOrEmpty;
import static java.util.Objects.requireNonNull;

@Stability.Internal
public class CoreVector {
  private final Either<float[], String> floatsOrBase64;

  public static CoreVector of(float[] floats) {
    return eitherOf(floats, null);
  }

  public static CoreVector of(String base64) {
    return eitherOf(null, base64);
  }

  public static CoreVector eitherOf(
      @Nullable float[] floats,
      @Nullable String base64Encoded
  ) {
    return new CoreVector(Either.of(floats, base64Encoded));
  }

  private CoreVector(Either<float[], String> floatsOrBase64) {
    this.floatsOrBase64 = requireNonNull(floatsOrBase64);

    floatsOrBase64.ifPresent(
        floats -> {
          if (floats.length == 0) {
            throw InvalidArgumentException.fromMessage("Vector must have at least one element.");
          }
        },
        base64 -> notNullOrEmpty(base64, "base64EncodedVector")
    );
  }

  public void writeTo(ObjectNode parent) {
    floatsOrBase64.ifPresent(
        floats -> {
          ArrayNode array = Mapper.createArrayNode();
          for (float f : floats) {
            array.add(f);
          }
          parent.set("vector", array);
        },
        base64Encoded -> parent.put("vector_base64", base64Encoded)
    );
  }
}
