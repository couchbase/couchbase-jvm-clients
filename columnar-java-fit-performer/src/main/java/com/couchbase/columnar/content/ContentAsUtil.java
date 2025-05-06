/*
 * Copyright (c) 2024 Couchbase, Inc.
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

package com.couchbase.columnar.content;

// CHECKSTYLE:OFF IllegalImport - Allow unbundled Jackson

import com.couchbase.columnar.client.java.Row;
import com.couchbase.columnar.client.java.json.JsonArray;
import com.couchbase.columnar.client.java.json.JsonObject;
import com.couchbase.columnar.util.Try;
import com.couchbase.columnar.util.grpc.ProtobufConversions;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;

import java.util.function.Supplier;

public class ContentAsUtil {
  public static Try<fit.columnar.ContentWas> contentType(
    fit.columnar.ContentAs contentAs,
    Row row
  ) {
    return ContentAsUtil.contentType(
      contentAs,
      () -> row.bytes(),
      () -> row.asNullable(JsonArray.class),
      () -> {
        // The SDK bypasses the deserializer if the target type is `JsonObject` or `JsonArray`.
        // To ensure we _really_ use `CustomDeserializer`, convert to a Jackson ObjectNode instead of our JsonObject.
        ObjectNode obj = row.asNullable(ObjectNode.class);
        return obj == null ? null : JsonObject.fromJson(obj.toString());
      },
      () -> row.asNullable(String.class)
    );
  }

  private static Try<fit.columnar.ContentWas> contentType(
    fit.columnar.ContentAs contentAs,
    Supplier<byte[]> asByteArray,
    Supplier<JsonArray> asList,
    Supplier<JsonObject> asMap,
    Supplier<String> asString
  ) {
    try {
      if (contentAs.hasAsByteArray()) {
        return new Try<>(fit.columnar.ContentWas.newBuilder()
          .setContentWasBytes(ByteString.copyFrom(asByteArray.get()))
          .build());
      } else if (contentAs.hasAsList()) {
        return new Try<>(fit.columnar.ContentWas.newBuilder()
          .setContentWasList(ProtobufConversions.jsonArrayToListValue(asList.get()))
          .build());
      } else if (contentAs.hasAsMap()) {
        return new Try<>(fit.columnar.ContentWas.newBuilder()
          .setContentWasMap(ProtobufConversions.jsonObjectToStruct(asMap.get()))
          .build());
      } else if (contentAs.hasAsString()) {
        return new Try<>(fit.columnar.ContentWas.newBuilder()
          .setContentWasString(asString.get())
          .build());
      } else {
        throw new UnsupportedOperationException("Java performer cannot handle contentAs " + contentAs);
      }
    } catch (RuntimeException err) {
      return new Try<>(err);
    }
  }

}
