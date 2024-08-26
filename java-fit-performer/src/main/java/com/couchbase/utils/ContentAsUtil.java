/*
 * Copyright 2023 Couchbase, Inc.
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
package com.couchbase.utils;

import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.protocol.shared.ContentAs;
import com.couchbase.client.protocol.shared.ContentTypes;
import com.google.protobuf.ByteString;

import java.util.List;
import java.util.function.Supplier;

public class ContentAsUtil {
  private static Try<ContentTypes> getNullContentType(){
    return new Try<>(ContentTypes.newBuilder().
            setContentAsNull(ContentTypes.NullValue.newBuilder()
                    .getDefaultInstanceForType()).build()
    );
  }

  public static Try<ContentTypes> contentType(ContentAs contentAs,
                                              Supplier<byte[]> asByteArray,
                                              Supplier<String> asString,
                                              Supplier<JsonObject> asJsonObject,
                                              Supplier<JsonArray> asJsonArray,
                                              Supplier<Boolean> asBoolean,
                                              Supplier<Integer> asInteger,
                                              Supplier<Double> asDouble) {
    try {
      if (contentAs.hasAsByteArray()) {
        byte[] byteArray = asByteArray.get();
        if (byteArray == null) return getNullContentType();
        return new Try<>(ContentTypes.newBuilder()
                .setContentAsBytes(ByteString.copyFrom(byteArray))
                .build());
      } else if (contentAs.hasAsString()) {
        String string = asString.get();
        if (string == null) return getNullContentType();
        return new Try<>(ContentTypes.newBuilder()
                .setContentAsString(string)
                .build());
      } else if (contentAs.hasAsJsonObject()) {
        JsonObject jsonObject = asJsonObject.get();
        if (jsonObject == null) return getNullContentType();
        return new Try<>(ContentTypes.newBuilder()
                .setContentAsBytes(ByteString.copyFrom(jsonObject.toBytes()))
                .build());
      } else if (contentAs.hasAsJsonArray()) {
        JsonArray jsonArray = asJsonArray.get();
        if (jsonArray == null) return getNullContentType();
        return new Try<>(ContentTypes.newBuilder()
                .setContentAsBytes(ByteString.copyFrom(jsonArray.toBytes()))
                .build());
      } else if (contentAs.getAsBoolean()) {
        Boolean bool = asBoolean.get();
        if (bool == null) return getNullContentType();
        return new Try<>(ContentTypes.newBuilder()
                .setContentAsBool(bool)
                .build());
      } else if (contentAs.hasAsInteger()) {
        Integer integer = asInteger.get();
        if (integer == null) return getNullContentType();
        return new Try<>(ContentTypes.newBuilder()
                .setContentAsInt64(integer)
                .build());
      } else if (contentAs.hasAsFloatingPoint()) {
        Double dbl = asDouble.get();
        if (dbl == null) return getNullContentType();
        return new Try<>(ContentTypes.newBuilder()
                .setContentAsDouble(dbl)
                .build());
      } else {
        throw new UnsupportedOperationException("Java performer cannot handle contentAs " + contentAs.toString());
      }
    } catch (RuntimeException err) {
      return new Try<>(err);
    }
  }

  public static Try<List<ContentTypes>> contentTypeList(ContentAs contentAs,
                                              Supplier<List<byte[]>> asByteArray,
                                              Supplier<List<String>> asString,
                                              Supplier<List<JsonObject>> asJsonObject,
                                              Supplier<List<JsonArray>> asJsonArray,
                                              Supplier<List<Boolean>> asBoolean,
                                              Supplier<List<Integer>> asInteger,
                                              Supplier<List<Double>> asDouble) {
    try {
      if (contentAs.hasAsByteArray()) {
        return new Try<>(asByteArray.get().stream()
                .map(v -> v != null
                        ? ContentTypes.newBuilder().setContentAsBytes(ByteString.copyFrom(v)).build()
                        : getNullContentType().value())
                .toList());
      } else if (contentAs.hasAsString()) {
        return new Try<>(asString.get().stream()
                .map(v -> v != null
                        ? ContentTypes.newBuilder().setContentAsString(v).build()
                        : getNullContentType().value()).toList());
      } else if (contentAs.hasAsJsonObject()) {
        return new Try<>(asJsonObject.get().stream()
                .map(v -> v != null
                        ? ContentTypes.newBuilder().setContentAsBytes(ByteString.copyFrom(v.toBytes())).build()
                        : getNullContentType().value())
                .toList());
      } else if (contentAs.hasAsJsonArray()) {
        return new Try<>(asJsonArray.get().stream()
                .map(v -> v != null
                        ? ContentTypes.newBuilder().setContentAsBytes(ByteString.copyFrom(v.toBytes())).build()
                        : getNullContentType().value())
                .toList());
      } else if (contentAs.getAsBoolean()) {
        return new Try<>(asBoolean.get().stream()
                .map(v -> v != null
                        ? ContentTypes.newBuilder().setContentAsBool(v).build()
                        : getNullContentType().value())
                .toList());
      } else if (contentAs.hasAsInteger()) {
        return new Try<>(asInteger.get().stream()
                .map(v -> v != null
                        ? ContentTypes.newBuilder().setContentAsInt64(v).build()
                        : getNullContentType().value())
                .toList());
      } else if (contentAs.hasAsFloatingPoint()) {
        return new Try<>(asDouble.get().stream()
                .map(v -> v != null
                        ?ContentTypes.newBuilder().setContentAsDouble(v).build()
                        : getNullContentType().value())
                .toList());
      } else {
        throw new UnsupportedOperationException("Java performer cannot handle contentAs " + contentAs.toString());
      }
    } catch (RuntimeException err) {
      return new Try<>(err);
    }
  }

  public static byte[] convert(ContentTypes content) {
    if (content.hasContentAsBytes()) {
      return content.getContentAsBytes().toByteArray();
    } else if (content.hasContentAsString()) {
      return content.getContentAsString().getBytes();
    } else throw new UnsupportedOperationException("Cannot convert " + content);
  }
}
