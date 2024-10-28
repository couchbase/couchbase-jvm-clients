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
import reactor.util.annotation.Nullable;

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

  public static Class<?> toJavaClass(ContentAs contentAs) {
    return switch (contentAs.getAsCase()) {
      case AS_STRING -> String.class;
      case AS_BYTE_ARRAY -> byte[].class;
      case AS_JSON_OBJECT -> JsonObject.class;
      case AS_JSON_ARRAY -> JsonArray.class;
      case AS_BOOLEAN -> Boolean.class;
      case AS_INTEGER -> Integer.class;
      case AS_FLOATING_POINT -> Double.class;

      default -> throw new UnsupportedOperationException("Java performer cannot handle contentAs " + contentAs);
    };
  }

  public static ContentTypes toFitContent(@Nullable Object value, ContentAs contentAs) {
    ContentTypes.Builder builder = ContentTypes.newBuilder();

    if (value == null) return builder.setContentAsNull(ContentTypes.NullValue.getDefaultInstance()).build();

    switch (contentAs.getAsCase()) {
      case AS_STRING -> builder.setContentAsString((String) value);
      case AS_BYTE_ARRAY -> builder.setContentAsBytes(ByteString.copyFrom((byte[]) value));
      case AS_JSON_OBJECT -> builder.setContentAsBytes(ByteString.copyFrom(((JsonObject) value).toBytes()));
      case AS_JSON_ARRAY -> builder.setContentAsBytes(ByteString.copyFrom(((JsonArray) value).toBytes()));
      case AS_BOOLEAN -> builder.setContentAsBool((Boolean) value);
      case AS_INTEGER -> builder.setContentAsInt64((Integer) value);
      case AS_FLOATING_POINT -> builder.setContentAsDouble((Double) value);

      default -> throw new UnsupportedOperationException("Java performer cannot handle contentAs " + contentAs);
    }

    return builder.build();
  }

  public static byte[] convert(ContentTypes content) {
    if (content.hasContentAsBytes()) {
      return content.getContentAsBytes().toByteArray();
    } else if (content.hasContentAsString()) {
      return content.getContentAsString().getBytes();
    } else throw new UnsupportedOperationException("Cannot convert " + content);
  }
}
