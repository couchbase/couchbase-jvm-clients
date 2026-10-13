/*
 * Copyright 2026 Couchbase, Inc.
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
package com.couchbase.client.performer.core.util;

import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Converts protobuf {@link Struct} messages into plain Java objects.
 */
public class StructUtil {
  private StructUtil() {
  }

  /**
   * Returns a map whose values are null, Boolean, Double, String, List, or Map.
   */
  public static Map<String, Object> toMap(Struct struct) {
    Map<String, Object> result = new LinkedHashMap<>();
    struct.getFieldsMap().forEach((key, value) -> result.put(key, toJava(value)));
    return result;
  }

  private static List<Object> toList(ListValue list) {
    List<Object> result = new ArrayList<>(list.getValuesCount());
    list.getValuesList().forEach(value -> result.add(toJava(value)));
    return result;
  }

  private static Object toJava(Value value) {
      return switch (value.getKindCase()) {
          case NULL_VALUE -> null;
          case BOOL_VALUE -> value.getBoolValue();
          case NUMBER_VALUE -> value.getNumberValue();
          case STRING_VALUE -> value.getStringValue();
          case LIST_VALUE -> toList(value.getListValue());
          case STRUCT_VALUE -> toMap(value.getStructValue());
          default -> throw new IllegalArgumentException("Unrecognized protobuf Value kind: " + value.getKindCase());
      };
  }
}
