/*
 * Copyright 2023 Couchbase, Inc.
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

package com.couchbase.client.java.query;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ArrayNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;

@Stability.Internal
public class QueryOptionsUtil {

  public static ArrayNode convert(List<Object> in) {
    ArrayNode out = Mapper.createArrayNode();

    in.forEach(v -> {
      if (v instanceof String) {
        out.add((String) v);
      }
      else if (v instanceof Integer) {
        out.add((Integer) v);
      }
      else if (v instanceof Long) {
        out.add((Long) v);
      }
      else if (v instanceof Double) {
        out.add((Double) v);
      }
      else if (v instanceof Boolean) {
        out.add((Boolean) v);
      }
      else if (v instanceof BigInteger) {
        out.add((BigInteger) v);
      }
      else if (v instanceof BigDecimal) {
        out.add((BigDecimal) v);
      }
      else if (v instanceof JsonObject) {
        ObjectNode converted = convert(((JsonObject) v).toMap());
        out.add(converted);
      }
      else if (v instanceof JsonArray) {
        ArrayNode converted = convert(((JsonArray) v).toList());
        out.add(converted);
      }
    });

    return out;
  }

  public static ObjectNode convert(Map<String, Object> in) {
    ObjectNode out = Mapper.createObjectNode();

    in.forEach((k, v) -> {
      if (v instanceof String) {
        out.put(k, (String) v);
      }
      else if (v instanceof Integer) {
        out.put(k, (Integer) v);
      }
      else if (v instanceof Long) {
        out.put(k, (Long) v);
      }
      else if (v instanceof Double) {
        out.put(k, (Double) v);
      }
      else if (v instanceof Boolean) {
        out.put(k, (Boolean) v);
      }
      else if (v instanceof BigInteger) {
        out.put(k, (BigInteger) v);
      }
      else if (v instanceof BigDecimal) {
        out.put(k, (BigDecimal) v);
      }
      else if (v instanceof JsonObject) {
        ObjectNode converted = convert(((JsonObject) v).toMap());
        out.set(k, converted);
      }
      else if (v instanceof JsonArray) {
        ArrayNode converted = convert(((JsonArray) v).toList());
        out.set(k, converted);
      }
    });

    return out;
  }
}
