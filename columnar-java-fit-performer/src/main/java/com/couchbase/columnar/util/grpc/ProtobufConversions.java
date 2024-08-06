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

package com.couchbase.columnar.util.grpc;

import com.couchbase.columnar.client.java.json.JsonArray;
import com.couchbase.columnar.client.java.json.JsonObject;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.protobuf.Struct;
import com.google.protobuf.NullValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProtobufConversions {

  public static ListValue jsonArrayToListValue(JsonArray jsonArray) {
    ListValue.Builder builder = ListValue.newBuilder();
    for (Object element : jsonArray.toList()) {
      if (element instanceof JsonArray el) {
        builder.addValues(Value.newBuilder().setListValue(jsonArrayToListValue(el)).build());
      } else if (element instanceof JsonObject el) {
        builder.addValues(Value.newBuilder().setStructValue(jsonObjectToStruct(el)).build());
      } else if (element == null) {
        builder.addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build());
      } else if (element instanceof String el) {
        builder.addValues(Value.newBuilder().setStringValue(el).build());
      } else if (element instanceof Boolean el) {
        builder.addValues(Value.newBuilder().setBoolValue(el).build());
      } else if (element instanceof Double el) {
        builder.addValues(Value.newBuilder().setNumberValue(el).build());
      } else if (element instanceof Float el) {
        builder.addValues(Value.newBuilder().setNumberValue(el).build());
      } else if (element instanceof Long el) {
        builder.addValues(Value.newBuilder().setNumberValue(el).build());
      } else if (element instanceof Integer el) {
        builder.addValues(Value.newBuilder().setNumberValue(el).build());
      }
    }
    return builder.build();
  }

  public static Struct jsonObjectToStruct(JsonObject jsonObject) {
    Struct.Builder builder = Struct.newBuilder();
    for (String key : jsonObject.getNames()) {
      Object element = jsonObject.get(key);
      Value toPut = null;
      if (element instanceof JsonObject) {
        toPut = Value.newBuilder().setStructValue(jsonObjectToStruct((JsonObject) element)).build();
      } else if (element instanceof JsonArray) {
        toPut = Value.newBuilder().setListValue(jsonArrayToListValue((JsonArray) element)).build();
      } else if (element instanceof String el) {
        toPut = Value.newBuilder().setStringValue(el).build();
      } else if (element instanceof Boolean el) {
        toPut = Value.newBuilder().setBoolValue(el).build();
      } else if (element instanceof Double el) {
        toPut = Value.newBuilder().setNumberValue(el).build();
      } else if (element instanceof Float el) {
        toPut = Value.newBuilder().setNumberValue(el).build();
      } else if (element instanceof Long el) {
        toPut = Value.newBuilder().setNumberValue(el).build();
      } else if (element instanceof Integer el) {
        toPut = Value.newBuilder().setNumberValue(el).build();
      }
      if (toPut != null) {
        builder.putFields(key, toPut);
      }
    }
    return builder.build();
  }


  public static List<Object> protobufListValueToList(ListValue listValue) {
    List<Object> jsonArray = new ArrayList<>();
    for (Value value : listValue.getValuesList()) {
      switch (value.getKindCase()) {
        case STRING_VALUE:
          jsonArray.add(value.getStringValue());
          break;
        case BOOL_VALUE:
          jsonArray.add(value.getBoolValue());
          break;
        case NUMBER_VALUE:
          jsonArray.add(value.getNumberValue());
          break;
        case STRUCT_VALUE:
          jsonArray.add(protobufStructToMap(value.getStructValue()));
          break;
        case LIST_VALUE:
          jsonArray.add(protobufListValueToList(value.getListValue()));
          break;
        case NULL_VALUE:
          jsonArray.add(null);
          break;
        default:
          throw new IllegalArgumentException("Unsupported value type: " + value.getKindCase());
      }
    }
    return jsonArray;
  }

  public static Map<String, Object> protobufStructToMap(Struct struct) {
    Map<String, Object> jsonObject = new HashMap<>();
    for (Map.Entry<String, Value> entry : struct.getFieldsMap().entrySet()) {
      String key = entry.getKey();
      Value value = entry.getValue();
      switch (value.getKindCase()) {
        case STRING_VALUE:
          jsonObject.put(key, value.getStringValue());
          break;
        case BOOL_VALUE:
          jsonObject.put(key, value.getBoolValue());
          break;
        case NUMBER_VALUE:
          jsonObject.put(key, value.getNumberValue());
          break;
        case STRUCT_VALUE:
          jsonObject.put(key, protobufStructToMap(value.getStructValue()));
          break;
        case LIST_VALUE:
          jsonObject.put(key, protobufListValueToList(value.getListValue()));
          break;
        case NULL_VALUE:
          jsonObject.put(key, null);
          break;
        default:
          throw new IllegalArgumentException("Unsupported value type: " + value.getKindCase());
      }
    }
    return jsonObject;
  }
}
