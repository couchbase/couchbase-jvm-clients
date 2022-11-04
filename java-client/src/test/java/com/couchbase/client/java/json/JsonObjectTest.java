/*
 * Copyright (c) 2016 Couchbase, Inc.
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

package com.couchbase.client.java.json;

import com.couchbase.client.core.error.InvalidArgumentException;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.couchbase.client.core.util.CbCollections.listOf;
import static com.couchbase.client.core.util.CbCollections.mapOf;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class JsonObjectTest {

  @Test
  void shouldExportEmptyObject() {
    String result = JsonObject.create().toString();
    assertEquals("{}", result);
  }

  @Test
  void shouldExportStrings() {
    String result = JsonObject.create().put("key", "value").toString();
    assertEquals("{\"key\":\"value\"}", result);
  }

  @Test
  void shouldConvertToBytes() {
    JsonObject input = JsonObject.create().put("key", "value");

    String stringResult = input.toString();
    byte[] bytesResult = input.toBytes();
    assertArrayEquals(stringResult.getBytes(StandardCharsets.UTF_8), bytesResult);
  }

  @Test
  void shouldExportNestedObjects() {
    JsonObject obj = JsonObject.create()
      .put("nested", JsonObject.create().put("a", true));
    assertEquals("{\"nested\":{\"a\":true}}", obj.toString());
  }

  @Test
  void shouldExportNestedArrays() {
    JsonObject obj = JsonObject.create()
      .put("nested", JsonArray.create().add(true).add(4).add("foo"));
    assertEquals("{\"nested\":[true,4,\"foo\"]}", obj.toString());
  }

  @Test
  void shouldExportEscapedJsonValue() {
    JsonObject obj = JsonObject.create().put("escapeSimple", "\"\b\r\n\f\t\\/");
    String escaped = "\\\"\\b\\r\\n\\f\\t\\\\/";
    assertEquals("{\"escapeSimple\":\"" + escaped + "\"}", obj.toString());
  }

  @Test
  void shouldExportEscapedJsonAttribute() {
    JsonObject obj = JsonObject.create().put("\"\b\r\n\f\t\\/", "escapeSimpleValue");
    String escaped = "\\\"\\b\\r\\n\\f\\t\\\\/";
    assertEquals("{\"" + escaped + "\":\"escapeSimpleValue\"}", obj.toString());
  }

  @Test
  void shouldExportEscapedControlCharInValue() {
    JsonObject obj = JsonObject.create().put("controlChar", "\u001F");
    String escaped = "\\u001F";
    assertEquals("{\"controlChar\":\"" + escaped + "\"}", obj.toString());
  }

  @Test
  void shouldExportEscapedControlCharInAttribute() {
    JsonObject obj = JsonObject.create().put("\u001F", "controlCharValue");
    String escaped = "\\u001F";
    assertEquals("{\"" + escaped + "\":\"controlCharValue\"}", obj.toString());
  }

  @Test
  void shouldReturnNullWhenNotFound() {
    JsonObject obj = JsonObject.create();
    assertNull(obj.getInt("notfound"));
  }

  @Test
  void shouldEqualBasedOnItsProperties() {
    JsonObject obj1 = JsonObject.create().put("foo", "bar");
    JsonObject obj2 = JsonObject.create().put("foo", "bar");
    assertEquals(obj1, obj2);

    obj1 = JsonObject.create().put("foo", "baz");
    obj2 = JsonObject.create().put("foo", "bar");
    assertNotEquals(obj1, obj2);

    obj1 = JsonObject.create().put("foo", "bar").put("bar", "baz");
    obj2 = JsonObject.create().put("foo", "bar");
    assertNotEquals(obj1, obj2);
  }

  @Test
  void shouldConvertNumbers() {
    JsonObject obj = JsonObject.create().put("number", 1L);

    assertEquals(Double.valueOf(1.0d), obj.getDouble("number"));
    assertEquals(Long.valueOf(1L), obj.getLong("number"));
    assertEquals(Integer.valueOf(1), obj.getInt("number"));
  }

  @Test
  void shouldConvertOverflowNumbers() {
    int maxValue = Integer.MAX_VALUE; //int max value is 2147483647
    long largeValue = maxValue + 3L;
    double largerThanIntMaxValue = largeValue + 0.56d;

    JsonObject obj = JsonObject.create().put("number", largerThanIntMaxValue);
    assertEquals(Double.valueOf(largerThanIntMaxValue), obj.getDouble("number"));
    assertEquals(Long.valueOf(largeValue), obj.getLong("number"));
    assertEquals(Integer.valueOf(maxValue), obj.getInt("number"));
  }

  @Test
  void shouldNotNullPointerOnGetNumber() {
    JsonObject obj = JsonObject.create();

    assertNull(obj.getDouble("number"));
    assertNull(obj.getLong("number"));
    assertNull(obj.getInt("number"));
  }

  @Test
  void shouldConstructEmptyObjectFromEmptyMap() {
    JsonObject obj = JsonObject.from(Collections.emptyMap());
    assertNotNull(obj);
    assertTrue(obj.isEmpty());
  }


  @Test
  void shouldNullPointerOnNullMap() {
    assertThrows(NullPointerException.class, () -> JsonObject.from(null));
  }

  @Test
  void shouldConstructJsonObjectFromMap() {
    String item1 = "item1";
    Double item2 = 2.2d;
    Long item3 = 3L;
    Boolean item4 = true;
    JsonArray item5 = JsonArray.create();
    JsonObject item6 = JsonObject.create();
    Map<String, Object> source = new HashMap<>(6);
    source.put("key1", item1);
    source.put("key2", item2);
    source.put("key3", item3);
    source.put("key4", item4);
    source.put("key5", item5);
    source.put("key6", item6);

    JsonObject obj = JsonObject.from(source);
    assertNotNull(obj);
    assertEquals(6, obj.size());
    assertEquals(item1, obj.get("key1"));
    assertEquals(item2, obj.get("key2"));
    assertEquals(item3, obj.get("key3"));
    assertEquals(item4, obj.get("key4"));
    assertEquals(item5, obj.get("key5"));
    assertEquals(item6, obj.get("key6"));
  }

  @Test
  void shouldDetectNullKeyInMap() {
    Map<String, Double> badMap = new HashMap<>(2);
    badMap.put("key1", 1.1d);
    badMap.put(null, 2.2d);
    assertThrows(NullPointerException.class, () -> JsonObject.from(badMap));
  }

  @Test
  void shouldAcceptNullValueInMap() {
    Map<String, Long> badMap = new HashMap<>(2);
    badMap.put("key1", 1L);
    badMap.put("key2", null);

    JsonObject obj = JsonObject.from(badMap);
    assertNotNull(obj);
    assertEquals(2, obj.size());
    assertNotNull(obj.get("key1"));
    assertNull(obj.get("key2"));
  }

  @Test
  void shouldDetectIncorrectItemInMap() {
    Object badItem = new CloneNotSupportedException();
    Map<String, Object> badMap = new HashMap<>(1);
    badMap.put("key1", badItem);
    assertThrows(InvalidArgumentException.class, () -> JsonObject.from(badMap));
  }

  @Test
  void shouldRecursivelyParseMaps() {
    Map<String, Double> subMap = new HashMap<>(2);
    subMap.put("value1", 1.2d);
    subMap.put("value2", 3.4d);
    Map<String, Object> recurseMap = new HashMap<>(2);
    recurseMap.put("key1", "test");
    recurseMap.put("key2", subMap);

    JsonObject obj = JsonObject.from(recurseMap);
    assertNotNull(obj);
    assertEquals(2, obj.size());
    assertEquals("test", obj.getString("key1"));

    assertNotNull(obj.get("key2"));
    assertEquals(JsonObject.class, obj.get("key2").getClass());
    assertEquals(2, obj.getObject("key2").size());
  }

  @Test
  void shouldRecursivelyParseLists() {
    List<Double> subList = new ArrayList<>(2);
    subList.add(1.2d);
    subList.add(3.4d);
    Map<String, Object> recurseMap = new HashMap<>(2);
    recurseMap.put("key1", "test");
    recurseMap.put("key2", subList);

    JsonObject obj = JsonObject.from(recurseMap);
    assertNotNull(obj);
    assertEquals(2, obj.size());
    assertEquals("test", obj.getString("key1"));

    assertNotNull(obj.get("key2"));
    assertEquals(JsonArray.class, obj.get("key2").getClass());
    assertEquals(2, obj.getArray("key2").size());
  }

  @Test
  void shouldThrowOnBadSubMap() {
    Map<Integer, String> badMap1 = mapOf(1, "test");
    Map<String, Object> badMap2 = mapOf("key1", new CloneNotSupportedException());

    InvalidArgumentException e = assertThrows(InvalidArgumentException.class, () -> JsonObject.from(mapOf("subMap", badMap1)));
    assertInstanceOf(ClassCastException.class, e.getCause());

    assertThrows(InvalidArgumentException.class, () -> JsonObject.from(mapOf("subMap", badMap2)));
  }

  @Test
  void shouldThrowOnBadSubList() {
    List<?> badSubList = listOf(new CloneNotSupportedException());
    Map<String, ?> source = mapOf("test", badSubList);
    assertThrows(InvalidArgumentException.class, () -> JsonObject.from(source));
  }

  @Test
  void shouldTreatJsonValueNullConstantAsNull() {
    JsonObject obj = JsonObject.create();
    obj.put("directNull", JsonValue.NULL);
    obj.put("subMapWithNull", JsonObject.from(
      Collections.singletonMap("subNull", JsonValue.NULL)));
    obj.put("subArrayWithNull", JsonArray.from(
      Collections.singletonList(JsonValue.NULL)));

    assertTrue(obj.containsKey("directNull"));
    assertNull(obj.get("directNull"));

    assertNotNull(obj.getObject("subMapWithNull"));
    assertTrue(obj.getObject("subMapWithNull").containsKey("subNull"));
    assertNull(obj.getObject("subMapWithNull").get("subNull"));

    assertNotNull(obj.getArray("subArrayWithNull"));
    assertNull(obj.getArray("subArrayWithNull").get(0));
  }

  @Test
  void shouldPutMapAsAJsonObject() {
    Map<String, Object> map = new HashMap<>(2);
    map.put("item1", "value1");
    map.put("item2", true);
    JsonObject obj = JsonObject.create().put("sub", map);

    assertTrue(obj.containsKey("sub"));
    assertNotNull(obj.get("sub"));
    assertInstanceOf(JsonObject.class, obj.get("sub"));
    assertTrue(obj.getObject("sub").containsKey("item1"));
    assertTrue(obj.getObject("sub").containsKey("item2"));
  }

  @Test
  void shouldPutListAsAJsonArray() {
    List<Object> list = new ArrayList<>(2);
    list.add("value1");
    list.add(true);
    JsonObject obj = JsonObject.create().put("sub", list);

    assertTrue(obj.containsKey("sub"));
    assertNotNull(obj.get("sub"));
    assertInstanceOf(JsonArray.class, obj.get("sub"));
    assertEquals(2, obj.getArray("sub").size());
    assertEquals("value1", obj.getArray("sub").get(0));
    assertEquals(Boolean.TRUE, obj.getArray("sub").get(1));
  }

  @Test
  void shouldConvertSubJsonValuesToCollections() {
    JsonObject sub1 = JsonObject.create().put("sub1.1", "test");
    JsonArray sub2 = JsonArray.create().add("sub2.1");
    JsonObject obj = JsonObject.create()
      .put("sub1", sub1)
      .put("sub2", sub2);

    Map<String, Object> asMap = obj.toMap();
    Object mSub1 = asMap.get("sub1");
    Object mSub2 = asMap.get("sub2");

    assertNotNull(mSub1);
    assertInstanceOf(Map.class, mSub1);
    assertEquals("test", ((Map) mSub1).get("sub1.1"));

    assertNotNull(mSub2);
    assertInstanceOf(List.class, mSub2);
    assertEquals("sub2.1", ((List) mSub2).get(0));
  }

  @Test
  void shouldThrowIfAddingToSelf() {
    JsonObject obj = JsonObject.create();
    try {
      obj.put("test", obj);
      fail();
    } catch (InvalidArgumentException e) {
      //success
    }
    try {
      obj.put("test", (Object) obj);
      fail();
    } catch (InvalidArgumentException e) {
      //success
    }
  }

  @Test
  void shouldConvertFromStringJson() {
    String someJson = "{\"test\": 123, \"string\": \"value\", \"bool\": true, \"subarray\": [ 123 ], \"subobj\":"
      + "{ \"sub\": \"obj\" }}";
    JsonObject expected = JsonObject.create()
      .put("test", 123)
      .put("string", "value")
      .put("bool", true)
      .put("subarray", JsonArray.from(123))
      .put("subobj", JsonObject.create().put("sub", "obj"));

    JsonObject converted = JsonObject.fromJson(someJson);

    assertEquals(expected, converted);
  }

  @Test
  void shouldConvertNullStringToNull() {
    assertNull(JsonObject.fromJson("null"));
  }

  @Test
  void shouldFailToConvertBadJsonString() {
    String badJson = "This is not \"JSON\"!";
    assertThrows(InvalidArgumentException.class, () -> JsonObject.fromJson(badJson));
  }

  @Test
  void shouldFailToConvertNonObjectJson() {
    String bad1 = "true";
    String bad2 = "123";
    String bad3 = "\"string\"";
    String bad4 = "[ 123 ]";

    assertThrows(InvalidArgumentException.class, () -> JsonObject.fromJson(bad1));
    assertThrows(InvalidArgumentException.class, () -> JsonObject.fromJson(bad2));
    assertThrows(InvalidArgumentException.class, () -> JsonObject.fromJson(bad3));
    assertThrows(InvalidArgumentException.class, () -> JsonObject.fromJson(bad4));
  }

  @Test
  void shouldSupportBigInteger() {
    BigInteger bigint = new BigInteger("12345678901234567890");
    JsonObject original = JsonObject
      .create()
      .put("value", bigint);


    String encoded = original.toString();
    assertEquals("{\"value\":12345678901234567890}", encoded);

    JsonObject decoded = JsonObject.fromJson(encoded);
    assertEquals(bigint, decoded.getBigInteger("value"));
    assertInstanceOf(BigInteger.class, decoded.getNumber("value"));
  }

  @Test
  void shouldSupportBigDecimalConverted() {
    BigDecimal bigdec = new BigDecimal("1234.5678901234567890432423432324");
    JsonObject original = JsonObject
      .create()
      .put("value", bigdec);


    String encoded = original.toString();
    assertEquals("{\"value\":1234.5678901234567890432423432324}", encoded);

    JsonObject decoded = JsonObject.fromJson(encoded);
    // This happens because of double rounding, set com.couchbase.json.decimalForFloat true for better accuracy
    // but more overhead
    assertEquals(
      new BigDecimal("1234.5678901234568911604583263397216796875"),
      decoded.getBigDecimal("value")
    );
    assertEquals(1234.567890123457, decoded.getDouble("value"), 0.1);
    assertInstanceOf(Double.class, decoded.getNumber("value"));
  }

  @Test
  void canPutWhenTypeIsUnknown() {
    Object map = mapOf("one", 1);
    Object list = listOf("red");
    JsonObject json = JsonObject.create()
        .put("map", map)
        .put("list", list);

    assertEquals(JsonObject.from(mapOf("one", 1)), json.getObject("map"));
    assertEquals(JsonArray.from(listOf("red")), json.getArray("list"));
  }
}
