/*
 * Copyright 2022 Couchbase, Inc.
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

package com.couchbase.client.java.http;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static com.couchbase.client.java.http.NameValuePair.nv;
import static org.junit.jupiter.api.Assertions.assertEquals;

class NameValuePairsTest {

  @Test
  void ofPairs() {
    String encoded = NameValuePairs.of(
        nv("1 ", "one "),
        nv("2 ", "two "),
        nv("1 ", "one ")
    ).urlEncoded;
    assertEquals(encoded, "1%20=one%20&2%20=two%20&1%20=one%20");
  }

  @Test
  void ofMap() {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("1 ", "one ");
    map.put("2 ", "two ");

    String encoded = NameValuePairs.of(map).urlEncoded;
    assertEquals(encoded, "1%20=one%20&2%20=two%20");
  }

  @Test
  void ofPreEncoded() {
    String s = "Garbage in, garbage out!";
    assertEquals(s, NameValuePairs.ofPreEncoded(s).urlEncoded);
  }
}
