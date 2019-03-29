/*
 * Copyright (c) 2018 Couchbase, Inc.
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

package com.couchbase.client.java.view;

import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import org.junit.jupiter.api.Test;

import static com.couchbase.client.java.view.ViewOptions.viewOptions;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the functionality of the {@link ViewOptions}.
 */
class ViewOptionsTest {

  @Test
  void shouldSetDefaults() {
    ViewOptions options = viewOptions();
    assertFalse(options.build().development());
    assertEquals("", options.export());
    assertEquals("ViewQuery{params=\"\"}", options.toString());
  }

  @Test
  void shouldReduce() {
    ViewOptions options = viewOptions().reduce(true);
    assertEquals("reduce=true", options.export());

    options = viewOptions().reduce(false);
    assertEquals("reduce=false", options.export());
  }

  @Test
  void shouldLimit() {
    ViewOptions options = viewOptions().limit(10);
    assertEquals("limit=10", options.export());
  }

  @Test
  void shouldSkip() {
    ViewOptions options = viewOptions().skip(3);
    assertEquals("skip=3", options.export());
  }

  @Test
  void shouldGroup() {
    ViewOptions options = viewOptions().group(true);
    assertEquals("group=true", options.export());

    options = viewOptions().group(false);
    assertEquals("group=false", options.export());
  }

  @Test
  void shouldGroupLevel() {
    ViewOptions options = viewOptions().groupLevel(2);
    assertEquals("group_level=2", options.export());
  }

  @Test
  void shouldSetInclusiveEnd() {
    ViewOptions options = viewOptions().inclusiveEnd(true);
    assertEquals("inclusive_end=true", options.export());

    options = viewOptions().inclusiveEnd(false);
    assertEquals("inclusive_end=false", options.export());
  }

  @Test
  void shouldSetStale() {
    ViewOptions options = viewOptions().stale(Stale.FALSE);
    assertEquals("stale=false", options.export());

    options = viewOptions().stale(Stale.TRUE);
    assertEquals("stale=ok", options.export());

    options = viewOptions().stale(Stale.UPDATE_AFTER);
    assertEquals("stale=update_after", options.export());
  }

  @Test
  void shouldSetOnError() {
    ViewOptions options = viewOptions().onError(OnError.CONTINUE);
    assertEquals("on_error=continue", options.export());

    options = viewOptions().onError(OnError.STOP);
    assertEquals("on_error=stop", options.export());
  }

  @Test
  void shouldSetDebug() {
    ViewOptions options = viewOptions().debug(true);
    assertEquals("debug=true", options.export());

    options = viewOptions().debug(false);
    assertEquals("debug=false", options.export());
  }

  @Test
  void shouldSetDescending() {
    ViewOptions options = viewOptions().descending(true);
    assertEquals("descending=true", options.export());

    options = viewOptions().descending(false);
    assertEquals("descending=false", options.export());
  }

  @Test
  void shouldHandleKey() {
    ViewOptions options = viewOptions().key("key");
    assertEquals("key=%22key%22", options.export());

    options = viewOptions().key(1);
    assertEquals("key=1", options.export());

    options = viewOptions().key(true);
    assertEquals("key=true", options.export());

    options = viewOptions().key(3.55);
    assertEquals("key=3.55", options.export());

    options = viewOptions().key(JsonArray.from("foo", 3));
    assertEquals("key=%5B%22foo%22%2C3%5D", options.export());

    options = viewOptions().key(JsonObject.empty().put("foo", true));
    assertEquals("key=%7B%22foo%22%3Atrue%7D", options.export());
  }

  @Test
  void shouldHandleKeys() {
    JsonArray keysArray = JsonArray.from("foo", 3, true);
    ViewOptions options = viewOptions().keys(keysArray);
    assertEquals("", options.export());
    assertEquals(keysArray.toString(), options.build().keys());
  }

  @Test
  void shouldOutputSmallKeysInToString() {
    JsonArray keysArray = JsonArray.from("foo", 3, true);
    ViewOptions options = viewOptions().keys(keysArray);
    assertEquals("", options.export());
    assertEquals("ViewQuery{params=\"\", keys=\"[\"foo\",3,true]\"}", options.toString());
  }

  @Test
  void shouldTruncateLargeKeysInToString() {
    StringBuilder largeString = new StringBuilder(142);
    for (int i = 0; i < 140; i++) {
      largeString.append('a');
    }
    largeString.append("bc");
    JsonArray keysArray = JsonArray.from(largeString.toString());
    ViewOptions options = viewOptions().keys(keysArray);
    assertEquals("", options.export());
    assertEquals("ViewQuery{params=\"\", keys=\"[\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
      "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
      "aaaaaaaaaaaaaaaaaaaaaaaaaaa...\"(146 chars total)}", options.toString());
  }

  @Test
  void shouldHandleStartKey() {
    ViewOptions options = viewOptions().startKey("key");
    assertEquals("startkey=%22key%22", options.export());

    options = viewOptions().startKey(1);
    assertEquals("startkey=1", options.export());

    options = viewOptions().startKey(true);
    assertEquals("startkey=true", options.export());

    options = viewOptions().startKey(3.55);
    assertEquals("startkey=3.55", options.export());

    options = viewOptions().startKey(JsonArray.from("foo", 3));
    assertEquals("startkey=%5B%22foo%22%2C3%5D", options.export());

    options = viewOptions().startKey(JsonObject.empty().put("foo", true));
    assertEquals("startkey=%7B%22foo%22%3Atrue%7D", options.export());
  }

  @Test
  void shouldHandleEndKey() {
    ViewOptions options = viewOptions().endKey("key");
    assertEquals("endkey=%22key%22", options.export());

    options = viewOptions().endKey(1);
    assertEquals("endkey=1", options.export());

    options = viewOptions().endKey(true);
    assertEquals("endkey=true", options.export());

    options = viewOptions().endKey(3.55);
    assertEquals("endkey=3.55", options.export());

    options = viewOptions().endKey(JsonArray.from("foo", 3));
    assertEquals("endkey=%5B%22foo%22%2C3%5D", options.export());

    options = viewOptions().endKey(JsonObject.empty().put("foo", true));
    assertEquals("endkey=%7B%22foo%22%3Atrue%7D", options.export());
  }

  @Test
  void shouldHandleStartKeyDocID() {
    ViewOptions options = viewOptions().startKeyDocId("mykey");
    assertEquals("startkey_docid=mykey", options.export());
  }

  @Test
  void shouldHandleEndKeyDocID() {
    ViewOptions options = viewOptions().endKeyDocId("mykey");
    assertEquals("endkey_docid=mykey", options.export());
  }

  @Test
  void shouldRespectDevelopmentParam() {
    ViewOptions options = viewOptions().development(true);
    assertTrue(options.build().development());

    options = viewOptions().development(false);
    assertFalse(options.build().development());
  }

  @Test
  void shouldConcatMoreParams() {
    ViewOptions options = viewOptions()
      .descending(true)
      .debug(true)
      .development(true)
      .group(true)
      .reduce(false)
      .startKey(JsonArray.from("foo", true));
    assertEquals("reduce=false&group=true&debug=true&descending=true&startkey=%5B%22foo%22%2Ctrue%5D",
      options.export());
  }

  @Test
  void shouldDisallowNegativeLimit() {
    assertThrows(IllegalArgumentException.class, () -> viewOptions().limit(-1));
  }

  @Test
  void shouldDisallowNegativeSkip() {
    assertThrows(IllegalArgumentException.class, () -> viewOptions().skip(-1));
  }

  @Test
  void shouldToggleDevelopment() {
    ViewOptions options = viewOptions().development(true);
    assertTrue(options.build().development());

    options = viewOptions().development(false);
    assertFalse(options.build().development());
  }

  @Test
  void shouldStoreKeysAsJsonOutsideParams() {
    JsonArray keys = JsonArray.create().add("1").add("2").add("3");
    String keysJson = keys.toString();
    ViewOptions options = viewOptions();
    assertNull(options.build().keys());

    options.keys(keys);
    assertEquals(keysJson, options.build().keys());
    assertFalse(options.export().contains("keys="));
    assertFalse(options.export().contains("3"));
  }

}