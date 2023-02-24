/*
 * Copyright (c) 2020 Couchbase, Inc.
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

package com.couchbase.client.java.search;

import com.couchbase.client.core.api.search.ClassicCoreSearchOps;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static com.couchbase.client.java.search.SearchOptions.searchOptions;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Verifies the functionality of the {@link SearchOptions}.
 */
class SearchOptionsTest {

  private JsonObject test(SearchOptions opts) {
    ObjectNode out = Mapper.createObjectNode();
    ClassicCoreSearchOps.injectOptions("indexName", out, Duration.ofSeconds(1), opts.build());
    return JsonObject.fromJson(out.toString());
  }

  /**
   * Makes sure that (only) when scoring is disabled, it shows up in the resulting query.
   */
  @Test
  void allowToDisableScoring() {
    JsonObject output = test(searchOptions().disableScoring(true));
    assertEquals(output.getString("score"), "none");

    output = test(searchOptions().disableScoring(false));
    assertFalse(output.containsKey("score"));

    output = test(searchOptions());
    assertFalse(output.containsKey("score"));
  }

  /**
   * Makes sure that the list of collection (when provided) are turned into their correct JSON
   * payload.
   */
  @Test
  void canProvideCollections() {
    JsonObject output = test(searchOptions().collections("a", "b"));
    assertEquals(output.getArray("collections"), JsonArray.from("a", "b"));
  }

  @Test
  void injectsTimeout() {
    JsonObject output = test(searchOptions());
    assertEquals(1000, output.getObject("ctl").getLong("timeout"));
  }

}
