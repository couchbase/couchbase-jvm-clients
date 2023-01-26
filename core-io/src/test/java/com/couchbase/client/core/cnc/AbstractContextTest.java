/*
 * Copyright (c) 2021 Couchbase, Inc.
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

package com.couchbase.client.core.cnc;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.couchbase.client.core.util.CbCollections.mapOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies the basic functionality of the Abstract Context.
 */
class AbstractContextTest {

  @Test
  void exportAsJson() {
    AbstractContext ctx = new AbstractContext() {
      @Override
      public void injectExportableParams(Map<String, Object> input) {
        input.put("foo", "bar");
      }
    };
    assertEquals("{\"foo\":\"bar\"}", ctx.exportAsString(Context.ExportFormat.JSON));
  }

  @Test
  void exportAsJsonPretty() {
    AbstractContext ctx = new AbstractContext() {
      @Override
      public void injectExportableParams(Map<String, Object> input) {
        input.put("foo", "bar");
      }
    };
    assertThat("{\n  \"foo\" : \"bar\"\n}").isEqualToIgnoringNewLines(ctx.exportAsString(Context.ExportFormat.JSON_PRETTY));
  }

  @Test
  void exportAsMap() {
    AbstractContext ctx = new AbstractContext() {
      @Override
      public void injectExportableParams(Map<String, Object> input) {
        input.put("foo", "bar");
      }
    };

    Map<String, Object> expected = mapOf("foo", "bar");
    assertEquals(expected, ctx.exportAsMap());
  }
}
