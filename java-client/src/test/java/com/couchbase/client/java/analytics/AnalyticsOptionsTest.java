/*
 * Copyright (c) 2022 Couchbase, Inc.
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

package com.couchbase.client.java.analytics;

import com.couchbase.client.java.json.JsonObject;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static com.couchbase.client.java.analytics.AnalyticsOptions.analyticsOptions;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AnalyticsOptionsTest {

  @Test
  void testAlwaysSetClientContextId() {
    JsonObject result = build(analyticsOptions());
    assertNotNull(result.getString("client_context_id"));
  }

  @Test
  void testCustomClientContextId() {
    JsonObject result = build(analyticsOptions().clientContextId("customClientContextId"));
    assertEquals("customClientContextId", result.getString("client_context_id"));
  }

  @Test
  void testScanWait() {
    JsonObject result = build(
      analyticsOptions().scanWait(Duration.ofSeconds(2)).scanConsistency(AnalyticsScanConsistency.REQUEST_PLUS)
    );
    assertEquals("2000ms", result.getString("scan_wait"));

    result = build(analyticsOptions().scanWait(Duration.ofSeconds(2)));
    assertFalse(result.containsKey("scan_wait"));

    result = build(
      analyticsOptions().scanWait(Duration.ofSeconds(2)).scanConsistency(AnalyticsScanConsistency.NOT_BOUNDED)
    );
    assertFalse(result.containsKey("scan_wait"));
  }

  @Test
  void testReadonly() {
    JsonObject result = build(analyticsOptions().readonly(true));
    assertTrue(result.getBoolean("readonly"));
  }

  private static JsonObject build(final AnalyticsOptions options) {
    JsonObject export = JsonObject.create();
    options.build().injectParams(export);
    return export;
  }

}