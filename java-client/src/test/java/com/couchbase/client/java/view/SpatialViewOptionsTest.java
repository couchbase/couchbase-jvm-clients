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
import org.junit.jupiter.api.Test;

import static com.couchbase.client.java.view.SpatialViewOptions.spatialViewOptions;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SpatialViewOptionsTest {

  @Test
  void shouldSetDefaults() {
    SpatialViewOptions options = spatialViewOptions();
    assertFalse(options.development());
    assertTrue(options.export().isEmpty());
  }

  @Test
  void shouldSetStartRange() {
    SpatialViewOptions options = spatialViewOptions().startRange(JsonArray.from(5.87, 47.27, 1000));
    assertEquals("start_range=[5.87,47.27,1000]", options.export());
  }

  @Test
  void shouldSetEndRange() {
    SpatialViewOptions options = spatialViewOptions().endRange(JsonArray.from(15.04, 55.06, null));
    assertEquals("end_range=[15.04,55.06,null]", options.export());
  }

  @Test
  void shouldSetRange() {
    SpatialViewOptions options = spatialViewOptions()
      .range(JsonArray.from(null, null, 1000), JsonArray.from(null, null, 2000));
    assertEquals("start_range=[null,null,1000]&end_range=[null,null,2000]", options.export());
  }

  @Test
  void shouldLimit() {
    SpatialViewOptions options = spatialViewOptions().limit(10);
    assertEquals("limit=10", options.export());
  }

  @Test
  void shouldSkip() {
    SpatialViewOptions options = spatialViewOptions().skip(3);
    assertEquals("skip=3", options.export());
  }

  @Test
  void shouldSetStale() {
    SpatialViewOptions options = spatialViewOptions().stale(Stale.FALSE);
    assertEquals("stale=false", options.export());

    options = spatialViewOptions().stale(Stale.TRUE);
    assertEquals("stale=ok", options.export());

    options = spatialViewOptions().stale(Stale.UPDATE_AFTER);
    assertEquals("stale=update_after", options.export());
  }

  @Test
  void shouldSetOnError() {
    SpatialViewOptions options = spatialViewOptions().onError(OnError.CONTINUE);
    assertEquals("on_error=continue", options.export());

    options = spatialViewOptions().onError(OnError.STOP);
    assertEquals("on_error=stop", options.export());

  }

  @Test
  void shouldSetDebug() {
    SpatialViewOptions options = spatialViewOptions().debug(true);
    assertEquals("debug=true", options.export());

    options = spatialViewOptions().debug(false);
    assertEquals("debug=false", options.export());
  }

  @Test
  void shouldDisallowNegativeLimit() {
    assertThrows(IllegalArgumentException.class, () -> spatialViewOptions().limit(-1));
  }

  @Test
  void shouldDisallowNegativeSkip() {
    assertThrows(IllegalArgumentException.class, () -> spatialViewOptions().skip(-1));
  }

  @Test
  void shouldToggleDevelopment() {
    SpatialViewOptions options = spatialViewOptions().development(true);
    assertTrue(options.development());

    options = spatialViewOptions().development(false);
    assertFalse(options.development());
  }

}