/*
 * Copyright 2021 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.java.manager.analytics;

import com.couchbase.client.core.error.InvalidArgumentException;
import org.junit.jupiter.api.Test;

import static com.couchbase.client.core.util.CbCollections.mapOf;
import static com.couchbase.client.java.manager.analytics.AsyncAnalyticsIndexManager.formatIndexFields;
import static com.couchbase.client.java.manager.analytics.AsyncAnalyticsIndexManager.quoteDataverse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AsyncAnalyticsIndexManagerTest {

  @Test
  void canQuoteCompoundDataverseNames() {
    assertEquals("`foo`", quoteDataverse("foo"));
    assertEquals("`foo`.`bar`.`zot`", quoteDataverse("foo/bar/zot"));
    assertEquals("`foo`.`bar`.`zot`", quoteDataverse("foo/bar", "zot"));
  }

  @Test
  void convertsSlashesOnlyinDataverseName() {
    assertEquals("`foo`.`bar/zot`", quoteDataverse("foo", "bar/zot"));
  }

  @Test
  void rejectsPreQuoted() {
    assertThrows(InvalidArgumentException.class, () -> quoteDataverse("`foo`"));
  }

  @Test
  void escapesIndexFields() {
    String result = formatIndexFields(mapOf("foo", AnalyticsDataType.INT64, "bar", AnalyticsDataType.STRING));
    assertEquals("(`bar`:string,`foo`:int64)", result);

    assertThrows(InvalidArgumentException.class, () -> formatIndexFields(mapOf("`foo`", AnalyticsDataType.INT64)));
  }
}
