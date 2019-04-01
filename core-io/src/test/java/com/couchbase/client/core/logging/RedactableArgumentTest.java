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

package com.couchbase.client.core.logging;

import com.couchbase.client.core.json.Mapper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static com.couchbase.client.core.logging.RedactableArgument.redactSystem;
import static com.couchbase.client.core.logging.RedactableArgument.redactUser;
import static org.junit.Assert.assertEquals;

public class RedactableArgumentTest {
  private static RedactionLevel origLevel;

  @BeforeClass
  public static void saveOrigLevel() {
    origLevel = LogRedaction.getRedactionLevel();
  }

  @AfterClass
  public static void restoreOrigLevel() {
    LogRedaction.setRedactionLevel(origLevel);
  }

  @Test
  public void shouldNotRedactLogsWhenDisabled() {
    LogRedaction.setRedactionLevel(RedactionLevel.NONE);

    assertEquals("1", redactUser(1).toString());
    assertEquals("null", redactMeta(null).toString());
    assertEquals("system", redactSystem("system").toString());
  }

  @Test
  public void shouldOnlyRedactUserOnPartial() {
    LogRedaction.setRedactionLevel(RedactionLevel.PARTIAL);

    assertEquals("<ud>user</ud>", redactUser("user").toString());
    assertEquals("meta", redactMeta("meta").toString());
    assertEquals("system", redactSystem("system").toString());
  }

  @Test
  public void forNowShouldRedactOnlyUserOnFull() {
    LogRedaction.setRedactionLevel(RedactionLevel.FULL);

    assertEquals("<ud>user</ud>", redactUser("user").toString());
    assertEquals("meta", redactMeta("meta").toString());
    assertEquals("system", redactSystem("system").toString());
  }

  @Test
  public void jsonSerialization() {
    LogRedaction.setRedactionLevel(RedactionLevel.FULL);

    String json = Mapper.encodeAsString(redactUser("bar"));
    assertEquals("\"<ud>bar</ud>\"", json);
  }
}
