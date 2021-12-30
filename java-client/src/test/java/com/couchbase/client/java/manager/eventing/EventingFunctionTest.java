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

package com.couchbase.client.java.manager.eventing;

import com.couchbase.client.core.error.InvalidArgumentException;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static com.couchbase.client.test.Util.readResource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EventingFunctionTest {

  @Test
  void canImportFromJson() {
    byte[] func = readResource("single_export.json", EventingFunctionTest.class)
      .getBytes(StandardCharsets.UTF_8);

    List<EventingFunction> eventingFunctions = EventingFunction.fromExportedFunctions(func);
    assertEquals(1, eventingFunctions.size());

    EventingFunction function = eventingFunctions.get(0);
    assertEquals("myfunc", function.name());
  }

  @Test
  void failsOnInvalidJson() {
    assertThrows(
      InvalidArgumentException.class,
      () -> EventingFunction.fromExportedFunctions("[{}]".getBytes(StandardCharsets.UTF_8))
    );
    assertThrows(
      InvalidArgumentException.class,
      () -> EventingFunction.fromExportedFunctions("{}".getBytes(StandardCharsets.UTF_8))
    );
  }

}