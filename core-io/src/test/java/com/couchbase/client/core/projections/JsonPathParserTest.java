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
package com.couchbase.client.core.projections;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JsonPathParserTest {
  @Test
  void name() {
    assertEquals(JsonPathParser.parse("name"), Collections.singletonList(new PathObjectOrField("name")));
  }

  @Test
  void secondElementOfArray() {
    assertEquals(JsonPathParser.parse("foo[2]"), Collections.singletonList(new PathArray("foo", 2)));
  }

  @Test
  void fooBar() {
    assertEquals(
      JsonPathParser.parse("foo.bar"), Arrays.asList(new PathObjectOrField("foo"), new PathObjectOrField("bar"))
    );
  }

  @Test
  void fooBarSecondItemFromArray() {
    assertEquals(
      JsonPathParser.parse("foo.bar[2]"), Arrays.asList(new PathObjectOrField("foo"), new PathArray("bar", 2))
    );
  }

  @Test
  void secondItemFromFooThenBar() {
    assertEquals(
      JsonPathParser.parse("foo[2].bar"), Arrays.asList(new PathArray("foo", 2), new PathObjectOrField("bar"))
    );
  }

  @Test
  void largeIndexFromFooThenBar() {
    assertEquals(
      JsonPathParser
        .parse("foo[9999].bar")
        , Arrays.asList(new PathArray("foo", 9999), new PathObjectOrField("bar"))
    );
  }

  @Test
  void secondItemFromFooThenBarThenBaz() {
    assertEquals(
      JsonPathParser
        .parse("foo[2].bar[80].baz")
        , Arrays.asList(new PathArray("foo", 2), new PathArray("bar", 80), new PathObjectOrField("baz"))
    );
  }

  @Test
  void badIdx() {
    assertThrows(IllegalArgumentException.class, () ->  JsonPathParser.parse("foo[bad]"));
  }

  @Test
  void missingIndexEnd() {
    assertThrows(IllegalArgumentException.class, () ->  JsonPathParser.parse("foo[12"));
  }

  @Test
  void empty() {
    assertEquals(JsonPathParser.parse(""), Collections.emptyList());
  }
}
