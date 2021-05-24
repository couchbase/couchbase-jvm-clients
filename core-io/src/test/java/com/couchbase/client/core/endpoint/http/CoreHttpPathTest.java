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

package com.couchbase.client.core.endpoint.http;

import org.junit.jupiter.api.Test;

import static com.couchbase.client.core.endpoint.http.CoreHttpPath.path;
import static com.couchbase.client.core.util.CbCollections.mapOf;
import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CoreHttpPathTest {

  @Test
  void withoutPlaceholders() {
    CoreHttpPath path = path("/foo");
    assertEquals("/foo", path.format());
    assertEquals("/foo", path.getTemplate());
    assertEquals(emptyMap(), path.getParams());
  }

  @Test
  void addsLeadingSlashIfMissing() {
    assertEquals("/foo", path("foo").format());
  }

  @Test
  void withPlaceholders() {
    CoreHttpPath path = path("/foo/{color}/{flavor}", mapOf(
        "color", "red",
        "flavor", "strawberry"
    ));

    assertEquals("/foo/red/strawberry", path.format());
    assertEquals("/foo/{color}/{flavor}", path.getTemplate());
    assertEquals(mapOf(
        "color", "red",
        "flavor", "strawberry"
    ), path.getParams());
  }

  @Test
  void replacementValuesAreUrlEncoded() {
    CoreHttpPath path = path("/{x}", mapOf("x", "foo /bar"));
    assertEquals("/foo%20%2Fbar", path.format());
    // getParams should return un-encoded values
    assertEquals(mapOf("x", "foo /bar"), path.getParams());
  }

  @Test
  void replacementValueMayContainCurlyBraces() {
    assertEquals(
        "/%7Bfoo%7D",
        path("/{x}", mapOf("x", "{foo}")).format());
  }

  @Test
  void complainsAboutMissingPlaceholders() {
    assertThrows(IllegalArgumentException.class, () ->
        path("/{foo}"));

    assertThrows(IllegalArgumentException.class, () ->
        path("/foo/{color}/{flavor}", mapOf("x", "y")));
  }
}
