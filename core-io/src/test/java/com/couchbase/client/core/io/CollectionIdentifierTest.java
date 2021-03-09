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

package com.couchbase.client.core.io;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class CollectionIdentifierTest {

  /**
   * Since so many invariants in the SDK rely on the fact that two instances are equal based on their inner
   * properties, add a couple regression tests to make sure.
   */
  @Test
  void ensureEquals() {
    assertEquals(CollectionIdentifier.fromDefault("foo"), CollectionIdentifier.fromDefault("foo"));
    assertNotEquals(CollectionIdentifier.fromDefault("foo"), CollectionIdentifier.fromDefault("bar"));

    assertEquals(
      new CollectionIdentifier("foo", Optional.of("scope"), Optional.of("collection")),
      new CollectionIdentifier("foo", Optional.of("scope"), Optional.of("collection"))
    );
    assertNotEquals(
      new CollectionIdentifier("foo", Optional.of("scope"), Optional.of("collection1")),
      new CollectionIdentifier("foo", Optional.of("scope"), Optional.of("collection"))
    );
    assertNotEquals(
      new CollectionIdentifier("foo", Optional.of("scope1"), Optional.of("collection")),
      new CollectionIdentifier("foo", Optional.of("scope"), Optional.of("collection"))
    );
    assertNotEquals(
      new CollectionIdentifier("foo1", Optional.of("scope"), Optional.of("collection")),
      new CollectionIdentifier("foo", Optional.of("scope"), Optional.of("collection"))
    );

    assertEquals(
      new CollectionIdentifier("foo", Optional.empty(), Optional.of("collection")),
      new CollectionIdentifier("foo", Optional.empty(), Optional.of("collection"))
    );
    assertEquals(
      new CollectionIdentifier("foo", Optional.of("scope"), Optional.empty()),
      new CollectionIdentifier("foo", Optional.of("scope"), Optional.empty())
    );
    assertEquals(
      new CollectionIdentifier("foo", Optional.empty(), Optional.empty()),
      new CollectionIdentifier("foo", Optional.empty(), Optional.empty())
    );
  }

}