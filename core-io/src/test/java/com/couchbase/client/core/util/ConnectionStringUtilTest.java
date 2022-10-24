/*
 * Copyright 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.util;

import com.couchbase.client.core.env.SeedNode;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static com.couchbase.client.core.util.CbCollections.setOf;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ConnectionStringUtilTest {

  @Test
  void canOmitPort() {
    assertSeedNodesMatch(
        "foo",
        setOf(
            SeedNode.create("foo")
        )
    );
  }

  @Test
  void portTypeDefaultsToKv() {
    assertSeedNodesMatch(
        "foo:123",
        setOf(
            SeedNode.create("foo").withKvPort(123)
        )
    );
  }

  @Test
  void canSpecifyPortTypes() {
    assertSeedNodesMatch(
        "foo:123=kv,bar:456=manager",
        setOf(
            SeedNode.create("foo").withKvPort(123),
            SeedNode.create("bar").withManagerPort(456)
        )
    );
  }

  @Test
  void duplicateHostsAreMerged() {
    assertSeedNodesMatch(
        "foo:123=kv,foo:456=manager",
        setOf(
            SeedNode.create("foo").withKvPort(123).withManagerPort(456)
        )
    );

    assertSeedNodesMatch(
        "foo:456=manager,foo",
        setOf(
            SeedNode.create("foo").withManagerPort(456)
        )
    );
  }

  @Test
  void lastPortWinsForDuplicateHost() {
    // Not ideal, since multiple nodes could be running on the same host at different ports.
    assertSeedNodesMatch(
        "foo:123,foo:456,foo:567=manager,foo:890=manager,bar,bar:321",
        setOf(
            SeedNode.create("foo").withKvPort(456).withManagerPort(890),
            SeedNode.create("bar").withKvPort(321)
        )
    );
  }

  private static void assertSeedNodesMatch(String connectionString, Set<SeedNode> expected) {
    ConnectionString cs = ConnectionString.create(connectionString);
    Set<SeedNode> actual = ConnectionStringUtil.populateSeedsFromConnectionString(cs);
    assertEquals(expected, actual);
  }

}
