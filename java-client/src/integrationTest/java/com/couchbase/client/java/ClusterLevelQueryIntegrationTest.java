/*
 * Copyright (c) 2019 Couchbase, Inc.
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

package com.couchbase.client.java;

import com.couchbase.client.core.error.FeatureNotAvailableException;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.Capabilities;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static com.couchbase.client.java.query.QueryOptions.queryOptions;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ClusterLevelQueryIntegrationTest extends JavaIntegrationTest {

  private static Cluster cluster;

  @BeforeAll
  static void setup() {
    cluster = createCluster();
  }

  @AfterAll
  static void tearDown() {
    cluster.disconnect();
  }

  @Test
  @IgnoreWhen(missesCapabilities = {Capabilities.GLOBAL_CONFIG, Capabilities.QUERY}, clusterTypes = ClusterType.CAVES)
  void performsClusterLevelQueryWithoutOpenBucket() {
    cluster.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);

    QueryResult result = cluster.query("select 1=1", queryOptions().clientContextId("my-context-id"));
    assertEquals(1, result.rowsAsObject().size());
    assertEquals("my-context-id", result.metaData().clientContextId());
  }

  @Test
  @IgnoreWhen(hasCapabilities = Capabilities.GLOBAL_CONFIG, clusterTypes = ClusterType.MOCKED)
  void failsIfNoBucketOpenAndNoClusterLevelAvailable() {
    assertThrows(FeatureNotAvailableException.class, () -> cluster.query("select 1=1"));
  }

}
