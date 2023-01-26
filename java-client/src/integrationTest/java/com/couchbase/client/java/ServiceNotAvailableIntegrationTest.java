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

package com.couchbase.client.java;

import com.couchbase.client.core.error.ServiceNotAvailableException;
import com.couchbase.client.java.search.SearchQuery;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.Capabilities;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Verifies that if certain services are not present they fail with an exception.
 * <p>
 * Note that KV, Views and Manager are not tested, because there always needs to be at least
 * one of them in the cluster to function.
 */
public class ServiceNotAvailableIntegrationTest extends JavaIntegrationTest {

  static private Cluster cluster;

  @BeforeAll
  static void beforeAll() {
    cluster = createCluster();
    Bucket bucket = cluster.bucket(config().bucketname());
    bucket.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);
  }

  @AfterAll
  static void afterAll() {
    cluster.disconnect();
  }

  @Test
  @IgnoreWhen(hasCapabilities = Capabilities.QUERY, clusterTypes = ClusterType.MOCKED)
  void shouldFailIfQueryNotPresent() {
    assertThrows(ServiceNotAvailableException.class, () -> cluster.query("select 1=1"));
  }

  @Test
  @IgnoreWhen(hasCapabilities = Capabilities.ANALYTICS,
    // Fails with FeatureNotAvailableException instead
    isProtostellar = true)
  void shouldFailIfAnalyticsNotPresent() {
    assertThrows(ServiceNotAvailableException.class, () -> cluster.analyticsQuery("select 1=1"));
  }

  @Test
  @IgnoreWhen(hasCapabilities = Capabilities.SEARCH,
    // Fails with FeatureNotAvailableException instead
    isProtostellar = true)
  void shouldFailIfSearchNotPresent() {
    assertThrows(
      ServiceNotAvailableException.class,
      () -> cluster.searchQuery("foo", SearchQuery.queryString("bar"))
    );
  }

}
