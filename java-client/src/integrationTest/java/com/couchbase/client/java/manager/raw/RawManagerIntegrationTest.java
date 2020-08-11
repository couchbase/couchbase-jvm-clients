/*
 * Copyright 2020 Couchbase, Inc.
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

package com.couchbase.client.java.manager.raw;

import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.IgnoreWhen;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static com.couchbase.client.test.ClusterType.MOCKED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@IgnoreWhen(clusterTypes = MOCKED)
public class RawManagerIntegrationTest extends JavaIntegrationTest {

  private static Cluster cluster;

  @BeforeAll
  static void setup() {
    cluster = Cluster.connect(seedNodes(), clusterOptions());

    // required for pre-GCCCP servers (< 6.5)
    Bucket bucket = cluster.bucket(config().bucketname());
    bucket.waitUntilReady(Duration.ofSeconds(5));
  }

  @AfterAll
  static void tearDown() {
    cluster.disconnect();
  }

  @Test
  void loadsImplementationVersionFromPools() {
    RawManagerRequest request = RawManagerRequest.get(ServiceType.MANAGER, "/pools");
    RawManagerResponse response = RawManager.call(cluster, request).block();

    assertNotNull(response);
    PoolsInfo poolsInfo = response.contentAs(PoolsInfo.class);
    assertNotNull(poolsInfo.implementationVersion);
  }

  @Test
  void callsNonExistentUri() {
    RawManagerRequest request = RawManagerRequest.get(ServiceType.MANAGER, "/poolsDoesNotExist");
    RawManagerResponse response = RawManager.call(cluster, request).block();

    assertNotNull(response);
    assertEquals(404, response.httpStatus());
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  static class PoolsInfo {
    public String implementationVersion;
  }

}
