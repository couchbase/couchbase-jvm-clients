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

package com.couchbase.client.java.http;

import com.couchbase.client.core.node.NodeIdentifier;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.Capabilities;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.IgnoreWhen;
import com.couchbase.client.test.Services;
import com.couchbase.client.test.TestNodeConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;

import static com.couchbase.client.core.util.CbCollections.mapOf;
import static com.couchbase.client.java.http.HttpPutOptions.httpPutOptions;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@IgnoreWhen(clusterTypes = {ClusterType.MOCKED, ClusterType.CAVES},
  isProtostellar = true)
class CouchbaseHttpClientIntegrationTest extends JavaIntegrationTest {

  private static Cluster cluster;
  private static CouchbaseHttpClient httpClient;

  @BeforeAll
  static void setup() {
    cluster = createCluster();
    httpClient = cluster.httpClient();

    // need to wait, otherwise targeting a specific node fails
    cluster.bucket(config().bucketname()).waitUntilReady(WAIT_UNTIL_READY_DEFAULT);
  }

  @AfterAll
  static void tearDown() {
    cluster.disconnect();
  }

  @Test
  void canReadStatusCodeForBadRequestManager() {
    assert404(HttpTarget.manager());
  }

  @Test
  @IgnoreWhen(missesCapabilities = Capabilities.ANALYTICS)
  void canReadStatusCodeForBadRequestAnalytics() {
    assert404(HttpTarget.analytics());
  }

  @Test
  @IgnoreWhen(missesCapabilities = Capabilities.QUERY)
  void canReadStatusCodeForBadRequestQuery() {
    assert404(HttpTarget.query());
  }

  @Test
  @IgnoreWhen(missesCapabilities = Capabilities.SEARCH)
  void canReadStatusCodeForBadRequestSearch() {
    assert404(HttpTarget.search());
  }

  @Test
  @IgnoreWhen(missesCapabilities = Capabilities.EVENTING)
  void canReadStatusCodeForBadRequestEventing() {
    assert404(HttpTarget.eventing());
  }

  NodeIdentifier firstNode() {
    TestNodeConfig n = config().nodes().get(0);
    return new NodeIdentifier(n.hostname(), n.ports().get(Services.MANAGER));
  }

  // On 6.0 and below, fails with:
  // Request failed with status code 400 and body: {"errors":{"roles":"The value must be supplied"}}
  @IgnoreWhen(missesCapabilities = {Capabilities.CREATE_AS_DELETED}, clusterTypes = ClusterType.CAPELLA)
  @Test
  void canIssueSuccessfulRequests() {
    // send all requests to same node
    HttpTarget target = HttpTarget.manager().withNode(firstNode());

    String username = UUID.randomUUID().toString();
    HttpPath path = HttpPath.of("/settings/rbac/users/{}/{}", "local", username);

    HttpResponse res;

    res = httpClient.put(target, path, httpPutOptions()
        .body(HttpBody.form(mapOf("password", "it's a secret to everybody"))));
    assertSuccess(res);

    res = httpClient.get(target, path);
    assertSuccess(res);
    assertTrue(res.contentAsString().contains(username));

    res = httpClient.delete(target, path);
    assertSuccess(res);

    res = httpClient.get(target, path);
    assertEquals(404, res.statusCode());
  }

  /**
   * verifies the handler isn't doing unwanted exception translation
   */
  void assert404(HttpTarget target) {
    HttpResponse res = httpClient.get(target, HttpPath.of("/bogus/path"));
    assertEquals(404, res.statusCode());
  }

  void assertSuccess(HttpResponse res) {
    assertTrue(res.success(), "Request failed with status code " + res.statusCode() + " and body: " + res.contentAsString());
  }
}
