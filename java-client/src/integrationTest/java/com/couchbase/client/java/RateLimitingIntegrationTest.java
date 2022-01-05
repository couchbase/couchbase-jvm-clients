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

package com.couchbase.client.java;

import com.couchbase.client.core.cnc.SimpleEventBus;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.endpoint.http.CoreHttpClient;
import com.couchbase.client.core.endpoint.http.CoreHttpResponse;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.QuotaLimitedException;
import com.couchbase.client.core.error.RateLimitedException;
import com.couchbase.client.core.error.TimeoutException;
import com.couchbase.client.core.error.UnambiguousTimeoutException;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.msg.RequestTarget;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.util.CbCollections;
import com.couchbase.client.core.util.UrlQueryStringBuilder;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.manager.collection.CollectionManager;
import com.couchbase.client.java.manager.collection.CollectionSpec;
import com.couchbase.client.java.manager.search.SearchIndex;
import com.couchbase.client.java.search.SearchOptions;
import com.couchbase.client.java.search.queries.QueryStringQuery;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.Capabilities;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static com.couchbase.client.core.endpoint.http.CoreHttpPath.path;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies rate and quota limits against clusters which support the "rate limiting" feature.
 */
@IgnoreWhen(missesCapabilities = Capabilities.RATE_LIMITING)
class RateLimitingIntegrationTest extends JavaIntegrationTest {

  private static final String RL_PASSWORD = "password";

  private static Cluster adminCluster;

  private ClusterEnvironment testEnvironment;

  @BeforeAll
  static void beforeAll() throws Exception {
    adminCluster = Cluster.connect(seedNodes(), clusterOptions());
    adminCluster.waitUntilReady(Duration.ofSeconds(5));

    enforceRateLimits();
  }

  @AfterAll
  static void afterAll() {
    adminCluster.disconnect();
  }

  @BeforeEach
  void beforeEach() {
    testEnvironment = ClusterEnvironment.builder().eventBus(new SimpleEventBus(true)).build();
  }

  @AfterEach
  void afterEach() {
    testEnvironment.shutdown();
  }

  @Test
  void kvRateLimitMaxCommands() throws Exception {
    String username = "kvRateLimit";

    Limits limits = new Limits();
    limits.keyValueLimits = new KeyValueLimits(10, 10, 10, 10);
    createRateLimitedUser(username, limits);

    try {
      Cluster cluster = Cluster.connect(seedNodes(), ClusterOptions.clusterOptions(username, RL_PASSWORD).environment(testEnvironment));
      Bucket bucket = cluster.bucket(config().bucketname());
      Collection collection = bucket.defaultCollection();
      bucket.waitUntilReady(Duration.ofSeconds(10));

      RateLimitedException ex = assertThrows(RateLimitedException.class, () -> {
        for (int i = 0; i < 30; i++) {
          collection.upsert("ratelimit", "test");
        }
      });
      assertTrue(ex.getMessage().contains("RATE_LIMITED_MAX_COMMANDS"));

      cluster.disconnect();
    } finally {
      adminCluster.users().dropUser(username);
    }
  }

  @Test
  void kvRateLimitIngress() throws Exception {
    String username = "kvRateLimitIngress";

    Limits limits = new Limits();
    limits.keyValueLimits = new KeyValueLimits(10, 100, 1, 10);
    createRateLimitedUser(username, limits);

    try {
      Cluster cluster = Cluster.connect(seedNodes(), ClusterOptions.clusterOptions(username, RL_PASSWORD).environment(testEnvironment));
      Bucket bucket = cluster.bucket(config().bucketname());
      Collection collection = bucket.defaultCollection();
      bucket.waitUntilReady(Duration.ofSeconds(10));

      collection.upsert("ratelimitingress", randomString(1024 * 512));

      RateLimitedException ex = assertThrows(
        RateLimitedException.class,
        () -> collection.upsert("ratelimitingress", randomString(1024 * 512))
      );
      assertTrue(ex.getMessage().contains("RATE_LIMITED_NETWORK_INGRESS"));

      cluster.disconnect();
    } finally {
      adminCluster.users().dropUser(username);
    }
  }

  @Test
  void kvRateLimitEgress() throws Exception {
    String username = "kvRateLimitEgress";

    Limits limits = new Limits();
    limits.keyValueLimits = new KeyValueLimits(10, 100, 10, 1);
    createRateLimitedUser(username, limits);

    try {
      Cluster cluster = Cluster.connect(seedNodes(), ClusterOptions.clusterOptions(username, RL_PASSWORD).environment(testEnvironment));
      Bucket bucket = cluster.bucket(config().bucketname());
      Collection collection = bucket.defaultCollection();
      bucket.waitUntilReady(Duration.ofSeconds(10));

      collection.upsert("ratelimitegress", randomString(1024 * 512));
      collection.get("ratelimitegress");
      collection.get("ratelimitegress");
      RateLimitedException ex = assertThrows(
        RateLimitedException.class,
        () -> collection.get("ratelimitegress")
      );
      assertTrue(ex.getMessage().contains("RATE_LIMITED_NETWORK_EGRESS"));

      cluster.disconnect();
    } finally {
      adminCluster.users().dropUser(username);
    }
  }

  @Test
  void kvRateLimitMaxConnections() throws Exception {
    String username = "kvRateLimitMaxConnections";

    Limits limits = new Limits();
    limits.keyValueLimits = new KeyValueLimits(2, 100, 10, 10);
    createRateLimitedUser(username, limits);

    try {
      Cluster cluster = Cluster.connect(seedNodes(), ClusterOptions.clusterOptions(username, RL_PASSWORD).environment(testEnvironment));
      Bucket bucket = cluster.bucket(config().bucketname());
      bucket.waitUntilReady(Duration.ofSeconds(10));

      Cluster cluster2 = Cluster.connect(seedNodes(), ClusterOptions.clusterOptions(username, RL_PASSWORD).environment(testEnvironment));
      Bucket bucket2 = cluster2.bucket(config().bucketname());
      // The first connect will succeed, but the second will time out due to the num connection limit
      assertThrows(UnambiguousTimeoutException.class, () -> bucket2.waitUntilReady(Duration.ofSeconds(3)));

      cluster.disconnect();
      cluster2.disconnect();
    } finally {
      adminCluster.users().dropUser(username);
    }
  }

  @Test
  void kvQuotaLimitScopesDataSize() throws Exception {
    String scopeName = "ratelimitDataSize2";
    ScopeRateLimits limits = new ScopeRateLimits();
    limits.kv = new KeyValueScopeRateLimit(1024 * 1024);
    createLimitedScope(scopeName, config().bucketname(), limits);

    CollectionManager collectionManager = adminCluster.bucket(config().bucketname()).collections();
    collectionManager.createCollection(CollectionSpec.create(scopeName, scopeName));

    Collection collection = adminCluster.bucket(config().bucketname()).scope(scopeName).collection(scopeName);

    try {
      collection.upsert("ratelimitkvscope", randomString(512));
      assertThrows(
        QuotaLimitedException.class,
        () -> collection.upsert("ratelimitkvscope", randomString(2048))
      );
    } finally {
      collectionManager.dropScope(scopeName);
    }
  }

  @Test
  @IgnoreWhen(missesCapabilities = Capabilities.QUERY)
  void queryRateLimitMaxCommands() throws Exception {
    String username = "queryRateLimit";

    Limits limits = new Limits();
    limits.queryLimits = new QueryLimits(10, 1, 10, 10);
    createRateLimitedUser(username, limits);

    try {
      Cluster cluster = Cluster.connect(seedNodes(), ClusterOptions.clusterOptions(username, RL_PASSWORD).environment(testEnvironment));
      cluster.waitUntilReady(Duration.ofSeconds(10));

      RateLimitedException ex = assertThrows(RateLimitedException.class, () -> {
        for (int i = 0; i < 50; i++) {
          cluster.query("select 1 = 1");
        }
      });
      assertTrue(ex.getMessage().contains("User has exceeded request rate limit"));

      cluster.disconnect();
    } finally {
      adminCluster.users().dropUser(username);
    }
  }

  @Test
  @IgnoreWhen(missesCapabilities = Capabilities.QUERY)
  void queryRateLimitIngress() throws Exception {
    String username = "queryRateLimitIngress";

    Limits limits = new Limits();
    limits.queryLimits = new QueryLimits(10000, 10000, 1, 10);
    createRateLimitedUser(username, limits);

    try {
      Cluster cluster = Cluster.connect(seedNodes(), ClusterOptions.clusterOptions(username, RL_PASSWORD).environment(testEnvironment));
      cluster.waitUntilReady(Duration.ofSeconds(10));

      RateLimitedException ex = assertThrows(RateLimitedException.class, () -> {
        for (int i = 0; i < 50; i++) {
          String content = String.join("", Collections.nCopies(1024 * 1024 * 5, "a"));
          cluster.query("UPSERT INTO `" + config().bucketname() + "` (KEY,VALUE) VALUES (\"key1\", \""+content+"\")");
        }
      });
      assertTrue(ex.getMessage().contains("User has exceeded input network traffic limit"));

      cluster.disconnect();
    } finally {
      adminCluster.users().dropUser(username);
    }
  }

  @Test
  @IgnoreWhen(missesCapabilities = Capabilities.QUERY)
  void queryRateLimitEgress() throws Exception {
    String username = "queryRateLimitEgress";

    Limits limits = new Limits();
    limits.queryLimits = new QueryLimits(10000, 10000, 10, 1);
    createRateLimitedUser(username, limits);

    try {
      Cluster cluster = Cluster.connect(seedNodes(), ClusterOptions.clusterOptions(username, RL_PASSWORD).environment(testEnvironment));
      cluster.waitUntilReady(Duration.ofSeconds(10));

      String content = String.join("", Collections.nCopies(1024 * 1024, "a"));
      cluster.query("UPSERT INTO `" + config().bucketname() + "` (KEY,VALUE) VALUES (\"key1\", \""+content+"\")");

      RateLimitedException ex = assertThrows(RateLimitedException.class, () -> {
        for (int i = 0; i < 50; i++) {
            cluster.query("SELECT * FROM `"+ config().bucketname() +"` USE KEYS [\"key1\"]");
        }
      });

      assertTrue(ex.getMessage().contains("User has exceeded results size limit"));

      cluster.disconnect();
    } finally {
      adminCluster.users().dropUser(username);
    }
  }

  @Test
  @IgnoreWhen(missesCapabilities = Capabilities.QUERY)
  void queryRateLimitConcurrentRequests() throws Exception {
    String username = "queryRateLimitConcurrentRequests";

    Limits limits = new Limits();
    limits.queryLimits = new QueryLimits(1, 100, 10, 10);
    createRateLimitedUser(username, limits);

    try {
      Cluster cluster = Cluster.connect(seedNodes(), ClusterOptions.clusterOptions(username, RL_PASSWORD).environment(testEnvironment));
      cluster.waitUntilReady(Duration.ofSeconds(10));

      RateLimitedException ex = assertThrows(RateLimitedException.class, () -> Flux
        .range(0, 50)
        .flatMap(i -> cluster.reactive().query("select 1=1"))
        .last()
        .block());

      assertTrue(ex.getMessage().contains("User has more requests running than allowed"));

      cluster.disconnect();
    } finally {
      adminCluster.users().dropUser(username);
    }
  }

  @Test
  @IgnoreWhen(missesCapabilities = Capabilities.QUERY)
  void clusterManagerRateLimitConcurrentRequests() throws Exception {
    String username = "clusterManagerRateLimitConcurrentRequests";

    Limits limits = new Limits();
    limits.clusterManagerLimits = new ClusterManagerLimits(1, 10, 10);
    createRateLimitedUser(username, limits);

    try {
      Cluster cluster = Cluster.connect(seedNodes(), ClusterOptions.clusterOptions(username, RL_PASSWORD).environment(testEnvironment));
      cluster.waitUntilReady(Duration.ofSeconds(10));

      RateLimitedException ex = assertThrows(RateLimitedException.class, () -> Flux
        .range(0, 10)
        .flatMap(i -> cluster.reactive().buckets().getAllBuckets())
        .last()
        .block());

      assertTrue(ex.getMessage().contains("Limit(s) exceeded [num_concurrent_requests]"));

      cluster.disconnect();
    } finally {
      adminCluster.users().dropUser(username);
    }
  }

  @Test
  @IgnoreWhen(missesCapabilities = Capabilities.SEARCH)
  void searchRateLimitMaxCommands() throws Exception {
    String username = "searchRateLimit";

    Limits limits = new Limits();
    limits.searchLimits = new SearchLimits(10, 1, 10, 10);
    createRateLimitedUser(username, limits);

    adminCluster.searchIndexes().upsertIndex(new SearchIndex("ratelimits", config().bucketname()));

    try {
      Cluster cluster = Cluster.connect(seedNodes(), ClusterOptions.clusterOptions(username, RL_PASSWORD).environment(testEnvironment));
      cluster.waitUntilReady(Duration.ofSeconds(10));

      RateLimitedException ex = assertThrows(RateLimitedException.class, () -> {
        for (int i = 0; i < 50; i++) {
          try {
            cluster.searchQuery("ratelimits", QueryStringQuery.queryString("a"),
              SearchOptions.searchOptions().timeout(Duration.ofSeconds(1)));
          } catch (TimeoutException e) {
            // continue
          } catch (CouchbaseException e) {
            if (e.getMessage().contains("no planPIndexes")) {
              continue;
            }
            throw e;
          }
        }
      });
      assertTrue(ex.getMessage().contains("num_queries_per_min"));

      cluster.disconnect();
    } finally {
      adminCluster.users().dropUser(username);
      adminCluster.searchIndexes().dropIndex("ratelimits");
    }
  }


  /**
   * Generates a random string to test ingress and egress.
   *
   * @param length the length of the random string.
   * @return the generated random string.
   */
  static String randomString(int length) {
    byte[] array = new byte[length];
    new Random().nextBytes(array);
    return new String(array, StandardCharsets.UTF_8);
  }

  /**
   * Makes sure that the rate limits are enforced on the server.
   */
  static void enforceRateLimits() throws Exception {
    CoreHttpClient client = adminCluster.core().httpClient(RequestTarget.manager());
    CoreHttpResponse result = client
      .post(path("/internalSettings"), CoreCommonOptions.DEFAULT)
      .form(UrlQueryStringBuilder.create().add("enforceLimits", true))
      .exec(adminCluster.core())
      .get();
    assertEquals(ResponseStatus.SUCCESS, result.status());
  }

  /**
   * Creates a user which is rate limited to perform the tests.
   */
  static void createRateLimitedUser(String username, Limits limits) throws Exception {
    Map<String, Object> jsonLimits = new HashMap<>();
    if (limits.keyValueLimits != null) {
      KeyValueLimits kv = limits.keyValueLimits;
      jsonLimits.put("kv", CbCollections.mapOf(
        "num_connections", kv.numConnections,
        "num_ops_per_min", kv.numOpsPerMin,
        "ingress_mib_per_min", kv.ingressMibPerMin,
        "egress_mib_per_min", kv.egressMibPerMin
      ));
    }
    if (limits.queryLimits != null) {
      QueryLimits query = limits.queryLimits;
      jsonLimits.put("query", CbCollections.mapOf(
        "num_queries_per_min", query.numQueriesPerMin,
        "num_concurrent_requests", query.numConcurrentRequests,
        "ingress_mib_per_min", query.ingressMibPerMin,
        "egress_mib_per_min", query.egressMibPerMin
      ));
    }
    if (limits.searchLimits != null) {
      SearchLimits fts = limits.searchLimits;
      jsonLimits.put("fts", CbCollections.mapOf(
        "num_queries_per_min", fts.numQueriesPerMin,
        "num_concurrent_requests", fts.numConcurrentRequests,
        "ingress_mib_per_min", fts.ingressMibPerMin,
        "egress_mib_per_min", fts.egressMibPerMin
      ));
    }
    if (limits.clusterManagerLimits != null) {
      ClusterManagerLimits cm = limits.clusterManagerLimits;
      jsonLimits.put("clusterManager", CbCollections.mapOf(
        "num_concurrent_requests", cm.numConcurrentRequests,
        "ingress_mib_per_min", cm.ingressMibPerMin,
        "egress_mib_per_min", cm.egressMibPerMin
      ));
    }

    UrlQueryStringBuilder formParams = UrlQueryStringBuilder.create();
    formParams.add("password", RL_PASSWORD);
    formParams.add("roles", "admin");
    if (!jsonLimits.isEmpty()) {
      formParams.add("limits", Mapper.encodeAsString(jsonLimits));
    }

    CoreHttpClient client = adminCluster.core().httpClient(RequestTarget.manager());
    CoreHttpResponse result = client
      .put(path("/settings/rbac/users/local/" + username), CoreCommonOptions.DEFAULT)
      .form(formParams)
      .exec(adminCluster.core())
      .get();
    assertEquals(ResponseStatus.SUCCESS, result.status());
  }

  static void createLimitedScope(String name, String bucket, ScopeRateLimits limits) throws Exception {
    Map<String, Object> jsonLimits = new HashMap<>();
    if (limits.kv != null) {
      jsonLimits.put("kv", CbCollections.mapOf(
        "data_size", limits.kv.dataSize
      ));
    }
    if (limits.fts != null) {
      jsonLimits.put("fts", CbCollections.mapOf(
        "num_fts_indexes", limits.fts.numFtsIndexes
      ));
    }
    if (limits.index != null) {
      jsonLimits.put("index", CbCollections.mapOf(
        "num_indexes", limits.index.numIndexes
      ));
    }
    if (limits.clusterManager != null) {
      jsonLimits.put("clusterManager", CbCollections.mapOf(
        "num_collections", limits.clusterManager.numCollections
      ));
    }

    UrlQueryStringBuilder formParams = UrlQueryStringBuilder.create();
    formParams.add("name", name);
    if (!jsonLimits.isEmpty()) {
      formParams.add("limits", Mapper.encodeAsString(jsonLimits));
    }

    CoreHttpClient client = adminCluster.core().httpClient(RequestTarget.manager());
    CoreHttpResponse result = client
      .post(path("/pools/default/buckets/" + bucket + "/scopes"), CoreCommonOptions.DEFAULT)
      .form(formParams)
      .exec(adminCluster.core())
      .get();
    assertEquals(ResponseStatus.SUCCESS, result.status());
  }

  static class Limits {
    KeyValueLimits keyValueLimits = null;
    QueryLimits queryLimits = null;
    SearchLimits searchLimits = null;
    ClusterManagerLimits clusterManagerLimits = null;
  }

  static class KeyValueLimits {
    int numConnections;
    int numOpsPerMin;
    int ingressMibPerMin;
    int egressMibPerMin;

    KeyValueLimits(int numConnections, int numOpsPerMin, int ingressMibPerMin, int egressMibPerMin) {
      this.numConnections = numConnections;
      this.numOpsPerMin = numOpsPerMin;
      this.ingressMibPerMin = ingressMibPerMin;
      this.egressMibPerMin = egressMibPerMin;
    }
  }

  static class QueryLimits {
    int numConcurrentRequests;
    int numQueriesPerMin;
    int ingressMibPerMin;
    int egressMibPerMin;

    QueryLimits(int numConcurrentRequests, int numQueriesPerMin, int ingressMibPerMin, int egressMibPerMin) {
      this.numConcurrentRequests = numConcurrentRequests;
      this.numQueriesPerMin = numQueriesPerMin;
      this.ingressMibPerMin = ingressMibPerMin;
      this.egressMibPerMin = egressMibPerMin;
    }
  }

  static class SearchLimits {
    int numConcurrentRequests;
    int numQueriesPerMin;
    int ingressMibPerMin;
    int egressMibPerMin;

    SearchLimits(int numConcurrentRequests, int numQueriesPerMin, int ingressMibPerMin, int egressMibPerMin) {
      this.numConcurrentRequests = numConcurrentRequests;
      this.numQueriesPerMin = numQueriesPerMin;
      this.ingressMibPerMin = ingressMibPerMin;
      this.egressMibPerMin = egressMibPerMin;
    }
  }

  static class ClusterManagerLimits {
    int numConcurrentRequests;
    int ingressMibPerMin;
    int egressMibPerMin;

    public ClusterManagerLimits(int numConcurrentRequests, int ingressMibPerMin, int egressMibPerMin) {
      this.numConcurrentRequests = numConcurrentRequests;
      this.ingressMibPerMin = ingressMibPerMin;
      this.egressMibPerMin = egressMibPerMin;
    }
  }

  static class ScopeRateLimits {
    KeyValueScopeRateLimit kv;
    SearchScopeRateLimit fts;
    IndexScopeRateLimit index;
    ClusterManagerScopeRateLimit clusterManager;
  }

  static class KeyValueScopeRateLimit {
    int dataSize;

    public KeyValueScopeRateLimit(int dataSize) {
      this.dataSize = dataSize;
    }
  }

  static class SearchScopeRateLimit {
    int numFtsIndexes;
  }

  static class IndexScopeRateLimit {
    int numIndexes;
  }

  static class ClusterManagerScopeRateLimit {
    int numCollections;
  }

}
