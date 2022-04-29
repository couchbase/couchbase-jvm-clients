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
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.util.CbCollections;
import com.couchbase.client.core.util.UrlQueryStringBuilder;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.MutationState;
import com.couchbase.client.java.kv.UpsertOptions;
import com.couchbase.client.java.manager.collection.CollectionManager;
import com.couchbase.client.java.manager.collection.CollectionSpec;
import com.couchbase.client.java.manager.search.SearchIndex;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.search.HighlightStyle;
import com.couchbase.client.java.search.SearchOptions;
import com.couchbase.client.java.search.SearchQuery;
import com.couchbase.client.java.search.queries.QueryStringQuery;
import com.couchbase.client.java.search.result.SearchResult;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.Capabilities;
import com.couchbase.client.test.IgnoreWhen;
import com.couchbase.client.test.Util;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static com.couchbase.client.core.endpoint.http.CoreHttpPath.path;
import static com.couchbase.client.core.util.CbCollections.mapOf;
import static com.couchbase.client.test.Util.waitUntilCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies rate and quota limits against clusters which support the "rate limiting" feature.
 */
@IgnoreWhen(missesCapabilities = Capabilities.RATE_LIMITING)
class RateLimitingIntegrationTest extends JavaIntegrationTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(RateLimitingIntegrationTest.class);

  private static final String RL_PASSWORD = "password";

  private static Cluster adminCluster;
  private static final int LOOP_GUARD = 100;

  @BeforeAll
  static void beforeAll() throws Exception {
    adminCluster = Cluster.connect(seedNodes(), clusterOptions());
    adminCluster.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);

    enforceRateLimits();
  }

  @AfterAll
  static void afterAll() {
    adminCluster.disconnect();
  }

  private static Cluster createTestCluster(String username) {
    return Cluster.connect(seedNodes(), ClusterOptions.clusterOptions(username, RL_PASSWORD)
        .environment(environmentCustomizer().andThen(env -> env.eventBus(new SimpleEventBus(true)))));
  }

  private boolean continueOnSearchError(CouchbaseException e) {
    // These errors are intermittently seen with FTS
    boolean ret = e.getMessage().contains("no planPIndexes")
            || e.getMessage().contains("pindex_consistency mismatched partition")
            || e.getMessage().contains("pindex not available");

    LOGGER.info("Continuing on search error: {}", ret);

    if (ret) {
      try {
        Thread.sleep(50);
      } catch (InterruptedException ex) {
        throw new RuntimeException(ex);
      }
    }

    return ret;
  }

  @Test
  void kvRateLimitMaxCommands() throws Exception {
    String username = "kvRateLimit";

    Limits limits = new Limits();
    limits.keyValueLimits = new KeyValueLimits(10, 10, 10, 10);
    createRateLimitedUser(username, limits);

    Cluster cluster = createTestCluster(username);

    try {
      Bucket bucket = cluster.bucket(config().bucketname());
      Collection collection = bucket.defaultCollection();
      bucket.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);

      RateLimitedException ex = assertThrows(RateLimitedException.class, () -> {
        for (int i = 0; i < 30; i++) {
          collection.upsert("ratelimit", "test");
        }
      });

      assertTrue(ex.getMessage().contains("RATE_LIMITED_MAX_COMMANDS"));
    } finally {
      cluster.disconnect();
      adminCluster.users().dropUser(username);
    }
  }

  @Test
  void kvRateLimitIngress() throws Exception {
    String username = "kvRateLimitIngress";

    Limits limits = new Limits();
    limits.keyValueLimits = new KeyValueLimits(10, 100, 1, 10);
    createRateLimitedUser(username, limits);

    Cluster cluster = createTestCluster(username);

    try {
      Bucket bucket = cluster.bucket(config().bucketname());
      Collection collection = bucket.defaultCollection();
      bucket.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);

      collection.upsert("ratelimitingress", randomString(1024 * 512),
        UpsertOptions.upsertOptions().timeout(Duration.ofSeconds(5)));

      RateLimitedException ex = assertThrows(
        RateLimitedException.class,
        () -> collection.upsert("ratelimitingress", randomString(1024 * 512),
          UpsertOptions.upsertOptions().timeout(Duration.ofSeconds(5)))
      );

      assertTrue(ex.getMessage().contains("RATE_LIMITED_NETWORK_INGRESS"));
    } finally {
      cluster.disconnect();
      adminCluster.users().dropUser(username);
    }
  }

  @Test
  void kvRateLimitEgress() throws Exception {
    String username = "kvRateLimitEgress";

    Limits limits = new Limits();
    limits.keyValueLimits = new KeyValueLimits(10, 100, 10, 1);
    createRateLimitedUser(username, limits);

    Cluster cluster = createTestCluster(username);

    try {
      Bucket bucket = cluster.bucket(config().bucketname());
      Collection collection = bucket.defaultCollection();
      bucket.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);

      collection.upsert("ratelimitegress", randomString(1024 * 512),
        UpsertOptions.upsertOptions().timeout(Duration.ofSeconds(5)));
      collection.get("ratelimitegress");
      collection.get("ratelimitegress");
      RateLimitedException ex = assertThrows(
        RateLimitedException.class,
        () -> collection.get("ratelimitegress")
      );

      assertTrue(ex.getMessage().contains("RATE_LIMITED_NETWORK_EGRESS"));
    } finally {
      cluster.disconnect();
      adminCluster.users().dropUser(username);
    }
  }

  @Test
  void kvRateLimitMaxConnections() throws Exception {
    String username = "kvRateLimitMaxConnections";

    Limits limits = new Limits();
    limits.keyValueLimits = new KeyValueLimits(2, 100, 10, 10);
    createRateLimitedUser(username, limits);

    Cluster cluster = createTestCluster(username);

    try {
      Bucket bucket = cluster.bucket(config().bucketname());
      bucket.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);

      Cluster cluster2 = createTestCluster(username);
      Bucket bucket2 = cluster2.bucket(config().bucketname());
      // The first connect will succeed, but the second will time out due to the num connection limit
      assertThrows(UnambiguousTimeoutException.class, () -> bucket2.waitUntilReady(Duration.ofSeconds(3)));

      cluster2.disconnect();
    } finally {
      cluster.disconnect();
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

    Cluster cluster = createTestCluster(username);

    try {
      cluster.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);

      RateLimitedException ex = assertThrows(RateLimitedException.class, () -> {
        for (int i = 0; i < 50; i++) {
          cluster.query("select 1 = 1");
        }
      });

      assertTrue(ex.getMessage().contains("User has exceeded request rate limit"));
    } finally {
      cluster.disconnect();
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

    Cluster cluster = createTestCluster(username);

    try {
      cluster.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);

      RateLimitedException ex = assertThrows(RateLimitedException.class, () -> {
        for (int i = 0; i < LOOP_GUARD; i++) {
          String content = repeatString(1024 * 1024 * 5, "a");
          try {
            cluster.query("UPSERT INTO `" + config().bucketname() + "` (KEY,VALUE) VALUES (\"key1\", \"" + content + "\")",
                    QueryOptions.queryOptions().timeout(Duration.ofSeconds(30)));//Can take a while
          }
          catch (RateLimitedException err) {
            throw err;
          }
          catch (CouchbaseException err) {
            // On CI seeing intermittent AmbiguousTimeoutException
            LOGGER.info("Caught and ignoring: " + err);
          }
        }
      });

      assertTrue(ex.getMessage().contains("User has exceeded input network traffic limit"));
    } finally {
      cluster.disconnect();
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

    Cluster cluster = createTestCluster(username);

    try {
      cluster.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);

      String content = repeatString(1024 * 1024, "a");
      Collection collection = cluster.bucket(config().bucketname()).defaultCollection();
      collection.upsert("key1", content, UpsertOptions.upsertOptions().timeout(Duration.ofSeconds(5)));

      RateLimitedException ex = assertThrows(RateLimitedException.class, () -> {
        for (int i = 0; i < 50; i++) {
          cluster.query("SELECT * FROM `" + config().bucketname() + "` USE KEYS [\"key1\"]",
            QueryOptions.queryOptions().timeout(Duration.ofSeconds(15)));
        }
      });

      assertTrue(ex.getMessage().contains("User has exceeded results size limit"));
    } finally {
      cluster.disconnect();
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
    Cluster cluster = createTestCluster(username);

    try {
      cluster.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);

      RateLimitedException ex = assertThrows(RateLimitedException.class, () -> Flux
        .range(0, 50)
        .flatMap(i -> cluster.reactive().query("select 1=1"))
        .blockLast());

      assertTrue(ex.getMessage().contains("User has more requests running than allowed"));
    } finally {
      cluster.disconnect();
      adminCluster.users().dropUser(username);
    }
  }

  @Test
  void clusterManagerRateLimitConcurrentRequests() throws Exception {
    String username = "clusterManagerRateLimitConcurrentRequests";

    Limits limits = new Limits();
    limits.clusterManagerLimits = new ClusterManagerLimits(1, 10, 10);
    createRateLimitedUser(username, limits);

    Cluster cluster = createTestCluster(username);

    try {
      cluster.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);

      RateLimitedException ex = assertThrows(RateLimitedException.class, () -> Flux
        .range(0, 10)
        .flatMap(i -> cluster.reactive().buckets().getAllBuckets())
        .blockLast());

      assertTrue(ex.getMessage().contains("Limit(s) exceeded [num_concurrent_requests]"));
    } finally {
      cluster.disconnect();
      adminCluster.users().dropUser(username);
    }
  }

  @Test
  void scopeQuotaLimitExceedMaxCollections() throws Exception {
    String scopeName = "exceedMaxCollections";

    ScopeRateLimits limits = new ScopeRateLimits();
    limits.clusterManager = new ClusterManagerScopeRateLimit(1);
    createLimitedScope(scopeName, config().bucketname(), limits);

    CollectionManager collectionManager = adminCluster.bucket(config().bucketname()).collections();

    try {
      collectionManager.createCollection(CollectionSpec.create("collection1", scopeName));

      assertThrows(QuotaLimitedException.class, () ->
        collectionManager.createCollection(CollectionSpec.create("collection2", scopeName)));
    } finally {
      collectionManager.dropScope(scopeName);
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
    Cluster cluster = createTestCluster(username);

    try {
      cluster.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);

      RateLimitedException ex = assertThrows(RateLimitedException.class, () -> {
        for (int i = 0; i < LOOP_GUARD; i++) {
          try {
            cluster.searchQuery("ratelimits", QueryStringQuery.queryString("a"),
              SearchOptions.searchOptions().timeout(Duration.ofSeconds(1)));
          } catch (TimeoutException e) {
            // continue
          } catch (CouchbaseException e) {
            LOGGER.info("Caught: " + e);
            if (continueOnSearchError(e)) {
              continue;
            }
            throw e;
          }
        }
      });

      assertTrue(ex.getMessage().contains("num_queries_per_min"));
    } finally {
      cluster.disconnect();
      adminCluster.users().dropUser(username);
      adminCluster.searchIndexes().dropIndex("ratelimits");
    }
  }

  @Test
  @IgnoreWhen(missesCapabilities = Capabilities.SEARCH)
  void searchRateLimitMaxEgress() throws Exception {
    String username = "searchRateLimitEgress";

    Limits limits = new Limits();
    limits.searchLimits = new SearchLimits(10, 100, 10, 1);
    createRateLimitedUser(username, limits);

    adminCluster.searchIndexes().upsertIndex(new SearchIndex("ratelimits-egress", config().bucketname()));
    Cluster cluster = createTestCluster(username);

    try {
      cluster.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);

      String content = repeatString(1024 * 1024, "a");
      MutationResult mutationResult = cluster.bucket(config().bucketname()).defaultCollection()
        .upsert("fts-egress", JsonObject.create().put("content", content),
          UpsertOptions.upsertOptions().timeout(Duration.ofSeconds(10)));

      RateLimitedException ex = assertThrows(RateLimitedException.class, () -> {
        for (int i = 0; i < LOOP_GUARD; i++) {
          try {
            cluster.searchQuery("ratelimits-egress", SearchQuery.wildcard("a*"),
              SearchOptions.searchOptions().timeout(Duration.ofSeconds(10)).fields("content")
                .consistentWith(MutationState.from(mutationResult.mutationToken().get())).highlight(HighlightStyle.HTML, "content"));
          } catch (TimeoutException e) {
            // continue
          } catch (CouchbaseException e) {
            LOGGER.info("Caught: " + e);
            if (continueOnSearchError(e)) {
              continue;
            }
            throw e;
          }
        }
      });

      assertTrue(ex.getMessage().contains("egress_mib_per_min"));
    } finally {
      cluster.disconnect();
      adminCluster.users().dropUser(username);
      adminCluster.searchIndexes().dropIndex("ratelimits-egress");
    }
  }

  @Test
  @IgnoreWhen(missesCapabilities = Capabilities.SEARCH)
  void searchRateLimitMaxIngress() throws Exception {
    String username = "searchRateLimitIngress";

    Limits limits = new Limits();
    limits.searchLimits = new SearchLimits(10, 100, 1, 10);
    createRateLimitedUser(username, limits);

    adminCluster.searchIndexes().upsertIndex(new SearchIndex("ratelimits-ingress", config().bucketname()));
    Cluster cluster = createTestCluster(username);

    try {
      cluster.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);
      String content = repeatString(1024 * 1024, "a");

      RateLimitedException ex = assertThrows(RateLimitedException.class, () -> {
        for (int i = 0; i < LOOP_GUARD; i++) {
          try {
            SearchResult res = cluster.searchQuery("ratelimits-ingress", QueryStringQuery.match(content),
              SearchOptions.searchOptions().timeout(Duration.ofSeconds(10)).limit(1));
          } catch (TimeoutException e) {
            // continue
          } catch (CouchbaseException e) {
            LOGGER.info("Caught: " + e);
            if (continueOnSearchError(e)) {
              continue;
            }
            throw e;
          }
        }
      });

      assertTrue(ex.getMessage().contains("ingress_mib_per_min"));
    } finally {
      cluster.disconnect();
      adminCluster.users().dropUser(username);
      adminCluster.searchIndexes().dropIndex("ratelimits-ingress");
    }
  }

  @Test
  @IgnoreWhen(missesCapabilities = Capabilities.SEARCH)
  void searchRateLimitConcurrentRequests() throws Exception {
    String username = "searchRateLimitConcurrentRequests";

    Limits limits = new Limits();
    limits.searchLimits = new SearchLimits(1, 100, 10, 10);
    createRateLimitedUser(username, limits);

    adminCluster.searchIndexes().upsertIndex(new SearchIndex("ratelimits", config().bucketname()));
    Cluster cluster = createTestCluster(username);

    try {
      cluster.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);

      RateLimitedException ex = assertThrows(RateLimitedException.class, () -> Flux
        .range(0, LOOP_GUARD)
        .flatMap(i -> cluster.reactive().searchQuery("ratelimits", QueryStringQuery.queryString("a"))
                .onErrorResume(err -> {
                  LOGGER.info("Got error " + err.toString());
                  if (err instanceof CouchbaseException
                          && continueOnSearchError((CouchbaseException) err)) {
                    return Mono.empty();
                  }
                  return Mono.error(err);
                }))
        .blockLast());

      assertTrue(ex.getMessage().contains("num_concurrent_requests"));
    } finally {
      cluster.disconnect();
      adminCluster.users().dropUser(username);
      adminCluster.searchIndexes().dropIndex("ratelimits");
    }
  }

  @Test
  @IgnoreWhen(missesCapabilities = Capabilities.SEARCH)
  void searchQuotaLimitScopesMaxIndexes() throws Exception {
    String scopeName = "ratelimitSearch";
    String collectionName = "searchCollection";
    ScopeRateLimits limits = new ScopeRateLimits();
    limits.fts = new SearchScopeRateLimit(1);
    createLimitedScope(scopeName, config().bucketname(), limits);

    CollectionManager collectionManager = adminCluster.bucket(config().bucketname()).collections();

    Map<String, Object> params = mapOf("mapping", mapOf(
      "types", mapOf(
        scopeName + "." + collectionName, mapOf(
          "enabled", true,
          "dynamic", true
        )
      ),
      "default_mapping", mapOf(
        "enabled", false
      ),
      "default_type", "_default",
      "default_analyzer", "standard",
      "default_field", "_all"
      ),
      "doc_config", mapOf(
        "mode", "scope.collection.type_field",
        "type_field", "type"
      ));

    try {
      CollectionSpec collectionSpec = CollectionSpec.create(collectionName, scopeName);
      collectionManager.createCollection(collectionSpec);
      waitUntilCondition(() -> collectionExists(collectionManager, collectionSpec));

      waitForService(adminCluster.bucket(config().bucketname()), ServiceType.SEARCH);
      Util.waitUntilCondition(() -> {
        try {
          adminCluster.searchIndexes().upsertIndex(new SearchIndex("ratelimits1", config().bucketname())
                  .params(params));
        }
        catch (CouchbaseException err) {
          // See intermittent failures on CI:
          // Unknown search error: {"error":"rest_create_index: error creating index: ratelimits1, err: manager_api: CreateIndex, Prepare failed, err: collection_utils: collection: 'searchCollection' doesn't belong to scope: 'ratelimitSearch' in bucket: "...
          LOGGER.info("Got error upserting index: {}", err.toString());
          return false;
        }

        return true;
      });
      QuotaLimitedException ex = assertThrows(
        QuotaLimitedException.class,
        () -> adminCluster.searchIndexes().upsertIndex(new SearchIndex("ratelimits2", config().bucketname())
          .params(params))
      );
    } finally {
      collectionManager.dropScope(scopeName);
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
   * Generates a string of a sequence repeated n times for Search service tests
   *
   * @param repeats the length of the random string.
   * @param seq     the character sequence to be repeated
   * @return the generated string.
   */
  static String repeatString(int repeats, String seq) {
    return String.join("", Collections.nCopies(repeats, seq));
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
      jsonLimits.put("query", mapOf(
        "num_queries_per_min", query.numQueriesPerMin,
        "num_concurrent_requests", query.numConcurrentRequests,
        "ingress_mib_per_min", query.ingressMibPerMin,
        "egress_mib_per_min", query.egressMibPerMin
      ));
    }
    if (limits.searchLimits != null) {
      SearchLimits fts = limits.searchLimits;
      jsonLimits.put("fts", mapOf(
        "num_queries_per_min", fts.numQueriesPerMin,
        "num_concurrent_requests", fts.numConcurrentRequests,
        "ingress_mib_per_min", fts.ingressMibPerMin,
        "egress_mib_per_min", fts.egressMibPerMin
      ));
    }
    if (limits.clusterManagerLimits != null) {
      ClusterManagerLimits cm = limits.clusterManagerLimits;
      jsonLimits.put("clusterManager", mapOf(
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
      jsonLimits.put("kv", mapOf(
        "data_size", limits.kv.dataSize
      ));
    }
    if (limits.fts != null) {
      jsonLimits.put("fts", mapOf(
        "num_fts_indexes", limits.fts.numFtsIndexes
      ));
    }
    if (limits.index != null) {
      jsonLimits.put("index", mapOf(
        "num_indexes", limits.index.numIndexes
      ));
    }
    if (limits.clusterManager != null) {
      jsonLimits.put("clusterManager", mapOf(
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

    public SearchScopeRateLimit(int numFtsIndexes) {
      this.numFtsIndexes = numFtsIndexes;
    }
  }

  static class IndexScopeRateLimit {
    int numIndexes;

    public IndexScopeRateLimit(int numIndexes) {
      this.numIndexes = numIndexes;
    }
  }

  static class ClusterManagerScopeRateLimit {
    int numCollections;

    public ClusterManagerScopeRateLimit(int numCollections) {
      this.numCollections = numCollections;
    }
  }

}
