/*
 * Copyright 2019 Couchbase, Inc.
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

package com.couchbase.client.java.manager.collection;

import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.error.CollectionExistsException;
import com.couchbase.client.core.error.CollectionNotFoundException;
import com.couchbase.client.core.error.ScopeExistsException;
import com.couchbase.client.core.error.ScopeNotFoundException;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.Capabilities;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;

import static com.couchbase.client.test.Util.waitUntilCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@IgnoreWhen(missesCapabilities = Capabilities.COLLECTIONS)
class CollectionManagerIntegrationTest extends JavaIntegrationTest {

  private static Cluster cluster;
  private static ClusterEnvironment environment;
  private static CollectionManager collections;

  @BeforeAll
  static void setup() {
    environment = environment().ioConfig(IoConfig.captureTraffic(ServiceType.MANAGER)).build();
    cluster = Cluster.connect(seedNodes(), ClusterOptions.clusterOptions(authenticator()).environment(environment));
    Bucket bucket = cluster.bucket(config().bucketname());
    collections = bucket.collections();
    bucket.waitUntilReady(Duration.ofSeconds(5));
  }

  @AfterAll
  static void tearDown() {
    cluster.disconnect();
    environment.shutdown();
  }

  @Test
  void shouldCreateScopeAndCollection() {
    String scope = randomString();
    String collection = randomString();
    CollectionSpec collSpec = CollectionSpec.create(collection, scope);
    ScopeSpec scopeSpec = ScopeSpec.create(scope);

    assertFalse(collectionExists(collSpec));
    assertThrows(ScopeNotFoundException.class, () -> collections.createCollection(collSpec));
    assertThrows(ScopeNotFoundException.class, () -> collections.getScope(scope));

    collections.createScope(scope);

    waitUntilCondition(() -> scopeExists(scope));
    ScopeSpec found = collections.getScope(scope);
    assertEquals(scopeSpec, found);

    collections.createCollection(collSpec);
    waitUntilCondition(() -> collectionExists(collSpec));

    assertNotEquals(scopeSpec, collections.getScope(scope));
    assertTrue(collections.getScope(scope).collections().contains(collSpec));
  }

  @Test
  void shouldThrowWhenScopeAlreadyExists() {
    String scope = randomString();

    collections.createScope(scope);
    waitUntilCondition(() -> scopeExists(scope));
    assertThrows(ScopeExistsException.class, () -> collections.createScope(scope));
  }

  @Test
  void shouldThrowWhenCollectionAlreadyExists() {
    String scope = randomString();
    collections.createScope(scope);
    waitUntilCondition(() -> scopeExists(scope));

    CollectionSpec collectionSpec = CollectionSpec.create(randomString(), scope);
    collections.createCollection(collectionSpec);

    assertThrows(CollectionExistsException.class, () -> collections.createCollection(collectionSpec));
  }

  @Test
  void shouldDropScopeAndCollections() {
    String scope = randomString();
    String collection1 = randomString();
    CollectionSpec collectionSpec1 = CollectionSpec.create(collection1, scope);
    String collection2 = randomString();
    CollectionSpec collectionSpec2 = CollectionSpec.create(collection2, scope);


    assertThrows(ScopeNotFoundException.class, () -> collections.dropScope("foobar"));
    assertThrows(ScopeNotFoundException.class, () -> collections.dropCollection(collectionSpec1));

    collections.createScope(scope);
    waitUntilCondition(() -> scopeExists(scope));

    collections.createCollection(collectionSpec1);
    collections.createCollection(collectionSpec2);

    collections.dropCollection(collectionSpec1);
    waitUntilCondition(() -> !collectionExists(collectionSpec1));
    assertThrows(CollectionNotFoundException.class, () -> collections.dropCollection(collectionSpec1));

    collections.dropScope(scope);
    waitUntilCondition(() -> !scopeExists(scope));

    assertThrows(ScopeNotFoundException.class, () -> collections.dropCollection(collectionSpec2));
  }

  @Test
  void shouldCreateCollectionWithMaxExpiry() {
    String scope = randomString();
    String collection1 = randomString();
    CollectionSpec collectionSpec1 = CollectionSpec.create(collection1, scope, Duration.ofSeconds(30));
    String collection2 = randomString();
    CollectionSpec collectionSpec2 = CollectionSpec.create(collection2, scope);

    collections.createScope(scope);
    waitUntilCondition(() -> scopeExists(scope));

    collections.createCollection(collectionSpec1);
    collections.createCollection(collectionSpec2);

    waitUntilCondition(() -> collectionExists(collectionSpec1));
    waitUntilCondition(() -> collectionExists(collectionSpec2));

    for (ScopeSpec ss : collections.getAllScopes()) {
      if (!ss.name().equals(scope)) {
        continue;
      }

      for (CollectionSpec cs : ss.collections()) {
        if (cs.name().equals(collection1)) {
          assertEquals(cs.maxExpiry(), Duration.ofSeconds(30));
        } else if (cs.name().equals(collection2)) {
          assertEquals(cs.maxExpiry(), Duration.ZERO);
        }
      }
    }
  }

  @Test
  void createMaxNumOfCollections() {
    String scopeName = randomString();
    int collectionsPerScope = 10;
    ScopeSpec scopeSpec = ScopeSpec.create(scopeName);
    collections.createScope(scopeName);
    waitUntilCondition(() -> scopeExists(scopeName));
    ScopeSpec found = collections.getScope(scopeName);
    assertEquals(scopeSpec, found);
    for (int i = 0; i < collectionsPerScope; i++) {
      CollectionSpec collectionSpec = CollectionSpec.create(String.valueOf(collectionsPerScope + i), scopeName);
      collections.createCollection(collectionSpec);
      waitUntilCondition(() -> collectionExists(collectionSpec));
      assertTrue(collections.getScope(scopeName).collections().contains(collectionSpec));
    }
  }

  /**
   * This test tries to create a collection under a scope which does not exist.
   */
  @Test
  void failCollectionOpIfScopeNotFound() {
    assertThrows(
      ScopeNotFoundException.class,
      () -> collections.createCollection(CollectionSpec.create("jesse", "dude-where-is-my-scope"))
    );

    assertThrows(
      ScopeNotFoundException.class,
      () ->  collections.dropCollection(CollectionSpec.create("chester", "dude-where-is-my-scope"))
    );
  }

  /**
   * Creates a random string in the right size for collection and scope names, which only support
   * up to 30 chars it seems.
   *
   * @return the random string to use
   */
  private String randomString() {
    return UUID.randomUUID().toString().substring(0, 10);
  }

  private boolean collectionExists(CollectionSpec spec) {
    try {
      ScopeSpec scope = collections.getScope(spec.scopeName());
      return scope.collections().contains(spec);
    } catch (ScopeNotFoundException e) {
      return false;
    }
  }

  private boolean scopeExists(String scopeName) {
    try {
      collections.getScope(scopeName);
      return true;
    } catch (ScopeNotFoundException e) {
      return false;
    }
  }
}
