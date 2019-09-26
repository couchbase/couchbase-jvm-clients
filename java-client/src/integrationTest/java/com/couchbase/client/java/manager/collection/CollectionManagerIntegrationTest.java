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
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.Capabilities;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.UUID;

import static com.couchbase.client.test.Util.waitUntilCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@IgnoreWhen(missesCapabilities = { Capabilities.COLLECTIONS})
class CollectionManagerIntegrationTest extends JavaIntegrationTest {

  private static Cluster cluster;
  private static ClusterEnvironment environment;
  private static CollectionManager collections;

  @BeforeAll
  static void setup() {
    environment = environment().ioConfig(IoConfig.captureTraffic(ServiceType.MANAGER)).build();
    cluster = Cluster.connect(connectionString(), ClusterOptions.clusterOptions(authenticator()).environment(environment));
    collections = cluster.bucket(config().bucketname()).collections();
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

    assertFalse(collections.collectionExists(collSpec));
    assertThrows(ScopeNotFoundException.class, () -> collections.createCollection(collSpec));
    assertThrows(ScopeNotFoundException.class, () -> collections.getScope(scope));

    collections.createScope(scopeSpec);

    waitUntilCondition(() -> collections.getAllScopes().contains(scopeSpec));
    ScopeSpec found = collections.getScope(scope);
    assertEquals(scopeSpec, found);

    collections.createCollection(collSpec);
    waitUntilCondition(() -> collections.collectionExists(collSpec));

    assertNotEquals(scopeSpec, collections.getScope(scope));
    assertTrue(collections.getScope(scope).collections().contains(collSpec));
  }

  @Test
  void shouldThrowWhenScopeAlreadyExists() {
    String scope = randomString();
    CollectionSpec collectionSpec = CollectionSpec.create(randomString(), scope);
    ScopeSpec scopeSpec = ScopeSpec.create(scope, new HashSet<>(Collections.singletonList(collectionSpec)));

    collections.createScope(scopeSpec);
    waitUntilCondition(() -> collections.getAllScopes().contains(scopeSpec));
    assertThrows(ScopeAlreaadyExistsException.class, () -> collections.createScope(scopeSpec));
  }

  @Test
  void shouldThrowWhenCollectionAlreadyExists() {
    String scope = randomString();
    CollectionSpec collectionSpec = CollectionSpec.create(randomString(), scope);
    ScopeSpec scopeSpec = ScopeSpec.create(scope, new HashSet<>(Collections.singletonList(collectionSpec)));

    collections.createScope(scopeSpec);
    waitUntilCondition(() -> collections.getAllScopes().contains(scopeSpec));
    assertThrows(CollectionAlreadyExistsException.class, () -> collections.createCollection(collectionSpec));
  }

  @Test
  void shouldDropScopeAndCollections() {
    String scope = randomString();
    String collection1 = randomString();
    CollectionSpec collectionSpec1 = CollectionSpec.create(collection1, scope);
    String collection2 = randomString();
    CollectionSpec collectionSpec2 = CollectionSpec.create(collection2, scope);

    ScopeSpec scopeSpec = ScopeSpec.create(scope, new HashSet<>(Arrays.asList(collectionSpec1, collectionSpec2)));

    assertThrows(ScopeNotFoundException.class, () -> collections.dropScope("foobar"));
    assertThrows(ScopeNotFoundException.class, () -> collections.dropCollection(collectionSpec1));

    collections.createScope(scopeSpec);
    waitUntilCondition(() -> collections.getAllScopes().contains(scopeSpec));

    collections.dropCollection(collectionSpec1);
    waitUntilCondition(() -> !collections.getScope(scope).collections().contains(collectionSpec1));
    assertThrows(CollectionNotFoundException.class, () -> collections.dropCollection(collectionSpec1));

    collections.dropScope(scope);
    waitUntilCondition(() -> !collections.getAllScopes().contains(scopeSpec));

    assertThrows(ScopeNotFoundException.class, () -> collections.dropCollection(collectionSpec2));
  }

  /**
   * Creates a random string in the right size for collections and scopeps which only support
   * up to 30 chars it seems.
   *
   * @return the random string to use
   */
  private String randomString() {
    return UUID.randomUUID().toString().substring(0, 10);
  }
}
