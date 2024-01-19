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
import com.couchbase.client.core.util.ConsistencyUtil;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.Capabilities;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static com.couchbase.client.java.manager.collection.CreateCollectionSettings.createCollectionSettings;
import static com.couchbase.client.test.Util.waitUntilCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

@IgnoreWhen(missesCapabilities = Capabilities.COLLECTIONS)
class CollectionManagerIntegrationTest extends JavaIntegrationTest {

  private static Cluster cluster;
  private static CollectionManager collections;

  @BeforeAll
  static void setup() {
    cluster = createCluster(env -> env.ioConfig(IoConfig.captureTraffic(ServiceType.MANAGER)));
    Bucket bucket = cluster.bucket(config().bucketname());
    collections = bucket.collections();
    bucket.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);
  }

  @AfterAll
  static void tearDown() {
    cluster.disconnect();
  }

  @Test
  void shouldCreateScopeAndCollection() {
    String scopeName = randomString();
    String collection = randomString();
    CollectionSpec collSpec = CollectionSpec.create(collection, scopeName);
    ScopeSpec scopeSpec = ScopeSpec.create(scopeName);

    assertFalse(collectionExists(collections, collSpec));
    assertThrows(ScopeNotFoundException.class, () -> collections.createCollection(collSpec.scopeName(), collSpec.name()));

    collections.createScope(scopeName);
    if (!config().isProtostellar()) ConsistencyUtil.waitUntilScopePresent(cluster.core(), config().bucketname(), scopeName);

    waitUntilCondition(() -> {
      Optional<ScopeSpec> scope = collections.getAllScopes().stream().filter(ss -> ss.name().equals(scopeName)).findFirst();
      return scope.isPresent() && scope.get().equals(scopeSpec);
    });

    collections.createCollection(collSpec.scopeName(), collSpec.name());
    if (!config().isProtostellar()) ConsistencyUtil.waitUntilCollectionPresent(cluster.core(), config().bucketname(), collSpec.scopeName(), collSpec.name());
    waitUntilCondition(() -> {
      boolean collExists = collectionExists(collections, collSpec);
      if (collExists) {
        Optional<ScopeSpec> scope = collections.getAllScopes().stream().filter(ss -> ss.name().equals(scopeName)).findFirst();
        return scope.isPresent() &&  scope.get().collections().stream()
          .anyMatch(c -> c.name().equals(collSpec.name()) && c.scopeName().equals(collSpec.scopeName()));
      }
      return false;
    });
  }

  @Test
  void shouldThrowWhenScopeAlreadyExists() {
    String scope = randomString();

    collections.createScope(scope);
    if (!config().isProtostellar()) ConsistencyUtil.waitUntilScopePresent(cluster.core(), config().bucketname(), scope);
    waitUntilCondition(() -> scopeExists(collections, scope));
    assertThrows(ScopeExistsException.class, () -> collections.createScope(scope));
  }

  @Test
  void shouldThrowWhenCollectionAlreadyExists() {
    String scope = randomString();
    collections.createScope(scope);
    if (!config().isProtostellar()) ConsistencyUtil.waitUntilScopePresent(cluster.core(), config().bucketname(), scope);
    waitUntilCondition(() -> scopeExists(collections, scope));

    CollectionSpec collectionSpec = CollectionSpec.create(randomString(), scope);
    collections.createCollection(collectionSpec.scopeName(), collectionSpec.name());
    if (!config().isProtostellar()) ConsistencyUtil.waitUntilCollectionPresent(cluster.core(), config().bucketname(), collectionSpec.scopeName(), collectionSpec.name());

    assertThrows(CollectionExistsException.class, () -> collections.createCollection(collectionSpec.scopeName(), collectionSpec.name()));
  }

  @Test
  void shouldDropScopeAndCollections() {
    String scope = randomString();
    String collection1 = randomString();
    CollectionSpec collectionSpec1 = CollectionSpec.create(collection1, scope);
    String collection2 = randomString();
    CollectionSpec collectionSpec2 = CollectionSpec.create(collection2, scope);


    assertThrows(ScopeNotFoundException.class, () -> collections.dropScope("foobar"));
    assertThrows(ScopeNotFoundException.class, () -> collections.dropCollection(collectionSpec1.scopeName(), collectionSpec1.name()));

    collections.createScope(scope);
    if (!config().isProtostellar()) ConsistencyUtil.waitUntilScopePresent(cluster.core(), config().bucketname(), scope);
    waitUntilCondition(() -> scopeExists(collections, scope));

    collections.createCollection(collectionSpec1.scopeName(), collectionSpec1.name());
    collections.createCollection(collectionSpec2.scopeName(), collectionSpec2.name());
    if (!config().isProtostellar()) ConsistencyUtil.waitUntilCollectionPresent(cluster.core(), config().bucketname(), collectionSpec1.scopeName(), collectionSpec1.name());
    if (!config().isProtostellar()) ConsistencyUtil.waitUntilCollectionPresent(cluster.core(), config().bucketname(), collectionSpec2.scopeName(), collectionSpec2.name());

    collections.dropCollection(collectionSpec1.scopeName(), collectionSpec1.name());
    if (!config().isProtostellar()) ConsistencyUtil.waitUntilCollectionDropped(cluster.core(), config().bucketname(), collectionSpec1.scopeName(), collectionSpec1.name());
    waitUntilCondition(() -> !collectionExists(collections, collectionSpec1));
    assertThrows(CollectionNotFoundException.class, () -> collections.dropCollection(collectionSpec1.scopeName(), collectionSpec1.name()));

    collections.dropScope(scope);
    if (!config().isProtostellar()) ConsistencyUtil.waitUntilScopeDropped(cluster.core(), config().bucketname(), scope);
    waitUntilCondition(() -> !scopeExists(collections, scope));

    assertThrows(ScopeNotFoundException.class, () -> collections.dropCollection(collectionSpec2.scopeName(), collectionSpec2.name()));
  }

  @Test
  // gRpc for protostellare createCollectionRequest has no maxTTL
  @IgnoreWhen(missesCapabilities = Capabilities.ENTERPRISE_EDITION)
  void shouldCreateCollectionWithMaxExpiryDeprecated() {
    String scope = randomString();
    String collection1 = randomString();
    CollectionSpec collectionSpec1 = CollectionSpec.create(collection1, scope, Duration.ofSeconds(30));
    String collection2 = randomString();
    CollectionSpec collectionSpec2 = CollectionSpec.create(collection2, scope);

    collections.createScope(scope);
    if (!config().isProtostellar()) ConsistencyUtil.waitUntilScopePresent(cluster.core(), config().bucketname(), scope);
    waitUntilCondition(() -> scopeExists(collections, scope));

    collections.createCollection(collectionSpec1);
    collections.createCollection(collectionSpec2);
    if (!config().isProtostellar()) ConsistencyUtil.waitUntilCollectionPresent(cluster.core(), config().bucketname(), collectionSpec1.scopeName(), collectionSpec1.name());
    if (!config().isProtostellar()) ConsistencyUtil.waitUntilCollectionPresent(cluster.core(), config().bucketname(), collectionSpec2.scopeName(), collectionSpec2.name());

    waitUntilCondition(() -> collectionExists(collections, collectionSpec1));  // maxTTL must also match
    waitUntilCondition(() -> collectionExists(collections, collectionSpec2));  // maxTTL must also match

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
    collections.dropCollection(collectionSpec1);
    collections.dropCollection(collectionSpec2);
  }

  @Test
  // gRpc for protostellare createCollectionRequest has no maxTTL
  @IgnoreWhen(missesCapabilities = Capabilities.ENTERPRISE_EDITION, isProtostellarWillWorkLater = true)
  void shouldCreateCollectionWithMaxExpiry() {
    String scope = randomString();
    String collection1 = randomString();
    CollectionSpec collectionSpec1 = CollectionSpec.create(collection1, scope, Duration.ofSeconds(30));
    String collection2 = randomString();
    CollectionSpec collectionSpec2 = CollectionSpec.create(collection2, scope);

    collections.createScope(scope);
    ConsistencyUtil.waitUntilScopePresent(cluster.core(), config().bucketname(), scope);
    waitUntilCondition(() -> scopeExists(collections, scope));

    collections.createCollection(collectionSpec1.scopeName(), collectionSpec1.name(), CreateCollectionSettings.createCollectionSettings().maxExpiry(collectionSpec1.maxExpiry()));
    collections.createCollection(collectionSpec2.scopeName(), collectionSpec2.name(), CreateCollectionSettings.createCollectionSettings().maxExpiry(collectionSpec2.maxExpiry()));
    ConsistencyUtil.waitUntilCollectionPresent(cluster.core(), config().bucketname(), collectionSpec1.scopeName(), collectionSpec1.name());
    ConsistencyUtil.waitUntilCollectionPresent(cluster.core(), config().bucketname(), collectionSpec2.scopeName(), collectionSpec2.name());

    waitUntilCondition(() -> collectionExists(collections, collectionSpec1));  // maxTTL must also match
    waitUntilCondition(() -> collectionExists(collections, collectionSpec2));  // maxTTL must also match

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

    collections.dropCollection(collectionSpec1.scopeName(), collectionSpec1.name());
    collections.dropCollection(collectionSpec2.scopeName(), collectionSpec2.name());
  }

  @Test
  @IgnoreWhen(clusterVersionIsBelow = "7.6")
  void expiryCanBeNeverExpire() {
    String scopeName = randomString();
    collections.createScope(scopeName);
    ConsistencyUtil.waitUntilScopePresent(cluster.core(), config().bucketname(), scopeName);

    String collectionName = randomString();
    collections.createCollection(
      scopeName,
      collectionName,
      createCollectionSettings()
        .maxExpiry(CollectionSpec.NEVER_EXPIRE)
    );

    ConsistencyUtil.waitUntilCollectionPresent(cluster.core(), config().bucketname(), scopeName, collectionName);

    CollectionSpec spec = getCollection(scopeName, collectionName);

    // assertSame instead of assertEquals, so users can write:
    //   if (spec.maxExpiry() == NEVER_EXPIRE)
    assertSame(CollectionSpec.NEVER_EXPIRE, spec.maxExpiry());
  }

  private CollectionSpec getCollection(String scopeName, String collectionName) {
    return collections.getAllScopes().stream()
      .filter(scope -> scope.name().equals(scopeName))
      .flatMap(scope -> scope.collections().stream()
        .filter(collection -> collection.name().equals(collectionName))
      ).findFirst().orElseThrow(() -> new CollectionNotFoundException(scopeName + "." + collectionName));
  }

  @Test
  void createMaxNumOfCollections() {
    String scopeName = randomString();
    int collectionsPerScope = 10;
    ScopeSpec scopeSpec = ScopeSpec.create(scopeName);
    collections.createScope(scopeName);
    if (!config().isProtostellar()) ConsistencyUtil.waitUntilScopePresent(cluster.core(), config().bucketname(), scopeName);
    waitUntilCondition(() -> scopeExists(collections, scopeName));

    List<ScopeSpec> scopeList = collections.getAllScopes();
    ScopeSpec scope = null;
    for (ScopeSpec sc : scopeList) {
      if (scopeName.equals(sc.name())) {
        scope = sc;
        break;
      }
    }

    assertEquals(scopeSpec, scope);
    for (int i = 0; i < collectionsPerScope; i++) {
      CollectionSpec collectionSpec = CollectionSpec.create(String.valueOf(collectionsPerScope + i), scopeName);
      collections.createCollection(collectionSpec.scopeName(), collectionSpec.name());
      if (!config().isProtostellar()) ConsistencyUtil.waitUntilCollectionPresent(cluster.core(), config().bucketname(), collectionSpec.scopeName(), collectionSpec.name());
      waitUntilCondition(() -> collectionExists(collections, collectionSpec));
      waitUntilCondition(() -> collections.getAllScopes().stream()
        .filter(s -> s.name().equals(collectionSpec.scopeName())).findFirst().map(ScopeSpec::collections).get()
        .stream().anyMatch(cs -> cs.name().equals(collectionSpec.name())));
    }
  }

  /**
   * This test tries to create a collection under a scope which does not exist.
   */
  @Test
  void failCollectionOpIfScopeNotFound() {
    assertThrows(
      ScopeNotFoundException.class,
      () -> collections.createCollection("dude-where-is-my-scope", "jesse")
    );

    assertThrows(
      ScopeNotFoundException.class,
      () -> collections.dropCollection("dude-where-is-my-scope", "chester")
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

}
