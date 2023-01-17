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

package com.couchbase.client.java.manager.bucket;

import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.error.BucketExistsException;
import com.couchbase.client.core.error.BucketNotFlushableException;
import com.couchbase.client.core.error.BucketNotFoundException;
import com.couchbase.client.core.error.FeatureNotAvailableException;
import com.couchbase.client.core.error.HttpStatusCodeException;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.util.ConsistencyUtil;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.Capabilities;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.IgnoreWhen;
import com.couchbase.client.test.Util;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.couchbase.client.java.manager.bucket.BucketType.MEMCACHED;
import static com.couchbase.client.java.manager.bucket.EvictionPolicyType.FULL;
import static com.couchbase.client.java.manager.bucket.EvictionPolicyType.NOT_RECENTLY_USED;
import static com.couchbase.client.java.manager.bucket.EvictionPolicyType.NO_EVICTION;
import static com.couchbase.client.java.manager.bucket.EvictionPolicyType.VALUE_ONLY;
import static com.couchbase.client.test.Util.waitUntilCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the functionality of the bucket manager.
 */
@IgnoreWhen(clusterTypes = { ClusterType.MOCKED, ClusterType.CAVES, ClusterType.CAPELLA },
  isProtostellarWillWorkLater = true
)
@Execution(ExecutionMode.CONCURRENT)
class BucketManagerIntegrationTest extends JavaIntegrationTest {

  private static Cluster cluster;
  private static BucketManager buckets;
  private final Set<String> bucketsToDrop = new HashSet<>();

  @BeforeAll
  static void setup() {
    cluster = createCluster(env -> env.ioConfig(IoConfig.captureTraffic(ServiceType.MANAGER)));
    Bucket bucket = cluster.bucket(config().bucketname());
    buckets = cluster.buckets();
    bucket.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);
  }

  @AfterEach
  void dropBuckets() {
    try {
      for (String bucketName : bucketsToDrop) {
        try {
          buckets.dropBucket(bucketName);
        } catch (BucketNotFoundException e) {
          // that's fine, the test probably dropped the bucket already
        }
      }
    } finally {
      bucketsToDrop.clear();
    }
  }

  @AfterAll
  static void tearDown() {
    cluster.disconnect();
  }

  private void waitUntilHealthy(String bucket) {
    ConsistencyUtil.waitUntilBucketPresent(cluster.core(), bucket);
    Util.waitUntilCondition(() -> {
      try {
        BucketSettings bkt = buckets.getBucket(bucket);
        return bkt.healthy();
      } catch (BucketNotFoundException err) {
        return false;
      }
    });
  }

  private void waitUntilDropped(String bucket) {
    ConsistencyUtil.waitUntilBucketDropped(cluster.core(), bucket);
    Util.waitUntilCondition(() -> {
      try {
        buckets.getBucket(bucket);
        return false;
      } catch (BucketNotFoundException err) {
        return true;
      }
    });
  }

  /**
   * This sanity test is kept intentionally vague on its assertions since it depends how the test-util decide
   * to setup the default bucket when the test is created.
   */
  @Test
  void getBucket() {
    assertCreatedBucket(buckets.getBucket(config().bucketname()));
  }

  /**
   * Since we don't know how many buckets are in the cluster when the test runs make sure it is at least one and
   * perform some basic assertions on them.
   */
  @Test
  void getAllBuckets() {
    Map<String, BucketSettings> allBucketSettings = buckets.getAllBuckets();
    assertFalse(allBucketSettings.isEmpty());

    for (Map.Entry<String, BucketSettings> entry : allBucketSettings.entrySet()) {
      if (entry.getKey().equals(config().bucketname())) {
        assertCreatedBucket(entry.getValue());
      }
    }
  }

  @Test
  void createEphemeralBucketWithDefaultEvictionPolicy() {
    String name = UUID.randomUUID().toString();
    createBucket(BucketSettings.create(name).bucketType(BucketType.EPHEMERAL));
    BucketSettings settings = buckets.getBucket(name);
    assertEquals(NO_EVICTION, settings.evictionPolicy());
  }

  @Test
  void createEphemeralBucketWithNruEvictionPolicy() {
    String name = UUID.randomUUID().toString();
    createBucket(BucketSettings.create(name)
        .bucketType(BucketType.EPHEMERAL)
        .evictionPolicy(NOT_RECENTLY_USED));

    BucketSettings settings = buckets.getBucket(name);
    assertEquals(NOT_RECENTLY_USED, settings.evictionPolicy());
  }

  @Test
  void createCouchbaseBucketWithDefaultEvictionPolicy() {
    String name = UUID.randomUUID().toString();
    createBucket(BucketSettings.create(name)
        .bucketType(BucketType.COUCHBASE));
    BucketSettings settings = buckets.getBucket(name);
    assertEquals(VALUE_ONLY, settings.evictionPolicy());
  }

  @Test
  void createCouchbaseBucketWithFullEvictionPolicy() {
    String name = UUID.randomUUID().toString();
    createBucket(BucketSettings.create(name)
        .bucketType(BucketType.COUCHBASE)
        .evictionPolicy(FULL));
    BucketSettings settings = buckets.getBucket(name);
    assertEquals(FULL, settings.evictionPolicy());
  }

  @Test
  @IgnoreWhen(missesCapabilities = Capabilities.BUCKET_MINIMUM_DURABILITY)
  void createCouchbaseBucketWithMinimumDurability() {
    String name = UUID.randomUUID().toString();
    createBucket(BucketSettings.create(name)
      .numReplicas(0)
      .bucketType(BucketType.COUCHBASE)
      .minimumDurabilityLevel(DurabilityLevel.MAJORITY));
    BucketSettings settings = buckets.getBucket(name);
    assertEquals(DurabilityLevel.MAJORITY, settings.minimumDurabilityLevel());
  }

  @Test
  @IgnoreWhen(missesCapabilities = {Capabilities.STORAGE_BACKEND})
  void createCouchbaseBucketWithStorageBackendCouchstore() {
    String name = UUID.randomUUID().toString();
    createBucket(BucketSettings.create(name)
        .bucketType(BucketType.COUCHBASE)
        .storageBackend(StorageBackend.COUCHSTORE));
    BucketSettings settings = buckets.getBucket(name);
    assertEquals(StorageBackend.COUCHSTORE, settings.storageBackend());
  }

  @Test
  @IgnoreWhen(missesCapabilities = {Capabilities.STORAGE_BACKEND})
  void createCouchbaseBucketWithStorageBackendDefault() {
    String name = UUID.randomUUID().toString();
    createBucket(BucketSettings.create(name)
        .bucketType(BucketType.COUCHBASE));
    BucketSettings settings = buckets.getBucket(name);
    assertEquals(StorageBackend.COUCHSTORE, settings.storageBackend());
  }

  @Test
  @IgnoreWhen(missesCapabilities = {Capabilities.STORAGE_BACKEND})
  void createCouchbaseBucketWithStorageBackendMagma() {
    String name = UUID.randomUUID().toString();
    createBucket(BucketSettings.create(name)
          .bucketType(BucketType.COUCHBASE)
          // Minimum RAM for Magma
          .ramQuotaMB(1024)
          .storageBackend(StorageBackend.MAGMA));
    BucketSettings settings = buckets.getBucket(name);
    assertEquals(StorageBackend.MAGMA, settings.storageBackend());
  }

  @Test
  void shouldPickNoDurabilityLevelIfNotSpecified() {
    String name = UUID.randomUUID().toString();
    createBucket(BucketSettings.create(name)
      .bucketType(BucketType.COUCHBASE));
    BucketSettings settings = buckets.getBucket(name);
    assertEquals(DurabilityLevel.NONE, settings.minimumDurabilityLevel());
  }

  @Test
  void createMemcachedBucket() {
    String name = UUID.randomUUID().toString();
    createBucket(BucketSettings.create(name).bucketType(MEMCACHED));
    BucketSettings settings = buckets.getBucket(name);
    assertEquals(MEMCACHED, settings.bucketType());
  }

  @Test
  void createAndDropBucket() {
    String name = UUID.randomUUID().toString();

    createBucket(BucketSettings.create(name));
    assertTrue(buckets.getAllBuckets().containsKey(name));

    buckets.dropBucket(name);
    waitUntilDropped(name);
    assertFalse(buckets.getAllBuckets().containsKey(name));
  }

  @Test
  void flushBucket() {
    Bucket bucket = cluster.bucket(config().bucketname());
    Collection collection = bucket.defaultCollection();

    String id = UUID.randomUUID().toString();
    collection.upsert(id, "value");
    assertTrue(collection.exists(id).exists());

    buckets.flushBucket(config().bucketname());
    waitUntilCondition(() -> !collection.exists(id).exists());
  }

  @Test
  void failIfBucketFlushDisabled() {
    String bucketName = UUID.randomUUID().toString();
    createBucket(BucketSettings.create(bucketName).flushEnabled(false));
    assertThrows(BucketNotFlushableException.class, () -> buckets.flushBucket(bucketName));
  }

  @Test
  void createShouldFailWhenPresent() {
    assertThrows(BucketExistsException.class, () ->
        buckets.createBucket(BucketSettings.create(config().bucketname())));
  }

  @Test
  void updateShouldOverrideWhenPresent() {
    BucketSettings loaded = buckets.getBucket(config().bucketname());

    long oldQuota = loaded.ramQuotaMB();
    long newQuota = oldQuota + 10;

    loaded.ramQuotaMB(newQuota);
    buckets.updateBucket(loaded);

    Util.waitUntilCondition(() -> {
      BucketSettings modified = buckets.getBucket(config().bucketname());
      return newQuota == modified.ramQuotaMB();
    });
  }

  @Test
  void updateShouldFailIfAbsent() {
    assertThrows(BucketNotFoundException.class, () -> buckets.updateBucket(BucketSettings.create("does-not-exist")));
  }

  @Test
  void getShouldFailIfAbsent() {
    assertThrows(BucketNotFoundException.class, () -> buckets.getBucket("does-not-exist"));
  }

  @Test
  void dropShouldFailIfAbsent() {
    assertThrows(BucketNotFoundException.class, () -> buckets.dropBucket("does-not-exist"));
  }

  @Test
  void createWithMoreThanOneReplica() {
    String name = UUID.randomUUID().toString();

    createBucket(BucketSettings.create(name).numReplicas(3));

    BucketSettings bucket = buckets.getBucket(name);
    assertEquals(3, bucket.numReplicas());
  }

  @Test
  @IgnoreWhen(missesCapabilities = {Capabilities.STORAGE_BACKEND})
  void customConflictResolution() {
    String bucketName = UUID.randomUUID().toString();
    try {
      createBucket(BucketSettings.create(bucketName).ramQuotaMB(100).conflictResolutionType(ConflictResolutionType.CUSTOM));

      BucketSettings bucket = buckets.getBucket(bucketName);
      assertEquals(ConflictResolutionType.CUSTOM, bucket.conflictResolutionType());
    } catch (HttpStatusCodeException ex) {
      assertTrue(ex.getMessage().contains("Conflict resolution type 'custom' is supported only with developer preview enabled"));
    }
  }

  @Test
  @IgnoreWhen(hasCapabilities = {Capabilities.ENTERPRISE_EDITION})
  void createBucketWithCompressionModeShouldFailOnCE() {
    String name = UUID.randomUUID().toString();
    assertThrows(FeatureNotAvailableException.class, () -> createBucket(BucketSettings.create(name)
            .compressionMode(CompressionMode.PASSIVE)));
  }

  @Test
  @IgnoreWhen(hasCapabilities = {Capabilities.ENTERPRISE_EDITION})
  void updateBucketWithCompressionModeShouldFailOnCE() {
    String name = UUID.randomUUID().toString();
    createBucket(BucketSettings.create(name));
    BucketSettings settings = buckets.getBucket(name);
    settings.compressionMode(CompressionMode.PASSIVE);
    assertThrows(FeatureNotAvailableException.class, () -> buckets.updateBucket(settings));
  }

  @Test
  @IgnoreWhen(missesCapabilities = {Capabilities.ENTERPRISE_EDITION})
  void createBucketWithCompressionModeShouldSucceedOnEE() {
    String name = UUID.randomUUID().toString();
    createBucket(BucketSettings.create(name)
            .compressionMode(CompressionMode.PASSIVE));
  }

  @Test
  @IgnoreWhen(missesCapabilities = {Capabilities.ENTERPRISE_EDITION})
  void updateBucketWithCompressionModeShouldSucceedOnEE() {
    String name = UUID.randomUUID().toString();
    createBucket(BucketSettings.create(name));
    BucketSettings settings = buckets.getBucket(name);
    settings.compressionMode(CompressionMode.PASSIVE);
    buckets.updateBucket(settings);
  }

  /**
   * Helper method to assert simple invariants for the bucket which has been created by the {@link JavaIntegrationTest}.
   */
  private void assertCreatedBucket(final BucketSettings settings) {
    assertEquals(config().bucketname(), settings.name());
    assertTrue(settings.ramQuotaMB() > 0);
  }

  private void createBucket(BucketSettings settings) {
    buckets.createBucket(settings);
    ConsistencyUtil.waitUntilBucketPresent(cluster.core(), settings.name());
    bucketsToDrop.add(settings.name());
    waitUntilHealthy(settings.name());
  }
}
