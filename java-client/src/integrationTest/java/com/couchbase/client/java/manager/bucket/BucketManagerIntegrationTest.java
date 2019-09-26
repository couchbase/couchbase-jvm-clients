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
import com.couchbase.client.core.error.KeyNotFoundException;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.UUID;

import static com.couchbase.client.test.Util.waitUntilThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the functionality of the bucket manager.
 */
@IgnoreWhen(clusterTypes = ClusterType.MOCKED)
class BucketManagerIntegrationTest extends JavaIntegrationTest {

  private static Cluster cluster;
  private static ClusterEnvironment environment;
  private static BucketManager buckets;

  @BeforeAll
  static void setup() {
    environment = environment().ioConfig(IoConfig.captureTraffic(ServiceType.MANAGER)).build();
    cluster = Cluster.connect(connectionString(), ClusterOptions.clusterOptions(authenticator()).environment(environment));
    cluster.bucket(config().bucketname());
    buckets = cluster.buckets();
  }

  @AfterAll
  static void tearDown() {
    cluster.disconnect();
    environment.shutdown();
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
  void createAndDropBucket() {
    String name = UUID.randomUUID().toString();

    buckets.createBucket(BucketSettings.create(name));
    assertTrue(buckets.getAllBuckets().containsKey(name));

    buckets.dropBucket(name);
    assertFalse(buckets.getAllBuckets().containsKey(name));
  }

  @Test
  void flushBucket() {
    Bucket bucket = cluster.bucket(config().bucketname());
    Collection collection = bucket.defaultCollection();

    String id =  UUID.randomUUID().toString();
    collection.upsert(id, "value");
    collection.exists(id);

    buckets.flushBucket(config().bucketname());

    waitUntilThrows(KeyNotFoundException.class, () -> collection.exists(id));
  }

  @Test
  void createShouldFailWhenPresent() {
    assertThrows(
      BucketAlreadyExistsException.class,
      () -> buckets.createBucket(BucketSettings.create(config().bucketname()))
    );
  }

  @Test
  void updateShouldOverrideWhenPresent() {
    BucketSettings loaded = buckets.getBucket(config().bucketname());

    long oldQuota = loaded.ramQuotaMB();
    long newQuota = oldQuota + 10;

    loaded.ramQuotaMB(newQuota);
    buckets.updateBucket(loaded);

    BucketSettings modified = buckets.getBucket(config().bucketname());
    assertEquals(newQuota, modified.ramQuotaMB());
  }

  @Test
  void updateShouldFailIfNotPresent() {
    assertThrows(BucketNotFoundException.class, () -> buckets.updateBucket(BucketSettings.create("foobar")));
  }

  /**
   * Helper method to assert simple invariants for the bucket which has been created by the {@link JavaIntegrationTest}.
   */
  private void assertCreatedBucket(final BucketSettings settings) {
    assertEquals(config().bucketname(), settings.name());
    assertTrue(settings.ramQuotaMB() > 0);
  }

}
