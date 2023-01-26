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

package com.couchbase.client.java.manager.collection;

import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.error.FeatureNotAvailableException;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.manager.bucket.BucketSettings;
import com.couchbase.client.java.manager.bucket.BucketType;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.Capabilities;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertThrows;

@IgnoreWhen(isProtostellarWillWorkLater = true)
public class CollectionManagerErrorIntegrationTest extends JavaIntegrationTest {

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

  /**
   * Note that the mock is also ignored since it returns a different 404 content msg (none) than the real server,
   * so better to ignore it than to introduce special handling logic just for the mock.
   */
  @Test
  @IgnoreWhen(hasCapabilities = Capabilities.COLLECTIONS, clusterTypes = ClusterType.MOCKED)
  void failsIfCollectionsNotSupported() {
    assertThrows(FeatureNotAvailableException.class, () -> collections.getAllScopes());
    assertThrows(FeatureNotAvailableException.class, () -> collections.createScope("foo"));
    assertThrows(FeatureNotAvailableException.class, () -> collections.dropScope("foo"));
    assertThrows(
      FeatureNotAvailableException.class,
      () -> collections.dropCollection(CollectionSpec.create("foo", "bar"))
    );
    assertThrows(
      FeatureNotAvailableException.class,
      () -> collections.createCollection(CollectionSpec.create("foo", "bar"))
    );
  }

  @Test
  @IgnoreWhen(missesCapabilities = Capabilities.COLLECTIONS, clusterTypes = {ClusterType.CAVES, ClusterType.CAPELLA})
  void failsUnderMemcachedBuckets() {
    String bucketName = UUID.randomUUID().toString();
    cluster.buckets().createBucket(BucketSettings.create(bucketName).bucketType(BucketType.MEMCACHED));

    try {
      Bucket bucket = cluster.bucket(bucketName);
      bucket.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);

      CollectionManager mgr = bucket.collections();
      assertThrows(FeatureNotAvailableException.class, () -> mgr.createScope("a"));
      assertThrows(FeatureNotAvailableException.class, () -> mgr.createCollection(CollectionSpec.create("a", "b")));
      assertThrows(FeatureNotAvailableException.class, () -> mgr.dropScope("a"));
      assertThrows(FeatureNotAvailableException.class, () -> mgr.dropCollection(CollectionSpec.create("a", "b")));
    } finally {
      cluster.buckets().dropBucket(bucketName);
    }
  }

}
