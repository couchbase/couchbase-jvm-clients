/*
 * Copyright 2024 Couchbase, Inc.
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

package com.couchbase.client.core.config;

import com.couchbase.client.core.node.StandardMemcachedHashingStrategy;
import com.couchbase.client.core.topology.BucketCapability;
import com.couchbase.client.core.topology.ClusterTopologyWithBucket;
import com.couchbase.client.core.topology.NetworkSelector;
import com.couchbase.client.core.topology.PortSelector;
import com.couchbase.client.core.topology.TopologyParser;
import com.couchbase.client.core.util.HostAndPort;
import org.junit.jupiter.api.Test;

import static com.couchbase.client.test.Util.readResource;
import static org.junit.jupiter.api.Assertions.assertEquals;

class LegacyConfigHelperTest {

  @Test
  void memcachedBucketHasCorrectKetamaAuthority() {
    String json = readResource("memcached_mixed_sherlock.json", MemcachedBucketConfigTest.class);

    TopologyParser parser = new TopologyParser(
      NetworkSelector.DEFAULT,
      PortSelector.TLS,
      StandardMemcachedHashingStrategy.INSTANCE
    );

    ClusterTopologyWithBucket cluster = parser.parse(json, "origin.example.com").requireBucket();

    MemcachedBucketConfig legacyBucket = (MemcachedBucketConfig) LegacyConfigHelper.toLegacyBucketConfig(cluster);

    assertEquals(
      new HostAndPort("192.168.56.101", 11210), // <- non-TLS KV port, very important!
      legacyBucket.ketamaRing()
        .toMap()
        .firstEntry()
        .getValue()
        .ketamaAuthority()
    );
  }

  @Test
  void bucketCapabilityEnumsAreInSync() {
    for (BucketCapability cap : BucketCapability.values()) {
      BucketCapabilities.valueOf(cap.name());
    }
    for (BucketCapabilities cap : BucketCapabilities.values()) {
      BucketCapability.valueOf(cap.name());
    }
  }

}
