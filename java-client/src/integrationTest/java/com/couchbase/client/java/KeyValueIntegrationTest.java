/*
 * Copyright (c) 2018 Couchbase, Inc.
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

import com.couchbase.client.core.env.SeedNode;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.ClusterAwareIntegrationTest;
import com.couchbase.client.test.Services;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * This integration test makes sure the various KV-based APIs work as they are intended to.
 *
 * <p>Note that specialized tests which assert some kind of special environment should be placed in
 * separate files for better debugability.</p>
 *
 * @since 3.0.0
 */
class KeyValueIntegrationTest extends JavaIntegrationTest {

  private Cluster cluster;
  private ClusterEnvironment environment;
  private Collection collection;

  @BeforeEach
  void beforeEach() {
    environment = environment().build();
    cluster = Cluster.connect(environment);
    Bucket bucket = cluster.bucket(config().bucketname());
    collection = bucket.defaultCollection();
  }

  @AfterEach
  void afterEach() {
    environment.shutdown(Duration.ofSeconds(1));
    cluster.shutdown();
  }


  @Test
  void insertAndGet() {
    String id = UUID.randomUUID().toString();
    MutationResult insertResult = collection.insert(id, "Hello, World");

    assertTrue(insertResult.cas() != 0);
    assertFalse(insertResult.mutationToken().isPresent());

    GetResult getResult = collection.get(id).get();
    assertEquals(id, getResult.id());
    assertEquals("Hello, World", getResult.contentAs(String.class));
    assertTrue(getResult.cas() != 0);
    assertFalse(getResult.expiration().isPresent());
  }

}
