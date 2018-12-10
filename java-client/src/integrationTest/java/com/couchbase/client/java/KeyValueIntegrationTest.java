package com.couchbase.client.java;

import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.test.ClusterAwareIntegrationTest;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

class KeyValueIntegrationTest extends ClusterAwareIntegrationTest {

  @Test
  void insertAndGet() {
    String seed = config().nodes().get(0).hostname();
    Cluster cluster = Cluster.connect(seed, config().adminUsername(), config().adminPassword());
    Bucket bucket = cluster.bucket(config().bucketname());
    Collection collection = bucket.defaultCollection();

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
