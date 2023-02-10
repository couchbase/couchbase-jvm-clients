/*
 * Copyright (c) 2022 Couchbase, Inc.
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

import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.error.UnambiguousTimeoutException;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.MutationState;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplicateTo;
import com.couchbase.client.java.kv.ScanResult;
import com.couchbase.client.java.kv.ScanSort;
import com.couchbase.client.java.kv.ScanTerm;
import com.couchbase.client.java.kv.ScanType;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.Capabilities;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.couchbase.client.java.kv.ScanOptions.scanOptions;
import static com.couchbase.client.java.kv.UpsertOptions.upsertOptions;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@IgnoreWhen(missesCapabilities = Capabilities.RANGE_SCAN)
class KeyValueRangeScanIntegrationTest extends JavaIntegrationTest  {

  private static final List<String> DOC_IDS = Arrays.asList(
    "a-d2b6bd5d-e46c-49a6-851d-0b700be4ba8e",
    "a-1c15231a-55ea-4a5b-b873-a7eeb4159c10",
    "a-1387d82d-4b8f-41d9-a568-a6acef9330aa",
    "b-211d975a-84ed-49c2-b578-e9ec4b016f44",
    "c-ba5ff9da-bb87-4dd0-be8e-6d24010062a1",
    "x-213623e2-e412-40e1-93af-be058d8b02d2",
    "z-13f9d804-2c22-42a5-bef4-1aef103492ea"
  );

  private static Cluster cluster;
  private static Collection collection;
  private static final Duration TIMEOUT = Duration.ofMinutes(4);

  @BeforeAll
  static void beforeAll() {
    cluster = createCluster();
    Bucket bucket = cluster.bucket(config().bucketname());
    collection = bucket.defaultCollection();

    bucket.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);
    loadSampleData(collection);
  }

  @AfterAll
  static void afterAll() {
    cluster.disconnect();
  }

  static void loadSampleData(final Collection collection) {
    for (String id : DOC_IDS) {
      JsonObject payload = JsonObject.create().put("random", UUID.randomUUID().toString());
      collection.upsert(id, payload, upsertOptions().durability(DurabilityLevel.MAJORITY));
    }
  }

  @Test
  void fullRangeScanOnCollectionWithContent() {
    AtomicLong count = new AtomicLong(0);
    collection.scan(ScanType.rangeScan(ScanTerm.minimum(), ScanTerm.maximum()),
        scanOptions().timeout(TIMEOUT)).forEach(item -> {
      count.incrementAndGet();
      assertTrue(item.contentAsBytes().length > 0);
      assertFalse(item.idOnly());
    });
    assertTrue(count.get() >= DOC_IDS.size());
  }

  @Test
  void fullRangeScanOnCollectionIdsOnly() {
    AtomicLong count = new AtomicLong(0);
    collection.scan(
      ScanType.rangeScan(ScanTerm.minimum(), ScanTerm.maximum()),
      scanOptions().idsOnly(true)
    ).forEach(item -> {
      count.incrementAndGet();
      assertTrue(item.idOnly());
      assertThrows(NoSuchElementException.class, item::contentAsBytes);
    });
    assertTrue(count.get() >= DOC_IDS.size());
  }

  @Test
  void partialRangeScan() {
    List<String> results = collection
      .scan(
        ScanType.rangeScan(ScanTerm.minimum(), ScanTerm.inclusive("c-ba5ff9da-bb87-4dd0-be8e-6d24010062a1")),
        scanOptions().sort(ScanSort.ASCENDING).timeout(TIMEOUT)
      )
      .map(ScanResult::id)
      .collect(Collectors.toList());

    List<String> expected = Arrays.asList(
      "a-1387d82d-4b8f-41d9-a568-a6acef9330aa",
      "a-1c15231a-55ea-4a5b-b873-a7eeb4159c10",
      "a-d2b6bd5d-e46c-49a6-851d-0b700be4ba8e",
      "b-211d975a-84ed-49c2-b578-e9ec4b016f44",
      "c-ba5ff9da-bb87-4dd0-be8e-6d24010062a1"
    );

    assertTrue(results.containsAll(expected));
  }

  @Test
  void samplingWithNegativeSeed() throws Exception {
    long limit = 3;
    long seed = -1;
    long count = collection.scan(ScanType.samplingScan(limit, seed)).count();
    assertEquals(limit, count);
  }

  @Test
  void samplingWithLimitAndContent() throws Exception {
    long limit = 3;
    AtomicLong count = new AtomicLong(0);
    collection.scan(ScanType.samplingScan(limit)).forEach(item -> {
      count.incrementAndGet();
      assertTrue(item.contentAsBytes().length > 0);
      assertFalse(item.idOnly());
    });
    assertEquals(limit, count.get());
  }

  @Test
  void samplingWithLimitIdsOnly() {
    long limit = 3;
    AtomicLong count = new AtomicLong(0);
    collection.scan(ScanType.samplingScan(limit), scanOptions().idsOnly(true)).forEach(item -> {
      count.incrementAndGet();
      assertThrows(NoSuchElementException.class, item::contentAsBytes);
      assertTrue(item.idOnly());
    });
    assertEquals(limit, count.get());
  }

  @Test
  void checkSamplingLimitMustBeGreaterThan0() {
    assertThrows(InvalidArgumentException.class, () -> collection.scan(ScanType.samplingScan(-1)));
  }

  @Test
  void checkScanTypeMustNotBeNull() {
    assertThrows(InvalidArgumentException.class, () -> collection.scan(null));
    assertThrows(InvalidArgumentException.class, () -> collection.scan(ScanType.rangeScan(null, null)));
    assertThrows(InvalidArgumentException.class, () -> collection.scan(ScanType.rangeScan(ScanTerm.minimum(), null)));
    assertThrows(InvalidArgumentException.class, () -> collection.scan(ScanType.rangeScan(null, ScanTerm.maximum())));
  }

  @Test
  void scanOptionsMustBeWithinBounds() {
    assertThrows(InvalidArgumentException.class,
      () -> collection.scan(ScanType.samplingScan(1), scanOptions().batchByteLimit(-1)));
    assertThrows(InvalidArgumentException.class,
      () -> collection.scan(ScanType.samplingScan(1), scanOptions().batchItemLimit(-1)));
    assertThrows(InvalidArgumentException.class,
      () -> collection.scan(ScanType.samplingScan(1), scanOptions().sort(null)));
  }

  @Test
  void throwsTimeoutWithContext() {
    UnambiguousTimeoutException ex = assertThrows(
      UnambiguousTimeoutException.class,
      () -> collection.scan(
        ScanType.rangeScan(ScanTerm.minimum(), ScanTerm.maximum()),
        scanOptions().timeout(Duration.ofMillis(1))
      ).forEach(r -> {})
    );

    assertNotNull(ex.context().getRangeScanContext());
  }

  @Test
  void supportsReadYourOwnWrite() {
    String id = UUID.randomUUID().toString();
    MutationResult upsertResult = collection.upsert(id, JsonObject.create().put("id", id));
    assertTrue(upsertResult.mutationToken().isPresent());
    MutationState mutationState = MutationState.from(upsertResult.mutationToken().get());

    AtomicBoolean idFound = new AtomicBoolean(false);
    collection.scan(
      ScanType.rangeScan(ScanTerm.minimum(), ScanTerm.maximum()),
      scanOptions().consistentWith(mutationState).timeout(Duration.ofSeconds(1))
    ).forEach(item -> {
      if (item.id().equals(id)) {
        idFound.set(true);
      }
    });

    assertTrue(idFound.get());
  }

}
