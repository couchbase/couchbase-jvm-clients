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

package com.couchbase.client.core.kv;

import com.couchbase.client.core.api.shared.CoreMutationState;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.error.FeatureNotAvailableException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

import static com.couchbase.client.core.util.CbCollections.mapOf;
import static com.couchbase.client.core.util.CbStrings.MAX_CODE_POINT_AS_STRING;
import static com.couchbase.client.core.util.CbStrings.MIN_CODE_POINT_AS_STRING;
import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for the range scan orchestrator to simulate errors and correct behavior from downstream components.
 */
class RangeScanOrchestratorTest {

  private static final CoreEnvironment ENVIRONMENT = CoreEnvironment.create();

  @AfterAll
  static void afterAll() {
    ENVIRONMENT.shutdown();
  }

  private static OrchestratorProxy newProxy(Map<Short, List<CoreRangeScanItem>> data) {
    return new OrchestratorProxy(ENVIRONMENT, true, data);
  }

  /**
   * Most basic test which makes sure that items are streamed as-is from the underlying partitions.
   */
  @Test
  void streamsUnsortedRangeScan() {
    OrchestratorProxy orchestrator = newProxy(mapOf(
      (short) 0, randomItemsSorted(5),
      (short) 1, randomItemsSorted(3)
    ));

    List<CoreRangeScanItem> result = orchestrator.runRangeScan(new TestRangeScan(), new TestScanOptions());
    assertEquals(8, result.size());
  }

  /**
   * Most basic test which makes sure that items are streamed as-is from the underlying partitions.
   */
  @Test
  void streamsUnsortedSamplingScan() {
    OrchestratorProxy orchestrator = newProxy(mapOf(
      (short) 0, randomItemsSorted(3),
      (short) 1, randomItemsSorted(4)
    ));

    List<CoreRangeScanItem> result = orchestrator.runSamplingScan(new TestSamplingScan(10), new TestScanOptions());
    assertEquals(7, result.size());
  }

  /**
   * Verify that even if more data is streamed back sampling is cut short at the configured limit.
   */
  @Test
  void samplingStopsAtLimit() {
    OrchestratorProxy orchestrator = newProxy(mapOf(
      (short) 0, randomItemsSorted(12),
      (short) 1, randomItemsSorted(10)
    ));

    List<CoreRangeScanItem> result = orchestrator.runSamplingScan( new TestSamplingScan(10), new TestScanOptions());
    assertEquals(10, result.size());
  }

  /**
   * Makes sure the operation fails if the bucket capability is not enabled
   */
  @Test
  void failIfBucketCapabilityNotAvailable() {
    OrchestratorProxy orchestrator = new OrchestratorProxy(ENVIRONMENT, false, emptyMap());
    assertThrows(FeatureNotAvailableException.class, () -> orchestrator.runRangeScan(new TestRangeScan(), new TestScanOptions()));
  }

  /**
   * Simulates items from an individual partition which are sorted by itself but not absolute.
   *
   * @param numItems the number of items to emit.
   * @return a sorted list of random scan items.
   */
  private static List<CoreRangeScanItem> randomItemsSorted(final int numItems) {
    return Flux
      .range(0, numItems)
      .map(i -> randomItem())
      .sort(Comparator.comparing(CoreRangeScanItem::key))
      .collectList()
      .block();
  }

  /**
   * Helper method to generate a random core range scan item.
   *
   * @return a random range scan item.
   */
  private static CoreRangeScanItem randomItem() {
    Random random = new Random();
    return new CoreRangeScanItem(random.nextInt(), Instant.now(), random.nextLong(), random.nextLong(),
      randomString().getBytes(StandardCharsets.UTF_8), randomString().getBytes(StandardCharsets.UTF_8)
    );
  }

  /**
   * Helper method to generate a random string, thanks stack overflow!
   *
   * @return a random string. amazing.
   */
  private static String randomString() {
    int leftLimit = 48; // numeral '0'
    int rightLimit = 122; // letter 'z'
    int targetStringLength = 10;
    Random random = new Random();

    return random.ints(leftLimit, rightLimit + 1)
      .filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
      .limit(targetStringLength)
      .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
      .toString();
  }

  class TestRangeScan implements CoreRangeScan {

    @Override
    public CoreScanTerm from() {
      return new CoreScanTerm(MIN_CODE_POINT_AS_STRING, false);
    }

    @Override
    public CoreScanTerm to() {
      return new CoreScanTerm(MAX_CODE_POINT_AS_STRING, false);
    }
  }

  class TestSamplingScan implements CoreSamplingScan {
    int limit;
    Optional<Long> seed = Optional.empty();

    public TestSamplingScan(int limit){
      this.limit = limit;
    }

    @Override
    public long limit() {
      return limit;
    }

    @Override
    public Optional<Long> seed() {
      return seed;
    }
  }

  class TestScanOptions implements CoreScanOptions{
    CoreCommonOptions commons = CoreCommonOptions.DEFAULT;

    public TestScanOptions(){
    }

    @Override
    public CoreCommonOptions commonOptions() {
      return commons;
    }

    @Override
    public boolean idsOnly() {
      return false;
    }

    @Override
    public CoreMutationState consistentWith() {
      return null;
    }

    @Override
    public int batchItemLimit() {
      return 0;
    }

    @Override
    public int batchByteLimit() {
      return 0;
    }

  }
}
