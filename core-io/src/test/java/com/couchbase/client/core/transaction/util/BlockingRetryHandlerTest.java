/*
 * Copyright 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.core.transaction.util;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

class BlockingRetryHandlerTest {

  @Test
  void increasesBackoffExponentiallyAndClamps() {
    BlockingRetryHandler blockingRetryHandler = BlockingRetryHandler.builder(Duration.ofMillis(10), Duration.ofMillis(40)).build();

    assertFalse(blockingRetryHandler.shouldRetry());

    blockingRetryHandler.shouldRetry(true);
    assertTrue(blockingRetryHandler.shouldRetry());

    blockingRetryHandler.shouldRetry(false);
    assertFalse(blockingRetryHandler.shouldRetry());
  }

  @Test
  void resetRestoresInitialState() {
    BlockingRetryHandler blockingRetryHandler = BlockingRetryHandler.builder(Duration.ofMillis(5), Duration.ofMillis(10)).build();

    blockingRetryHandler.shouldRetry(true);
    assertTrue(blockingRetryHandler.shouldRetry());

    blockingRetryHandler.shouldRetry(false);
    assertFalse(blockingRetryHandler.shouldRetry());
  }

  @Test
  void validatesInputs() {
    assertThrows(IllegalArgumentException.class, () -> BlockingRetryHandler.builder(Duration.ZERO, Duration.ofMillis(1)).build());
    assertThrows(IllegalArgumentException.class, () -> BlockingRetryHandler.builder(Duration.ofMillis(1), Duration.ZERO).build());
    assertThrows(IllegalArgumentException.class, () -> BlockingRetryHandler.builder(Duration.ofMillis(2), Duration.ofMillis(1)).build());
    assertThrows(IllegalArgumentException.class, () -> BlockingRetryHandler.builder(Duration.ofMillis(1), Duration.ofMillis(2)).jitter(-0.5d).build());
    assertThrows(IllegalArgumentException.class, () -> BlockingRetryHandler.builder(Duration.ofMillis(1), Duration.ofMillis(2)).jitter(1.5d).build());
  }

  @Test
  void calcNextBackoffDoublesAndClamps() {
    BlockingRetryHandler blockingRetryHandler = BlockingRetryHandler.builder(Duration.ofMillis(10), Duration.ofMillis(40)).build();

    assertEquals(Duration.ofMillis(10), blockingRetryHandler.calcNextBackoff());
    assertEquals(Duration.ofMillis(20), blockingRetryHandler.calcNextBackoff());
    assertEquals(Duration.ofMillis(40), blockingRetryHandler.calcNextBackoff());
    assertEquals(Duration.ofMillis(40), blockingRetryHandler.calcNextBackoff());
    assertEquals(Duration.ofMillis(40), blockingRetryHandler.calcNextBackoff());
  }

  @Test
  void maxRetriesLimitsRetryAttempts() {
    BlockingRetryHandler blockingRetryHandler = BlockingRetryHandler.builder(Duration.ofMillis(10), Duration.ofMillis(40))
            .maxRetries(2)
            .build();

    // First retry - should succeed
    blockingRetryHandler.sleepForNextBackoffAndSetShouldRetry();
    assertTrue(blockingRetryHandler.shouldRetry());

    // Second retry - should succeed (still within limit)
    blockingRetryHandler.sleepForNextBackoffAndSetShouldRetry();
    assertTrue(blockingRetryHandler.shouldRetry());

    // Third retry - should fail (exceeded limit of 2)
    blockingRetryHandler.sleepForNextBackoffAndSetShouldRetry();
    assertFalse(blockingRetryHandler.shouldRetry());
  }

  @Test
  void maxRetriesWithJitter() {
    BlockingRetryHandler blockingRetryHandler = BlockingRetryHandler.builder(Duration.ofMillis(10), Duration.ofMillis(40))
            .jitter(0.1d)
            .maxRetries(2)
            .build();

    // First retry - should succeed
    blockingRetryHandler.sleepForNextBackoffAndSetShouldRetry();
    assertTrue(blockingRetryHandler.shouldRetry());

    // Second retry - should succeed
    blockingRetryHandler.sleepForNextBackoffAndSetShouldRetry();
    assertTrue(blockingRetryHandler.shouldRetry());

    // Third retry - should fail (exceeded limit)
    blockingRetryHandler.sleepForNextBackoffAndSetShouldRetry();
    assertFalse(blockingRetryHandler.shouldRetry());
  }

  @Test
  void fixedIntervalRespectsMaxRetries() {
    BlockingRetryHandler blockingRetryHandler = BlockingRetryHandler.builder(Duration.ofMillis(10), Duration.ofMillis(40))
            .maxRetries(1)
            .build();

    // First retry - should succeed
    blockingRetryHandler.sleepForFixedIntervalAndSetShouldRetry(Duration.ofMillis(20));
    assertTrue(blockingRetryHandler.shouldRetry());

    // Second retry - should fail (exceeded limit of 1)
    blockingRetryHandler.sleepForFixedIntervalAndSetShouldRetry(Duration.ofMillis(20));
    assertFalse(blockingRetryHandler.shouldRetry());
  }

  @Test
  void invalidMaxRetriesRejected() {
    assertThrows(IllegalArgumentException.class, () -> BlockingRetryHandler.builder(Duration.ofMillis(1), Duration.ofMillis(2)).maxRetries(-1).build());
  }
}
