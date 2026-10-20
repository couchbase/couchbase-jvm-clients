/*
 * Copyright 2026 Couchbase, Inc.
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

package com.couchbase.client.core.service;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KeyValueIdleDetectorTest {
  private static final Duration IDLE_TIMEOUT = Duration.ofMinutes(5);
  private static final long THIRD = IDLE_TIMEOUT.toNanos() / 3;

  private long now = 1_000_000;
  private int noops;
  private final List<String> closes = new ArrayList<>();

  private KeyValueIdleDetector newDetector(Duration idleTimeout) {
    return new KeyValueIdleDetector(
      idleTimeout,
      new KeyValueIdleDetector.Connection() {
        @Override
        public void sendNoop() {
          noops++;
        }

        @Override
        public void closeBecauseIdle(String reason) {
          closes.add(reason);
        }
      },
      () -> now
    );
  }

  private KeyValueIdleDetector started() {
    KeyValueIdleDetector idle = newDetector(IDLE_TIMEOUT);
    idle.start();
    return idle;
  }

  @Test
  void noopIntervalIsAThirdOfTheIdleTimeout() {
    assertEquals(THIRD, newDetector(IDLE_TIMEOUT).noopIntervalNanos());
    assertEquals(1, newDetector(Duration.ofNanos(1)).noopIntervalNanos(), "on for any positive timeout");
    assertEquals(0, newDetector(Duration.ZERO).noopIntervalNanos());
    assertEquals(0, newDetector(Duration.ofSeconds(-1)).noopIntervalNanos());
  }

  @Test
  void checksAtLeastEverySecondButAtMostEveryMillisecond() {
    assertEquals(TimeUnit.SECONDS.toNanos(1), newDetector(IDLE_TIMEOUT).checkIntervalNanos());
    // More often, for a short idle timeout: every third of it.
    assertEquals(TimeUnit.MILLISECONDS.toNanos(200), newDetector(Duration.ofMillis(600)).checkIntervalNanos());
    assertEquals(TimeUnit.MILLISECONDS.toNanos(1), newDetector(Duration.ofNanos(10)).checkIntervalNanos());
    // Every second when off too (the connection's periodic check does more than this).
    assertEquals(TimeUnit.SECONDS.toNanos(1), newDetector(Duration.ZERO).checkIntervalNanos());
  }

  @Test
  void sendsNoopAfterAThirdOfTheIdleTimeoutThenAnotherThenClosesAfterAllOfIt() {
    KeyValueIdleDetector idle = started();

    now += THIRD - 1;
    idle.check();
    assertEquals(0, noops);

    now += 1;
    idle.check();
    assertEquals(1, noops);

    now += THIRD - 1;
    idle.check();
    assertEquals(1, noops, "still waiting for an answer to the first");

    now += 1;
    idle.check();
    assertEquals(2, noops, "the first went unanswered");

    now += THIRD - 1;
    idle.check();
    assertEquals(2, noops);
    assertTrue(closes.isEmpty());

    now += 1;
    idle.check();
    assertEquals(2, noops);
    assertEquals(1, closes.size(), "the second went unanswered too");
    assertEquals(IDLE_TIMEOUT.toNanos(), 3 * THIRD, "so it closes after the whole idle timeout");
    assertTrue(closes.get(0).contains("after each of the last 2 NOOPs"), closes.get(0));
    assertTrue(closes.get(0).contains("io.configIdleRedialTimeout = PT5M"), closes.get(0));
  }

  @Test
  void answeredNoopKeepsTheConnectionOpen() {
    KeyValueIdleDetector idle = started();

    for (int i = 1; i <= 100; i++) {
      now += THIRD;
      idle.check();
      assertEquals(i, noops);

      now += 1;
      idle.read(); // the NOOP's response
    }
    assertTrue(closes.isEmpty());
  }

  @Test
  void answerAfterTheSecondNoopStartsTheCountAgain() {
    KeyValueIdleDetector idle = started();

    now += THIRD;
    idle.check();
    now += THIRD;
    idle.check();
    assertEquals(2, noops);

    // The second one's answer, just before the check that would have closed the connection.
    now += THIRD - 1;
    idle.read();
    now += 1;
    idle.check();
    assertTrue(closes.isEmpty());

    // Quiet again: it takes two more unanswered NOOPs.
    now += THIRD - 1;
    idle.check();
    assertEquals(3, noops);
    now += THIRD;
    idle.check();
    assertEquals(4, noops);
    assertTrue(closes.isEmpty());
    now += THIRD;
    idle.check();
    assertEquals(1, closes.size());
  }

  @Test
  void onePauseDoesNotCloseAHealthyConnection() {
    // For example, a long GC pause: the check runs before the reader reads the response that arrived during it.
    KeyValueIdleDetector idle = started();

    now += THIRD;
    idle.check();
    assertEquals(1, noops);

    now += 3 * THIRD; // the pause
    idle.check();
    assertEquals(2, noops);
    assertTrue(closes.isEmpty());

    now += 1;
    idle.read(); // the response that arrived during the pause
    now += TimeUnit.SECONDS.toNanos(1);
    idle.check();
    assertTrue(closes.isEmpty());
  }

  @Test
  void trafficMeansNoNoops() {
    KeyValueIdleDetector idle = started();

    for (int i = 0; i < 100; i++) {
      now += THIRD - 1;
      idle.read();
      idle.check();
    }
    assertEquals(0, noops);
    assertTrue(closes.isEmpty());
  }

  @Test
  void offWhenTheIdleTimeoutIsNotPositive() {
    KeyValueIdleDetector idle = newDetector(Duration.ZERO);
    idle.start();

    now += TimeUnit.DAYS.toNanos(1);
    idle.check();
    assertEquals(0, noops);
    assertTrue(closes.isEmpty());
  }
}
