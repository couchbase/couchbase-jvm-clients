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
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

class KeyValueIdleDetectorTest {
  private static final Duration IDLE_TIMEOUT = Duration.ofSeconds(10);
  private static final long HALF = IDLE_TIMEOUT.toNanos() / 2;

  private long now = 1_000_000;
  private int noops;
  private int closes;

  private KeyValueIdleDetector newDetector(Duration idleTimeout) {
    return new KeyValueIdleDetector(
      idleTimeout,
      new KeyValueIdleDetector.Connection() {
        @Override
        public void writeNoop() {
          noops++;
        }

        @Override
        public void closeBecauseIdle() {
          closes++;
        }
      },
      () -> now
    );
  }

  @Test
  void readerIdleTimeIsHalfTheIdleTimeoutButAtLeastOneMillisecond() {
    assertEquals(TimeUnit.SECONDS.toNanos(150), KeyValueIdleDetector.readerIdleNanos(Duration.ofMinutes(5)));
    assertEquals(TimeUnit.MILLISECONDS.toNanos(1), KeyValueIdleDetector.readerIdleNanos(Duration.ofNanos(10)));
    assertEquals(0, KeyValueIdleDetector.readerIdleNanos(Duration.ofNanos(1)));
    assertEquals(0, KeyValueIdleDetector.readerIdleNanos(Duration.ZERO));
    assertEquals(0, KeyValueIdleDetector.readerIdleNanos(Duration.ofSeconds(-1)));
  }

  @Test
  void writerChecksAtLeastEverySecond() {
    assertEquals(TimeUnit.SECONDS.toNanos(1), newDetector(IDLE_TIMEOUT).checkIntervalNanos());
    // More often, for a short idle timeout: every half timeout.
    assertEquals(TimeUnit.MILLISECONDS.toNanos(200), newDetector(Duration.ofMillis(400)).checkIntervalNanos());
    // No limit, with no idle detection.
    assertEquals(0, newDetector(Duration.ZERO).checkIntervalNanos());
  }

  @Test
  void sendsNoopAfterHalfTheIdleTimeoutThenClosesAfterTheOtherHalf() throws Exception {
    KeyValueIdleDetector idle = newDetector(IDLE_TIMEOUT);
    idle.start();

    now += HALF - 1;
    idle.check();
    assertEquals(0, noops);

    now += 1;
    idle.check();
    assertEquals(1, noops);

    now += HALF - 1;
    idle.check();
    assertEquals(1, noops, "only one NOOP");
    assertEquals(0, closes);

    now += 1;
    idle.check();
    assertEquals(1, closes);
    idle.stop();
  }

  @Test
  void readingResetsTheTimer() throws Exception {
    KeyValueIdleDetector idle = newDetector(IDLE_TIMEOUT);
    idle.start();

    now += HALF;
    idle.check();
    assertEquals(1, noops);

    now += 1;
    idle.read(); // for example, the NOOP's response

    now += HALF - 1;
    idle.check();
    assertEquals(1, noops);
    assertEquals(0, closes);

    now += 1;
    idle.check();
    assertEquals(2, noops, "quiet again for half the timeout, so another NOOP (not a close)");
    assertEquals(0, closes);
    idle.stop();
  }

  @Test
  void stuckWriterCheckClosesOnlyIfTheWriterIsWritingAndTheServerIsQuietForTheWholeTimeout() {
    KeyValueIdleDetector idle = newDetector(IDLE_TIMEOUT);
    idle.start();

    now += IDLE_TIMEOUT.toNanos();
    idle.checkStuckWriter();
    assertEquals(0, closes, "the writer isn't writing, so it checks itself");

    idle.startedWriting();
    now -= 1;
    idle.checkStuckWriter();
    assertEquals(0, closes, "not quiet long enough");

    now += 1;
    idle.checkStuckWriter();
    assertEquals(1, closes);

    idle.finishedWriting();
    idle.stop();
  }

  @Test
  void noIdleDetectionWhenTheTimeoutIsNotPositive() throws Exception {
    KeyValueIdleDetector idle = newDetector(Duration.ZERO);
    idle.start();
    idle.startedWriting();

    now += TimeUnit.DAYS.toNanos(1);
    idle.check();
    idle.checkStuckWriter();
    assertEquals(0, noops);
    assertEquals(0, closes);
    idle.stop();
  }
}
