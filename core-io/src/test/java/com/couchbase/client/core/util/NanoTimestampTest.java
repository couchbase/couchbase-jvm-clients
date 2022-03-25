/*
 * Copyright 2022 Couchbase, Inc.
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

package com.couchbase.client.core.util;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static com.couchbase.client.core.util.NanoTimestamp.never;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NanoTimestampTest {
  @Test
  void compare() throws Exception {
    NanoTimestamp t1 = NanoTimestamp.now();
    assertEquals(1, t1.compareTo(never()));
    assertEquals(-1, never().compareTo(t1));
    //noinspection EqualsWithItself
    assertEquals(0, t1.compareTo(t1));

    MILLISECONDS.sleep(1);
    NanoTimestamp t2 = NanoTimestamp.now();
    assertEquals(-1, t1.compareTo(t2));
    assertEquals(1, t2.compareTo(t1));

    assertFalse(t1.elapsed().isNegative());
    assertFalse(t2.minus(t1).isNegative());
    assertTrue(t1.minus(t2).isNegative());

    assertEquals(Duration.ZERO, t1.minus(t1));

    assertTrue(never().hasElapsed(Duration.ofDays(365 * 146)));
  }
}
