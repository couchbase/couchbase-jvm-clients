/*
 * Copyright 2023 Couchbase, Inc.
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

package com.couchbase.client.core.classic;

import com.couchbase.client.core.api.kv.CoreExpiry;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static com.couchbase.client.core.classic.ClassicExpiryHelper.encode;
import static org.junit.jupiter.api.Assertions.*;

class ClassicExpiryHelperTest {

  @Test
  void shortDurationsAreEncodedVerbatim() {
    // this behavior isn't *critical*, but it does mitigate clock drift for very short durations
    assertEncodedAsSecondsFromNow(Duration.ofSeconds(1));
    assertEncodedAsSecondsFromNow(Duration.ofDays(30).minusSeconds(1));
  }

  @Test
  void longDurationsAreConvertedToEpochSecond() {
    // if duration is exactly 30 days, err on the side of caution and convert to epoch second
    assertEncodedAsEpochSecond(Duration.ofDays(30));
    assertEncodedAsEpochSecond(Duration.ofDays(30).plusSeconds(1));
  }

  @Test
  void expiryNoneEncodesToZero() {
    assertEquals(0, encode(CoreExpiry.NONE));
  }

  @Test
  void epochSecondsEncodedVerbatim() {
    long epochSecond = Instant.parse("2020-01-01T00:00:00Z").getEpochSecond();
    assertEquals(epochSecond, encode(CoreExpiry.of(Instant.ofEpochSecond(epochSecond))));
  }

  private static void assertEncodedAsSecondsFromNow(Duration expiry) {
    long result = encode(CoreExpiry.of(expiry));
    assertEquals(expiry.getSeconds(), result);
  }

  private static void assertEncodedAsEpochSecond(Duration expiry) {
    long currentTimeMillis = 1000L;
    long result = encode(CoreExpiry.of(expiry), () -> currentTimeMillis);
    assertEquals(1 + expiry.getSeconds(), result);
  }
}
