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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.kv.CoreExpiry;
import com.couchbase.client.core.error.InvalidArgumentException;

import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;

import static com.couchbase.client.core.api.kv.CoreExpiry.LATEST_VALID_EXPIRY_INSTANT;
import static java.util.concurrent.TimeUnit.DAYS;

@Stability.Internal
public class ClassicExpiryHelper {
  /**
   * Durations longer than this must be converted to an
   * epoch second before being passed to the server.
   */
  private static final int MAXIMUM_RELATIVE_EXPIRY_SECONDS = (int) DAYS.toSeconds(30);

  private ClassicExpiryHelper() {
    throw new AssertionError("not instantiable");
  }

  public static long encode(CoreExpiry expiry) {
    return encode(expiry, System::currentTimeMillis);
  }

  public static long encode(CoreExpiry expiry, Supplier<Long> millisClock) {
    if (expiry.isNone()) {
      return 0;
    }

    Instant instant = expiry.absolute();
    if (instant != null) {
      return instant.getEpochSecond();
    }

    //noinspection ConstantConditions
    return encode(expiry.relative(), millisClock);
  }

  private static long encode(Duration duration, Supplier<Long> millisClock) {
    long seconds = duration.getSeconds();
    if (seconds <= MAXIMUM_RELATIVE_EXPIRY_SECONDS) {
      return seconds;
    }

    // Convert the relative expiry to absolute, otherwise the server misinterprets it :-p
    long epochSecond = (millisClock.get() / 1000) + seconds;
    if (epochSecond > LATEST_VALID_EXPIRY_INSTANT.getEpochSecond()) {
      throw InvalidArgumentException.fromMessage(
          "Requested expiry duration " + duration + " is too long; the final expiry time must be <= " + LATEST_VALID_EXPIRY_INSTANT
      );
    }
    return epochSecond;
  }
}
