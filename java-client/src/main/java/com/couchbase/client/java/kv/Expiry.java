/*
 * Copyright 2020 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.java.kv;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.EventBus;
import com.couchbase.client.core.error.InvalidArgumentException;

import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.DAYS;

@Stability.Internal
public class Expiry {
  // Durations longer than this must be converted to an
  // epoch second before being passed to the server.
  private static final int RELATIVE_EXPIRY_CUTOFF_SECONDS = (int) DAYS.toSeconds(30);

  // Prior to SDK 3.2, any duration longer than this was interpreted as a workaround
  // for JCBC-1645. Now we avoid ambiguity by disallowing such durations.
  private static final int LATEST_VALID_EXPIRY_DURATION = (int) DAYS.toSeconds(365) * 50;

  // Any instant earlier than this is almost certainly the result
  // of a programming error. The selected value is > 30 days so
  // we don't need to worry about instant's epoch second being
  // misinterpreted as a number of seconds from the current time.
  private static final Instant EARLIEST_VALID_EXPIRY_INSTANT = Instant.ofEpochSecond(DAYS.toSeconds(31));

  // The server interprets the 32-bit expiry field as an unsigned
  // integer. This means the maximum value is 4294967295 seconds,
  // which corresponds to 2106-02-07T06:28:15Z.
  private static final long UNSIGNED_INT_MAX_VALUE = 4294967295L;
  private static final Instant LATEST_VALID_EXPIRY_INSTANT = Instant.ofEpochSecond(UNSIGNED_INT_MAX_VALUE);

  private static final Expiry NONE = absolute(Instant.ofEpochSecond(0));

  private final Duration duration;
  private final Instant instant;

  private Expiry(Duration duration, Instant instant) {
    this.duration = duration;
    this.instant = instant;
  }

  public static Expiry none() {
    return NONE;
  }

  /**
   * @throws InvalidArgumentException if the Duration is less than 1 second or greater than 18,250 days (~50 years)
   */
  public static Expiry relative(Duration expiry) {
    requireNonNull(expiry);

    long seconds = expiry.getSeconds();

    if (seconds <= 0) {
      throw InvalidArgumentException.fromMessage(
          "When specifying expiry as a Duration," +
              " it must be at least 1 second, but got " + expiry + "." +
              " If you want to explicitly disable expiration, use Instant.EPOCH instead." +
              " If for some reason you want the document to expire immediately," +
              " use Instant.ofEpochSecond(DAYS.toSeconds(31))");
    }

    // Some users may have worked around JCBC-1645 by stuffing the absolute timestamp
    // into the duration. We used to accommodate this by passing very large values through
    // unmodified, but starting with SDK 3.2 this is no longer allowed.
    if (seconds > LATEST_VALID_EXPIRY_DURATION) {
      throw InvalidArgumentException.fromMessage(
          "When specifying expiry as a Duration," +
              " it must not be longer than " + LATEST_VALID_EXPIRY_DURATION + ", but got " + expiry + "." +
              " If you truly require a longer expiry, please specify it as an Instant instead.");
    }

    return new Expiry(expiry, null);
  }

  /**
   * @throws InvalidArgumentException if the Instant is after 2106-02-07T06:28:15Z, or is non-zero and before 1970-02-01T00:00:00Z
   */
  public static Expiry absolute(Instant expiry) {
    requireNonNull(expiry);

    // Basic sanity check, prevent instant from being interpreted as a relative duration.
    // Allow EPOCH (zero instant) because that is how "get with expiry" represents "no expiry"
    if (expiry.isBefore(EARLIEST_VALID_EXPIRY_INSTANT) && !expiry.equals(Instant.EPOCH)) {
      throw InvalidArgumentException.fromMessage("Expiry instant must be zero (for no expiry) or later than " + EARLIEST_VALID_EXPIRY_INSTANT + ", but got " + expiry);
    }

    if (expiry.isAfter(LATEST_VALID_EXPIRY_INSTANT)) {
      // Anything after this would roll over when converted to an unsigned 32-bit value
      // and cause the document to expire sooner than expected.
      throw InvalidArgumentException.fromMessage("Expiry instant must be no later than " + LATEST_VALID_EXPIRY_DURATION + ", but got " + expiry);
    }

    return new Expiry(null, expiry);
  }

  private static final Supplier<Long> systemClock = System::currentTimeMillis;

  /**
   * @throws InvalidArgumentException if the expiry occurs after 2106-02-07T06:28:15Z
   */
  public long encode() {
    return encode(systemClock);
  }

  // Visible for testing
  long encode(Supplier<Long> millisClock) {
    if (instant != null) {
      return instant.getEpochSecond();
    }

    long seconds = duration.getSeconds();
    if (seconds < RELATIVE_EXPIRY_CUTOFF_SECONDS) {
      return seconds;
    }

    long epochSecond = (millisClock.get() / 1000) + seconds;
    if (epochSecond > LATEST_VALID_EXPIRY_INSTANT.getEpochSecond()) {
      throw InvalidArgumentException.fromMessage(
          "Document would expire sooner than requested, since the end of duration " + duration +
              " is after " + LATEST_VALID_EXPIRY_INSTANT);
    }

    return epochSecond;
  }

  /**
   * This method remains as a courtesy to users brave enough to rely on
   * Couchbase internal API. It is scheduled for removal in a future version of the SDK.
   *
   * @deprecated Please use {@link #encode()} instead.
   */
  @Deprecated
  public long encode(EventBus eventBus) {
    return encode();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("Expiry{");
    if (duration != null) {
      sb.append(duration.isZero() ? "none" : duration);
    } else {
      sb.append(instant.getEpochSecond() == 0 ? "none" : instant);
    }
    return sb.append("}").toString();
  }
}
