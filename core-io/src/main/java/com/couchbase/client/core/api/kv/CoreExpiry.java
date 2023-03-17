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

package com.couchbase.client.core.api.kv;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.InvalidArgumentException;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.DAYS;

/**
 * A relative or absolute expiry.
 * <p>
 * Exactly one of {@link #relative} or {@link #absolute} is non-null,
 * except for the special {@link #NONE} instance, which has null for both.
 */
@Stability.Internal
public final class CoreExpiry {
  /**
   * Latest expiry instant that can be represented in the Memcached binary protocol.
   * <p>
   * The server interprets the Memcached protocol's 32-bit expiry field as an unsigned
   * integer. This means the maximum value is 4294967295 seconds,
   * which corresponds to 2106-02-07T06:28:15Z.
   */
  public static final Instant LATEST_VALID_EXPIRY_INSTANT = Instant.ofEpochSecond(4294967295L);

  /**
   * Earliest expiry instant that can be represented in the Memcached binary protocol.
   * <p>
   * A negative instant (before the epoch) could be misinterpreted as in the future.
   * An instant within 30 days after the epoch would be misinterpreted as a relative expiry.
   * <p>
   * Fortunately, setting a document's expiry this far in the past is almost certainly a
   * programming error, so it's fine for the SDK to throw an exception.
   */
  public static final Instant EARLIEST_VALID_EXPIRY_INSTANT = Instant.ofEpochSecond(DAYS.toSeconds(31));

  @Nullable private final Duration relative;
  @Nullable private final Instant absolute;

  /**
   * A "null object" that represents the absence of an expiry.
   * <p>
   * Its {@link #isNone()} method returns true.
   * Its {@link #absolute()} and {@link #relative} methods both return null.
   */
  public static final CoreExpiry NONE = new CoreExpiry();

  private CoreExpiry() {
    this.relative = null;
    this.absolute = null;
  }

  private CoreExpiry(Duration relative) {
    this.relative = requireNonNull(relative);
    this.absolute = null;
  }

  private CoreExpiry(Instant absolute) {
    this.relative = null;
    this.absolute = requireNonNull(absolute);
  }

  /**
   * Returns {@link #NONE} if the duration is zero, otherwise a relative CoreExpiry with this duration.
   *
   * @throws InvalidArgumentException if duration is non-zero and less than 1 second.
   */
  public static CoreExpiry of(Duration duration) {
    if (duration.isZero()) {
      return NONE;
    }

    if (duration.getSeconds() < 1) {
      // A negative duration would be interpreted as an absolute expiry, potentially in the future.
      // A positive duration less than 1 second would be rounded down to zero (no expiry).
      throw InvalidArgumentException.fromMessage(
          "A non-zero expiry duration must be >= 1 second, but got " + duration
      );
    }

    return new CoreExpiry(duration);
  }

  /**
   * Returns {@link #NONE} if the instant's epoch second is zero, otherwise an absolute CoreExpiry with this instant.
   *
   * @throws InvalidArgumentException if instant is non-zero and outside the valid range.
   * @see #EARLIEST_VALID_EXPIRY_INSTANT
   * @see #LATEST_VALID_EXPIRY_INSTANT
   */
  public static CoreExpiry of(Instant instant) {
    if (instant.equals(Instant.EPOCH)) {
      return NONE;
    }

    if (instant.isAfter(LATEST_VALID_EXPIRY_INSTANT)) {
      throw InvalidArgumentException.fromMessage(
          "Requested expiry " + instant + " is too far in the future; must be <= " + LATEST_VALID_EXPIRY_INSTANT
      );
    }

    if (instant.isBefore(EARLIEST_VALID_EXPIRY_INSTANT)) {
      // Otherwise it gets incorrectly interpreted as a relative expiry, or an expiry in the far future.
      throw InvalidArgumentException.fromMessage(
          "Requested expiry " + instant + " is too far in the past; must be >= " + EARLIEST_VALID_EXPIRY_INSTANT
      );
    }

    return new CoreExpiry(instant);
  }

  public boolean isNone() {
    return this == NONE;
  }

  @Nullable
  public Duration relative() {
    return relative;
  }

  @Nullable
  public Instant absolute() {
    return absolute;
  }

  public void when(
      Consumer<Instant> ifAbsolute,
      Consumer<Duration> ifRelative,
      Runnable ifNone
  ) {
    if (isNone()) {
      ifNone.run();
    } else if (relative != null) {
      ifRelative.accept(relative);
    } else if (absolute != null) {
      ifAbsolute.accept(absolute);
    } else {
      throw new AssertionError("invariant failed");
    }
  }

  @Override
  public String toString() {
    if (this == NONE) {
      return "none";
    }
    return relative != null ? relative.toString() : String.valueOf(absolute);
  }
}
