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
import com.couchbase.client.core.api.kv.CoreExpiry;
import com.couchbase.client.core.error.InvalidArgumentException;

import java.time.Duration;
import java.time.Instant;

import static com.couchbase.client.core.util.Validators.notNull;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.DAYS;

@Stability.Internal
public class Expiry {
  // Prior to SDK 3.2, any duration longer than this was interpreted as a workaround
  // for JCBC-1645. Now we avoid ambiguity by disallowing such durations.
  private static final int LATEST_VALID_EXPIRY_DURATION = (int) DAYS.toSeconds(365) * 50;

  private static final Expiry NONE = new Expiry(CoreExpiry.NONE);

  private final CoreExpiry core;

  private Expiry(CoreExpiry core) {
    this.core = requireNonNull(core);
  }

  public static Expiry none() {
    return NONE;
  }

  /**
   * @throws InvalidArgumentException if the Duration is non-zero and less than 1 second, or greater than 18,250 days (~50 years)
   */
  public static Expiry relative(Duration expiry) {
    notNull(expiry, "expiry");

    // Perhaps not ideal, but there's code in the wild that depends on this behavior.
    // This is also consistent with how Instant.EPOCH (zero) means "no expiry".
    if (expiry.isZero()) {
      return none();
    }

    long seconds = expiry.getSeconds();

    if (seconds <= 0) {
      throw InvalidArgumentException.fromMessage(
          "When specifying expiry as a Duration," +
              " it must be either zero (to explicitly disable expiration) or at least 1 second, but got " + expiry + "." +
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

    return new Expiry(CoreExpiry.of(expiry));
  }

  /**
   * @throws InvalidArgumentException if the Instant is after 2106-02-07T06:28:15Z, or is non-zero and before 1970-02-01T00:00:00Z
   */
  public static Expiry absolute(Instant expiry) {
    notNull(expiry, "expiry");
    return new Expiry(CoreExpiry.of(expiry));
  }

  public CoreExpiry encode() {
    return core;
  }

  @Override
  public String toString() {
    return core.toString();
  }
}
