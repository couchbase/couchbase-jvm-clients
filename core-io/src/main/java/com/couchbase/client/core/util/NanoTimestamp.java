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

import com.couchbase.client.core.annotation.Stability;

import java.time.Duration;
import java.util.Objects;

@Stability.Internal
public class NanoTimestamp implements Comparable<NanoTimestamp> {
  private static final NanoTimestamp NEVER = new NanoTimestamp(System.nanoTime() - (Long.MAX_VALUE / 2));

  private final long nanoTime;

  private NanoTimestamp(long nanoTime) {
    this.nanoTime = nanoTime;
  }

  /**
   * Returns a timestamp representing the current time.
   */
  public static NanoTimestamp now() {
    return new NanoTimestamp(System.nanoTime());
  }

  /**
   * Returns a timestamp from ~146 years ago, for representing
   * the time of events that have not occurred.
   *
   * @see #isNever()
   */
  public static NanoTimestamp never() {
    return NEVER;
  }

  /**
   * Returns true if this timestamp was created by {@link #never()}, otherwise false.
   */
  public boolean isNever() {
    return this == NEVER;
  }

  @Override
  public int compareTo(NanoTimestamp o) {
    long difference = nanoTime - o.nanoTime;
    if (difference < 0) {
      return -1;
    }
    if (difference > 0) {
      return 1;
    }
    return 0;
  }

  /**
   * Returns the time elapsed since this timestamp was created.
   * <p>
   * NOTE: If this timestamp is {@link #never()}, the returned value
   * will be at least 146 years.
   *
   * @see #isNever()
   */
  public Duration elapsed() {
    return Duration.ofNanos(System.nanoTime() - nanoTime);
  }

  /**
   * Returns true if the time elapsed since this timestamp was created is
   * greater than or equal to the given duration, otherwise false.
   * <p>
   * If this timestamp is {@link #never()}, this method returns true for
   * all "reasonable" durations (less than ~146 years).
   */
  public boolean hasElapsed(Duration d) {
    return elapsed().compareTo(d) >= 0;
  }

  public Duration minus(NanoTimestamp rhs) {
    return Duration.ofNanos(this.nanoTime - rhs.nanoTime);
  }

  @Override
  public String toString() {
    return "NanoTimestamp{" +
        "elapsed=" + elapsed() +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    NanoTimestamp that = (NanoTimestamp) o;
    return nanoTime == that.nanoTime;
  }

  @Override
  public int hashCode() {
    return Objects.hash(nanoTime);
  }
}
