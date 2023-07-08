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

package com.couchbase.client.core.util;

import com.couchbase.client.core.annotation.Stability;

import java.time.Duration;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

@Stability.Internal
public class Deadline {
  private final NanoTimestamp start = NanoTimestamp.now();
  private final Duration duration;

  private Deadline(Duration duration) {
    this.duration = requireNonNull(duration);
  }

  public static Deadline of(Duration duration) {
    return new Deadline(duration);
  }

  public static Deadline of(Duration duration, double scale) {
    return new Deadline(Duration.ofNanos((long) (duration.toNanos() * scale)));
  }

  public boolean exceeded() {
    return start.hasElapsed(duration);
  }

  public Optional<Duration> remaining() {
    Duration d = duration.minus(start.elapsed());
    return d.isZero() || d.isNegative() ? Optional.empty() : Optional.of(d);
  }

  @Override
  public String toString() {
    return "Deadline{" +
      "duration=" + duration +
      ", remaining=" + remaining() +
      '}';
  }
}
