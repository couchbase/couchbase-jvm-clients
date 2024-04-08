/*
 * Copyright 2024 Couchbase, Inc.
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

package com.couchbase.columnar.client.java;

import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.function.Consumer;

import static com.couchbase.client.core.util.CbObjects.defaultIfNull;

/**
 * For specifying default timeouts in
 * {@link Cluster#newInstance(String, Credential, Consumer)}.
 */
public final class TimeoutOptions {
  TimeoutOptions() {
  }

  @Nullable private Duration connectTimeout;
  @Nullable private Duration queryTimeout;

  Unmodifiable build() {
    return new Unmodifiable(this);
  }

  private static @Nullable Duration requireNullOrNonNegative(@Nullable Duration d, String name) {
    if (d != null && d.isNegative()) {
      throw new IllegalArgumentException(name + " must be non-negative, but got: " + d);
    }
    return d;
  }

  public TimeoutOptions queryTimeout(@Nullable Duration queryTimeout) {
    this.queryTimeout = requireNullOrNonNegative(queryTimeout, "queryTimeout");
    return this;
  }

  public TimeoutOptions connectTimeout(Duration connectTimeout) {
    this.connectTimeout = requireNullOrNonNegative(connectTimeout, "connectTimeout");
    return this;
  }

  static class Unmodifiable {
    private final Duration queryTimeout;
    private final Duration connectTimeout;

    private Unmodifiable(TimeoutOptions builder) {
      this.connectTimeout = defaultIfNull(builder.connectTimeout, Duration.ofSeconds(10));
      this.queryTimeout = defaultIfNull(builder.queryTimeout, Duration.ofMinutes(10));
    }

    public Duration queryTimeout() {
      return queryTimeout;
    }

    public Duration connectTimeout() {
      return connectTimeout;
    }

    @Override
    public String toString() {
      return "TimeoutOptions{" +
        "queryTimeout=" + queryTimeout +
        ", connectTimeout=" + connectTimeout +
        '}';
    }
  }
}
