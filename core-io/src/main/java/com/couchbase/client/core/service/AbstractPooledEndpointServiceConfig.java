/*
 * Copyright 2019 Couchbase, Inc.
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

package com.couchbase.client.core.service;

import com.couchbase.client.core.error.InvalidArgumentException;

import java.time.Duration;

import static java.util.Objects.requireNonNull;

public abstract class AbstractPooledEndpointServiceConfig implements ServiceConfig {

  public static final int DEFAULT_MAX_ENDPOINTS = 12;
  public static final int DEFAULT_MIN_ENDPOINTS = 0;
  public static final Duration DEFAULT_IDLE_TIME = Duration.ofSeconds(30);

  private final int minEndpoints;
  private final int maxEndpoints;
  private final Duration idleTime;

  AbstractPooledEndpointServiceConfig(Builder builder) {
    this.minEndpoints = builder.minEndpoints;
    this.maxEndpoints = builder.maxEndpoints;
    this.idleTime = requireNonNull(builder.idleTime);
  }

  @Override
  public int minEndpoints() {
    return minEndpoints;
  }

  @Override
  public int maxEndpoints() {
    return maxEndpoints;
  }

  @Override
  public Duration idleTime() {
    return idleTime;
  }

  @Override
  public boolean pipelined() {
    return false;
  }

  // public so methods can be invoked via reflection without requiring Method.setAccessible(true)
  public abstract static class Builder<SELF extends Builder> {
    private int minEndpoints;
    private int maxEndpoints;
    private Duration idleTime;

    public SELF minEndpoints(int minEndpoints) {
      if (minEndpoints < 0) {
        throw InvalidArgumentException.fromMessage("minEndpoints must be >= 0 but got " + minEndpoints);
      }
      this.minEndpoints = minEndpoints;
      this.maxEndpoints = Math.max(maxEndpoints, minEndpoints);
      return self();
    }

    public SELF maxEndpoints(int maxEndpoints) {
      if (maxEndpoints < 1) {
        throw InvalidArgumentException.fromMessage("maxEndpoints must be >= 1 but got " + maxEndpoints);
      }
      this.maxEndpoints = maxEndpoints;
      this.minEndpoints = Math.min(maxEndpoints, minEndpoints);
      return self();
    }

    public SELF idleTime(Duration idleTime) {
      if (idleTime.isNegative()) {
        throw InvalidArgumentException.fromMessage("idleTime must be non-negative but got " + idleTime);
      }
      this.idleTime = requireNonNull(idleTime);
      return self();
    }

    @SuppressWarnings("unchecked")
    private SELF self() {
      return (SELF) this;
    }
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "{" +
      "minEndpoints=" + minEndpoints +
      ", maxEndpoints=" + maxEndpoints +
      ", idleTime=" + idleTime +
      '}';
  }
}
