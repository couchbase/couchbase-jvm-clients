/*
 * Copyright (c) 2018 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.endpoint;

import java.time.Duration;

public class CircuitBreakerConfig {

  private final boolean enabled;
  private final int volumeThreshold;
  private final int errorThresholdPercentage;
  private final Duration sleepWindow;
  private final Duration rollingWindow;

  public static Builder builder() {
    return new Builder();
  }

  public static CircuitBreakerConfig create() {
    return builder().build();
  }

  public static CircuitBreakerConfig disabled() {
    return builder().enabled(false).build();
  }

  private CircuitBreakerConfig(final Builder builder) {
    this.enabled = builder.enabled;
    this.volumeThreshold = builder.volumeThreshold;
    this.errorThresholdPercentage = builder.errorThresholdPercentage;
    this.sleepWindow = builder.sleepWindow;
    this.rollingWindow = builder.rollingWindow;
  }

  public boolean enabled() {
    return enabled;
  }

  public int volumeThreshold() {
    return volumeThreshold;
  }

  public int errorThresholdPercentage() {
    return errorThresholdPercentage;
  }

  public Duration sleepWindow() {
    return sleepWindow;
  }

  public Duration rollingWindow() {
    return rollingWindow;
  }

  public static class Builder {

    private boolean enabled = true;
    private int volumeThreshold = 20;
    private int errorThresholdPercentage = 50;
    private Duration sleepWindow = Duration.ofSeconds(5);
    private Duration rollingWindow = Duration.ofMinutes(1);

    public Builder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
    }

    public Builder volumeThreshold(int volumeThreshold) {
      this.volumeThreshold = volumeThreshold;
      return this;
    }

    public Builder errorThresholdPercentage(int errorThresholdPercentage) {
      this.errorThresholdPercentage = errorThresholdPercentage;
      return this;
    }

    public Builder sleepWindow(Duration sleepWindow) {
      this.sleepWindow = sleepWindow;
      return this;
    }

    public Builder rollingWindow(Duration rollingWindow) {
      this.rollingWindow = rollingWindow;
      return this;
    }

    public CircuitBreakerConfig build() {
      return new CircuitBreakerConfig(this);
    }
  }

}
