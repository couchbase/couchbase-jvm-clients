/*
 * Copyright (c) 2019 Couchbase, Inc.
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

package com.couchbase.client.core.service;

import java.time.Duration;

public class KeyValueServiceConfig implements ServiceConfig {

  public static final int DEFAULT_ENDPOINTS = 1;

  private final int endpoints;

  public static Builder builder() {
    return new Builder().endpoints(DEFAULT_ENDPOINTS);
  }

  public static Builder endpoints(int endpoints) {
    return builder().endpoints(endpoints);
  }

  private KeyValueServiceConfig(Builder builder) {
    this.endpoints = builder.endpoints;
  }

  @Override
  public int minEndpoints() {
    return endpoints;
  }

  @Override
  public int maxEndpoints() {
    return endpoints;
  }

  @Override
  public Duration idleTime() {
    return Duration.ZERO;
  }

  @Override
  public boolean pipelined() {
    return true;
  }

  public static class Builder {
    private int endpoints;

    public Builder endpoints(int endpoints) {
      this.endpoints = endpoints;
      return this;
    }

    public KeyValueServiceConfig build() {
      return new KeyValueServiceConfig(this);
    }
  }

  @Override
  public String toString() {
    return "KeyValueServiceConfig{" +
      "endpoints=" + endpoints +
      '}';
  }
}
