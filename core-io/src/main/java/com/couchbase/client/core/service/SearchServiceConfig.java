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

public class SearchServiceConfig extends AbstractPooledEndpointServiceConfig {

  public static Builder builder() {
    return new Builder()
      .minEndpoints(DEFAULT_MIN_ENDPOINTS)
      .maxEndpoints(DEFAULT_MAX_ENDPOINTS)
      .idleTime(DEFAULT_IDLE_TIME);
  }

  public static Builder minEndpoints(int minEndpoints) {
    return builder().minEndpoints(minEndpoints);
  }

  public static Builder maxEndpoints(int maxEndpoints) {
    return builder().maxEndpoints(maxEndpoints);
  }

  public static Builder idleTime(Duration idleTime) {
    return builder().idleTime(idleTime);
  }

  private SearchServiceConfig(Builder builder) {
    super(builder);
  }

  public static class Builder extends AbstractPooledEndpointServiceConfig.Builder<Builder> {
    public SearchServiceConfig build() {
      return new SearchServiceConfig(this);
    }
  }

}
