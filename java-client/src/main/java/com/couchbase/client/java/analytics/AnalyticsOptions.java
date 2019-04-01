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

package com.couchbase.client.java.analytics;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.java.CommonOptions;
import com.couchbase.client.java.json.JsonObject;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class AnalyticsOptions extends CommonOptions<AnalyticsOptions> {

  public static AnalyticsOptions DEFAULT = new AnalyticsOptions();

  private int priority;
  private String clientContextId;
  private Map<String, Object> rawParams;

  public static AnalyticsOptions analyticsOptions() {
    return new AnalyticsOptions();
  }

  private AnalyticsOptions() {}

  public AnalyticsOptions priority(final int priority) {
    this.priority = priority;
    return this;
  }

  public AnalyticsOptions clientContextId(final String clientContextId) {
    this.clientContextId = clientContextId;
    return this;
  }

  public AnalyticsOptions rawParam(final String key, final Object value) {
    if (rawParams == null) {
      rawParams = new HashMap<>();
    }
    this.rawParams.put(key, value);
    return this;
  }

  @Stability.Internal
  public AnalyticsOptions.BuiltQueryOptions build() {
    return new AnalyticsOptions.BuiltQueryOptions();
  }

  public class BuiltQueryOptions extends BuiltCommonOptions {

    public int priority() {
      return priority;
    }

    public void injectParams(final JsonObject input) {
      input.put("client_context_id", clientContextId == null
          ? UUID.randomUUID().toString()
          : clientContextId);

      if (rawParams != null) {
        for (Map.Entry<String, Object> entry : rawParams.entrySet()) {
          input.put(entry.getKey(), entry.getValue());
        }
      }
    }

  }

}
