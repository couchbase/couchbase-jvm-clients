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
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.json.JsonValue;
import com.couchbase.client.java.query.QueryOptions;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class AnalyticsOptions extends CommonOptions<AnalyticsOptions> {

  private int priority;
  private String clientContextId;
  private Map<String, Object> rawParams;
  private JsonValue parameters;
  private ScanConsistency scanConsistency;


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

  /**
   * Scan consistency for the query
   *
   * @param scanConsistency the index scan consistency to be used
   * @return {@link QueryOptions} for further chaining
   */
  public AnalyticsOptions scanConsistency(ScanConsistency scanConsistency) {
    this.scanConsistency = scanConsistency;
    return this;
  }

  /**
   * Named parameters if the query is parameterized with custom names
   *
   * @param named {@link JsonObject} with name as key
   * @return this {@link QueryOptions} for chaining.
   */
  public AnalyticsOptions parameters(final JsonObject named) {
    this.parameters = named;
    return this;
  }

  /**
   * Positional parameters if the query is parameterized with position numbers
   *
   * @param positional {@link JsonArray} in the same order as positions
   * @return this {@link QueryOptions} for chaining.
   */
  public AnalyticsOptions parameters(final JsonArray positional) {
    this.parameters = positional;
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
  public Built build() {
    return new Built();
  }

  public class Built extends BuiltCommonOptions {

    public int priority() {
      return priority;
    }

    public void injectParams(final JsonObject input) {
      input.put("client_context_id", clientContextId == null
          ? UUID.randomUUID().toString()
          : clientContextId);


      if (scanConsistency != null) {
        input.put("scan_consistency", scanConsistency.export());
      }

      if (parameters != null) {
        if (parameters instanceof JsonArray && !((JsonArray) parameters).isEmpty()) {
          input.put("args", (JsonArray) parameters);
        } else if (parameters instanceof JsonObject && !((JsonObject) parameters).isEmpty()) {
          JsonObject namedParams = (JsonObject) parameters;
          namedParams.getNames().forEach(key -> {
            Object value = namedParams.get(key);
            if (key.charAt(0) != '$') {
              input.put('$' + key, value);
            } else {
              input.put(key, value);
            }
          });
        }
      }

      if (rawParams != null) {
        for (Map.Entry<String, Object> entry : rawParams.entrySet()) {
          input.put(entry.getKey(), entry.getValue());
        }
      }
    }

  }

}
