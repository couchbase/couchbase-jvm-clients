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
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryOptions;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static com.couchbase.client.core.util.Validators.notNull;

public class AnalyticsOptions extends CommonOptions<AnalyticsOptions> {

  private String clientContextId;
  private JsonObject namedParameters;
  private JsonArray positionalParameters;
  private int priority;
  private Map<String, Object> raw;
  private boolean readonly = false;
  private AnalyticsScanConsistency scanConsistency;
  private JsonSerializer serializer;

  public static AnalyticsOptions analyticsOptions() {
    return new AnalyticsOptions();
  }

  private AnalyticsOptions() {}

  public AnalyticsOptions priority(final boolean priority) {
    this.priority = priority ? -1 : 0;
    return this;
  }

  public AnalyticsOptions clientContextId(final String clientContextId) {
    notNull(clientContextId, "ClientContextId");
    this.clientContextId = clientContextId;
    return this;
  }

  public AnalyticsOptions readonly(boolean readonly) {
    this.readonly = readonly;
    return this;
  }

  public AnalyticsOptions serializer(final JsonSerializer serializer) {
    notNull(serializer, "JsonSerializer");
    this.serializer = serializer;
    return this;
  }

  /**
   * Scan consistency for the query
   *
   * @param scanConsistency the index scan consistency to be used
   * @return {@link QueryOptions} for further chaining
   */
  public AnalyticsOptions scanConsistency(final AnalyticsScanConsistency scanConsistency) {
    notNull(scanConsistency, "AnalyticsScanConsistency");
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
    this.namedParameters = named;
    positionalParameters = null;
    return this;
  }

  /**
   * Positional parameters if the query is parameterized with position numbers
   *
   * @param positional {@link JsonArray} in the same order as positions
   * @return this {@link QueryOptions} for chaining.
   */
  public AnalyticsOptions parameters(final JsonArray positional) {
    this.positionalParameters = positional;
    namedParameters = null;
    return this;
  }

  /**
   * Raw parameters for the query
   *
   * @param param the parameter name
   * @param value the parameter value
   * @return {@link QueryOptions} for further chaining
   */
  public AnalyticsOptions raw(final String param, final Object value) {
    if (raw == null) {
      raw = new HashMap<>();
    }
    this.raw.put(param, value);
    return this;
  }

  @Stability.Internal
  public Built build() {
    return new Built();
  }

  public class Built extends BuiltCommonOptions {

    public boolean readonly() {
      return readonly;
    }

    public JsonSerializer serializer() {
      return serializer;
    }

    public int priority() {
      return priority;
    }

    public void injectParams(final JsonObject input) {
      input.put("client_context_id", clientContextId == null
          ? UUID.randomUUID().toString()
          : clientContextId);

      if (scanConsistency != null) {
        input.put("scan_consistency", scanConsistency.toString());
      }

      boolean positionalPresent = positionalParameters != null && !positionalParameters.isEmpty();
      if (namedParameters != null && !namedParameters.isEmpty()) {
        namedParameters.getNames().forEach(key -> {
          Object value = namedParameters.get(key);
          if (key.charAt(0) != '$') {
            input.put('$' + key, value);
          } else {
            input.put(key, value);
          }
        });
      }

      if (positionalPresent) {
        input.put("args", positionalParameters);
      }

      if (readonly) {
        input.put("readonly", true);
      }

      if (raw != null) {
        for (Map.Entry<String, Object> entry : raw.entrySet()) {
          input.put(entry.getKey(), entry.getValue());
        }
      }
    }

  }

}
