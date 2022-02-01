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

import com.couchbase.client.core.annotation.SinceCouchbase;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.util.Golang;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CommonOptions;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;

/**
 * Allows customizing various analytics query options.
 * <p>
 * This object is NOT thread safe and must be constructed on a single thread and then passed to the
 * {@link Cluster#analyticsQuery(String, AnalyticsOptions)} method.
 *
 * @since 3.0.0
 */
public class AnalyticsOptions extends CommonOptions<AnalyticsOptions> {

  private String clientContextId;
  private JsonObject namedParameters;
  private JsonArray positionalParameters;
  private int priority;
  private Map<String, Object> raw;
  private boolean readonly = false;
  private AnalyticsScanConsistency scanConsistency;
  private String scanWait;
  private JsonSerializer serializer;

  /**
   * The options should only be instantiated through the {@link #analyticsOptions()} static method.
   */
  private AnalyticsOptions() {}

  /**
   * Creates new {@link AnalyticsOptions} with all default params set.
   *
   * @return constructed {@link AnalyticsOptions} with its default values.
   */
  public static AnalyticsOptions analyticsOptions() {
    return new AnalyticsOptions();
  }

  /**
   * Allows assigning a different server-side priority to the current query.
   *
   * @param priority if this request should have higher priority than other requests on the server.
   * @return the same {@link AnalyticsOptions} for chaining purposes.
   */
  public AnalyticsOptions priority(final boolean priority) {
    this.priority = priority ? -1 : 0;
    return this;
  }

  /**
   * Supports providing a custom client context ID for this query.
   * <p>
   * If no client context ID is provided by the user, a UUID is generated and sent automatically so by default it is
   * always possible to identify a query when debugging. If you do not want to send one, pass either null or an empty
   * string to this method.
   *
   * @param clientContextId the client context ID - if null or empty it will not be sent.
   * @return the same {@link AnalyticsOptions} for chaining purposes.
   */
  public AnalyticsOptions clientContextId(final String clientContextId) {
    if (clientContextId == null || clientContextId.isEmpty()) {
      this.clientContextId = null;
    } else {
      this.clientContextId = clientContextId;
    }
    return this;
  }

  /**
   * Allows explicitly marking a query as being readonly and not mutating and documents on the server side.
   * <p>
   * In addition to providing some security in that you are not accidentally modifying data, setting this flag to true
   * also helps the client to more proactively retry and re-dispatch a query since then it can be sure it is idempotent.
   * As a result, if your query is readonly then it is a good idea to set this flag.
   * <p>
   * Note that this property is only supported when using Couchbase Server 6.5 or later.
   *
   * @param readonly true if readonly should be set, false is the default and will use the server side default.
   * @return the same {@link AnalyticsOptions} for chaining purposes.
   */
  @SinceCouchbase("6.5")
  public AnalyticsOptions readonly(boolean readonly) {
    this.readonly = readonly;
    return this;
  }

  /**
   * Provides a custom {@link JsonSerializer} to be used for decoding the rows as they return from the server.
   * <p>
   * If no serializer is provided, the default one from the {@link ClusterEnvironment} will be used (which is
   * sufficient for most tasks at hand).
   *
   * @param serializer a custom serializer for this request.
   * @return the same {@link AnalyticsOptions} for chaining purposes.
   */
  public AnalyticsOptions serializer(final JsonSerializer serializer) {
    notNull(serializer, "JsonSerializer");
    this.serializer = serializer;
    return this;
  }

  /**
   * Customizes the consistency guarantees for this query.
   * <p>
   * Tuning the scan consistency allows to trade data "freshness" for latency and vice versa. By default
   * {@link AnalyticsScanConsistency#NOT_BOUNDED} is used, which means that the server returns the data it has in the
   * index right away. This is fast, but might not include the most recent mutations. If you want to include all
   * the mutations up to the point of the query, use {@link AnalyticsScanConsistency#REQUEST_PLUS}.
   *
   * @param scanConsistency the index scan consistency to be used for this query.
   * @return the same {@link AnalyticsOptions} for chaining purposes.
   */
  public AnalyticsOptions scanConsistency(final AnalyticsScanConsistency scanConsistency) {
    notNull(scanConsistency, "AnalyticsScanConsistency");
    this.scanConsistency = scanConsistency;
    return this;
  }

  /**
   * Allows customizing how long the query engine is willing to wait until the index catches up to whatever scan
   * consistency is asked for in this query.
   * <p>
   * Note that if {@link AnalyticsScanConsistency#NOT_BOUNDED} is used (which is the default), this method doesn't
   * do anything at all. If no value is provided to this method, the server default is used.
   *
   * @param wait the maximum duration the analytics engine should wait before failing.
   * @return the same {@link AnalyticsOptions} for chaining purposes.
   */
  @Stability.Uncommitted
  public AnalyticsOptions scanWait(final Duration wait) {
    this.scanWait = Golang.encodeDurationToMs(notNull(wait, "Wait Duration"));
    return this;
  }

  /**
   * Sets named parameters for this query.
   * <p>
   * Note that named and positional parameters cannot be used at the same time. If one is set, the other one is "nulled"
   * out and not being sent to the server.
   *
   * @param named {@link JsonObject} the named parameters.
   * @return the same {@link AnalyticsOptions} for chaining purposes.
   */
  public AnalyticsOptions parameters(final JsonObject named) {
    notNull(named, "Named Parameters");
    this.namedParameters = named;
    positionalParameters = null;
    return this;
  }

  /**
   * Sets positional parameters for this query.
   * <p>
   * Note that named and positional parameters cannot be used at the same time. If one is set, the other one is "nulled"
   * out and not being sent to the server.
   *
   * @param positional {@link JsonArray} the positional parameters.
   * @return the same {@link AnalyticsOptions} for chaining purposes.
   */
  public AnalyticsOptions parameters(final JsonArray positional) {
    notNull(positional, "Positional Parameters");
    this.positionalParameters = positional;
    namedParameters = null;
    return this;
  }

  /**
   * Allows providing custom JSON key/value pairs for advanced usage.
   * <p>
   * If available, it is recommended to use the methods on this object to customize the query. This method should
   * only be used if no such setter can be found (i.e. if an undocumented property should be set or you are using
   * an older client and a new server-configuration property has been added to the cluster).
   * <p>
   * Note that the value will be passed through a JSON encoder, so do not provide already encoded JSON as the value. If
   * you want to pass objects or arrays, you can use {@link JsonObject} and {@link JsonArray} respectively.
   *
   * @param key the parameter name (key of the JSON property)  or empty.
   * @param value the parameter value (value of the JSON property).
   * @return the same {@link AnalyticsOptions} for chaining purposes.
   */
  public AnalyticsOptions raw(final String key, final Object value) {
    notNullOrEmpty(key, "Key");
    if (raw == null) {
      raw = new HashMap<>();
    }
    this.raw.put(key, value);
    return this;
  }

  @Stability.Internal
  public Built build() {
    return new Built();
  }

  @Stability.Internal
  public class Built extends BuiltCommonOptions {

    Built() { }

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

      if (scanWait != null && !scanWait.isEmpty() && scanConsistency != null
        && AnalyticsScanConsistency.NOT_BOUNDED != scanConsistency) {
          input.put("scan_wait", scanWait);
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
