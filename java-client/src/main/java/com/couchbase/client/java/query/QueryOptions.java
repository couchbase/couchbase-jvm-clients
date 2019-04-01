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

package com.couchbase.client.java.query;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.util.Golang;
import com.couchbase.client.java.CommonOptions;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.json.JsonValue;

/**
 * N1QL query rrequest options.
 *
 * @since 3.0.0
 */
@Stability.Volatile
public class QueryOptions extends CommonOptions<QueryOptions> {

  public static QueryOptions DEFAULT = new QueryOptions();
  private Map<String, Object> rawParams;
  private Map<String, String> credentials;
  private ScanConsistency scanConsistency;
  private QueryProfile queryProfile;
  private String clientContextId;
  private Boolean metricsDisabled;
  private String scanWait;
  private Integer maxParallelism;
  private Integer pipelineCap;
  private Integer pipelineBatch;
  private Integer scanCap;
  private Boolean readonly;
  private Boolean pretty;
  private JsonValue parameters;
  private boolean prepared;

  private QueryOptions() {}

  public static QueryOptions queryOptions() {
    return new QueryOptions();
  }

  /**
   * Raw parameters for the query
   *
   * @param param the parameter name
   * @param value the parameter value
   * @return {@link QueryOptions} for further chaining
   */
  public QueryOptions withRawParams(String param, Object value) {
    if (this.rawParams == null) {
      this.rawParams = new HashMap<>();
    }
    this.rawParams.put(param, value.toString());
    return this;
  }

  /**
   * Additional credentials for the query
   *
   * @param user the user name
   * @param password the user password
   * @return {@link QueryOptions} for further chaining
   */
  public QueryOptions withCredentials(String user, String password) {
    if (this.credentials == null) {
      this.credentials = new HashMap<>();
    }
    this.credentials.put(user, password);
    return this;
  }

  /**
   * Scan consistency for the query
   *
   * @param scanConsistency the index scan consistency to be used
   * @return {@link QueryOptions} for further chaining
   */
  public QueryOptions withScanConsistency(ScanConsistency scanConsistency) {
    this.scanConsistency = scanConsistency;
    return this;
  }

  /**
   * Set the profiling information level for query execution
   *
   * @param queryProfile the query profile level to be used
   * @return {@link QueryOptions} for further chaining
   */
  public QueryOptions withProfile(QueryProfile queryProfile) {
    this.queryProfile = queryProfile;
    return this;
  }

  /**
   * Adds a client context ID to the request, that will be sent back in the response, allowing clients
   * to meaningfully trace requests/responses when many are exchanged.
   *
   * @param clientContextId the client context ID (null to send none)
   * @return this {@link QueryOptions} for chaining.
   */
  public QueryOptions withClientContextId(String clientContextId) {
    this.clientContextId = clientContextId;
    return this;
  }

  /**
   * If set to true (false being the default), the metrics object will not be returned from N1QL and
   * as a result be more efficient. Note that if metrics are disabled you are losing information
   * to diagnose problems - so use with care!
   *
   * @param metricsDisabled true if disabled, false otherwise (false = default).
   * @return this {@link QueryOptions} for chaining.
   */
  public QueryOptions withMetricsDisabled(boolean metricsDisabled) {
    this.metricsDisabled = metricsDisabled;
    return this;
  }

  /**
   * If the {@link ScanConsistency#NOT_BOUNDED NOT_BOUNDED scan consistency} has been chosen, does nothing.
   *
   * Otherwise, sets the maximum time the client is willing to wait for an index to catch up to the
   * vector timestamp in the request.
   *
   * @param wait the duration.
   * @return this {@link QueryOptions} for chaining.
   */
  public QueryOptions withScanWait(Duration wait) {
    if (this.scanConsistency == ScanConsistency.NOT_BOUNDED) {
      this.scanWait = null;
    } else {
      this.scanWait = Golang.encodeDurationToMs(wait);
    }
    return this;
  }

  /**
   * Allows to override the default maximum parallelism for the query execution on the server side.
   *
   * @param maxParallelism the maximum parallelism for this query, 0 or negative values disable it.
   * @return this {@link QueryOptions} for chaining.
   */
  public QueryOptions withMaxParallelism(int maxParallelism) {
    this.maxParallelism = maxParallelism;
    return this;
  }


  /**
   * If set to false, the server will be instructed to remove extra whitespace from the JSON response
   * in order to save bytes. In performance-critical environments as well as large responses this is
   * recommended in order to cut down on network traffic.
   *
   * Note that this option is only supported in Couchbase Server 4.5.1 or later.
   *
   * @param prettyEnabled if set to false, pretty responses are disabled.
   * @return this {@link QueryOptions} for chaining.
   */
  public QueryOptions withPrettyEnabled(boolean prettyEnabled) {
    this.pretty = prettyEnabled;
    return this;
  }

  /**
   * If set to true, it will signal the query engine on the server that only non-data modifying requests
   * are allowed. Note that this rule is enforced on the server and not the SDK side.
   *
   * Controls whether a query can change a resulting record set.
   *
   * If readonly is true, then the following statements are not allowed:
   *  - CREATE INDEX
   *  - DROP INDEX
   *  - INSERT
   *  - MERGE
   *  - UPDATE
   *  - UPSERT
   *  - DELETE
   *
   * @param readonlyEnabled true if readonly should be forced, false is the default and will use the server side default.
   * @return this {@link QueryOptions} for chaining.
   */
  public QueryOptions withReadonlyEnabled(boolean readonlyEnabled) {
    this.readonly = readonlyEnabled;
    return this;
  }

  /**
   * Advanced: Maximum buffered channel size between the indexer client and the query service for index scans.
   *
   * This parameter controls when to use scan backfill. Use 0 or a negative number to disable.
   *
   * @param scanCap the scan_cap param, use 0 or negative number to disable.
   * @return this {@link QueryOptions} for chaining.
   */
  public QueryOptions withScanCap(int scanCap) {
    this.scanCap = scanCap;
    return this;
  }

  /**
   * Advanced: Controls the number of items execution operators can batch for Fetch from the KV.
   *
   * @param pipelineBatch the pipeline_batch param.
   * @return this {@link QueryOptions} for chaining.
   */
  public QueryOptions pipelineBatch(int pipelineBatch) {
    this.pipelineBatch = pipelineBatch;
    return this;
  }

  /**
   * Advanced: Maximum number of items each execution operator can buffer between various operators.
   *
   * @param pipelineCap the pipeline_cap param.
   * @return this {@link QueryOptions} for chaining.
   */
  public QueryOptions pipelineCap(int pipelineCap) {
    this.pipelineCap = pipelineCap;
    return this;
  }

  /**
   * Named parameters if the query is parameterized with custom names
   *
   * @param named {@link JsonObject} with name as key
   * @return this {@link QueryOptions} for chaining.
   */
  public QueryOptions withParameters(final JsonObject named) {
    this.parameters = named;
    return this;
  }

  /**
   * Positional parameters if the query is parameterized with position numbers
   *
   * @param positional {@link JsonArray} in the same order as positions
   * @return this {@link QueryOptions} for chaining.
   */
  public QueryOptions withParameters(final JsonArray positional) {
    this.parameters = positional;
    return this;
  }

  /**
   * Set to true if the query is already prepared/to be prepared
   *
   * @param prepared true if prepared, else false
   * @return this {@link QueryOptions} for chaining.
   */
  public QueryOptions prepared(final boolean prepared) {
    this.prepared = prepared;
    return this;
  }

  @Stability.Internal
  public BuiltQueryOptions build() {
    return new BuiltQueryOptions();
  }

  public class BuiltQueryOptions extends BuiltCommonOptions {

    public Map<String, Object> getRawParams() {
      return rawParams;
    }

    public Map<String, String> credentials() {
      return credentials;
    }

    public ScanConsistency scanConsistency() {
      return scanConsistency;
    }

    public QueryProfile profile() {
      return queryProfile;
    }

    private String clientContextId() { return clientContextId; }

    private boolean metricsDisabled() { return metricsDisabled; }

    private String scanWait()  { return scanWait; }

    private int pipelineBatch() { return pipelineBatch; }

    private int pipelineCap() { return pipelineCap; }

    private int scanCap() { return scanCap; }

    private boolean readOnly() { return readonly; }

    private boolean pretty() { return pretty; }

    public JsonValue parameters() {
      return parameters;
    }

    public boolean isPrepared() {
      return prepared;
    }

    @Stability.Internal
    public void injectParams(JsonObject queryJson) {
      if (credentials != null && !credentials.isEmpty()) {
        JsonArray creds = JsonArray.create();
        for (Map.Entry<String, String> c : credentials.entrySet()) {
          if (c.getKey() != null && !c.getKey().isEmpty()) {
            creds.add(JsonObject.create()
                    .put("user", c.getKey())
                    .put("pass", c.getValue()));
          }
        }
        if (!creds.isEmpty()) {
          queryJson.put("creds", creds);
        }
      }

      if (parameters != null) {
        if (parameters instanceof JsonArray && !((JsonArray) parameters).isEmpty()) {
          queryJson.put("args", (JsonArray) parameters);
        } else if (parameters instanceof JsonObject && !((JsonObject) parameters).isEmpty()) {
          JsonObject namedParams = (JsonObject) parameters;
          namedParams.getNames().forEach(key -> {
            Object value = namedParams.get(key);
            if (key.charAt(0) != '$') {
              queryJson.put('$' + key, value);
            } else {
              queryJson.put(key, value);
            }
          });
        }
      }

      if (scanConsistency != null) {
        queryJson.put("scan_consistency", scanConsistency.export());
      }

      if (queryProfile != null) {
        queryJson.put("profile", queryProfile.toString());
      }

      if (scanWait != null && (ScanConsistency.REQUEST_PLUS == scanConsistency)) {
        queryJson.put("scan_wait", scanWait);
      }

      if (clientContextId != null) {
        queryJson.put("client_context_id", clientContextId);
      }

      if (maxParallelism != null) {
        queryJson.put("max_parallelism", maxParallelism.toString());
      }

      if (pipelineCap != null) {
        queryJson.put("pipeline_cap", pipelineCap.toString());
      }

      if (pipelineBatch != null) {
        queryJson.put("pipeline_batch", pipelineBatch.toString());
      }

      if (scanCap != null) {
        queryJson.put("scan_cap", scanCap.toString());
      }

      if (metricsDisabled != null) {
        queryJson.put("metrics", metricsDisabled.toString());
      }

      if (pretty != null) {
        queryJson.put("pretty", pretty.toString());
      }

      if (readonly != null) {
        queryJson.put("readonly", readonly.toString());
      }

      boolean autoPrepare = Boolean.parseBoolean(System.getProperty("com.couchbase.client.query.autoprepared", "false"));
      if (autoPrepare) {
        queryJson.put("auto_prepare", "true");
      }

      if (rawParams != null) {
        for (Map.Entry<String, Object> entry : rawParams.entrySet()) {
          queryJson.put(entry.getKey(), entry.getValue());
        }
      }
    }
  }
}