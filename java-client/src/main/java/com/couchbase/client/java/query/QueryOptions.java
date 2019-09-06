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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.msg.kv.MutationToken;
import com.couchbase.client.core.util.Golang;
import com.couchbase.client.java.CommonOptions;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.json.JsonValue;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * N1QL query request options.
 *
 * @since 3.0.0
 */
@Stability.Volatile
public class QueryOptions extends CommonOptions<QueryOptions> {

  private Optional<Map<String, Object>> rawParams = Optional.empty();
  private Optional<Map<String, String>> credentials = Optional.empty();
  private Optional<ScanConsistency> scanConsistency = Optional.empty();
  private Optional<QueryProfile> queryProfile = Optional.empty();
  private Optional<String> clientContextId = Optional.empty();
  private Optional<Boolean> metricsDisabled = Optional.empty();
  private Optional<String> scanWait = Optional.empty();
  private Optional<Integer> maxParallelism = Optional.empty();
  private Optional<Integer> pipelineCap = Optional.empty();
  private Optional<Integer> pipelineBatch = Optional.empty();
  private Optional<Integer> scanCap = Optional.empty();
  private Optional<Boolean> readonly = Optional.empty();
  private Optional<JsonValue> parameters = Optional.empty();
  private Optional<Boolean> prepared = Optional.empty();
  private Optional<Boolean> adhoc = Optional.empty();
  private Optional<List<MutationToken>> consistentWith = Optional.empty();

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
  public QueryOptions rawParams(String param, Object value) {
    if (!this.rawParams.isPresent()) {
      this.rawParams = Optional.of(new HashMap<>());
    }
    this.rawParams.get().put(param, value.toString());
    return this;
  }

  /**
   * Additional credentials for the query
   *
   * @param user the user name
   * @param password the user password
   * @return {@link QueryOptions} for further chaining
   */
  public QueryOptions credentials(String user, String password) {
    if (!this.credentials.isPresent()) {
      this.credentials = Optional.of(new HashMap<>());
    }
    this.credentials.get().put(user, password);
    return this;
  }

  /**
   * If set to false, enables transparent prepared statement support.
   *
   * @param adhoc if this is an ad-hoc query.
   * @return {@link QueryOptions} for further chaining.
   */
  public QueryOptions adhoc(final boolean adhoc) {
    this.adhoc = Optional.of(adhoc);
    return this;
  }

  /**
   * Scan consistency for the query
   *
   * @param scanConsistency the index scan consistency to be used
   * @return {@link QueryOptions} for further chaining
   */
  public QueryOptions scanConsistency(ScanConsistency scanConsistency) {
    this.scanConsistency = Optional.of(scanConsistency);
    return this;
  }

  /**
   * Set the profiling information level for query execution
   *
   * @param queryProfile the query profile level to be used
   * @return {@link QueryOptions} for further chaining
   */
  public QueryOptions profile(QueryProfile queryProfile) {
    this.queryProfile = Optional.of(queryProfile);
    return this;
  }

  /**
   * Adds a client context ID to the request, that will be sent back in the response, allowing clients
   * to meaningfully trace requests/responses when many are exchanged.
   *
   * @param clientContextId the client context ID (null to send none)
   * @return this {@link QueryOptions} for chaining.
   */
  public QueryOptions clientContextId(String clientContextId) {
    this.clientContextId = Optional.of(clientContextId);
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
  public QueryOptions metricsDisabled(boolean metricsDisabled) {
    this.metricsDisabled = Optional.of(metricsDisabled);
    return this;
  }

  /**
   * If the {@link ScanConsistency#NOT_BOUNDED scan consistency} has been chosen, does nothing.
   *
   * Otherwise, sets the maximum time the client is willing to wait for an index to catch up to the
   * vector timestamp in the request.
   *
   * @param wait the duration.
   * @return this {@link QueryOptions} for chaining.
   */
  public QueryOptions scanWait(Duration wait) {
    if (this.scanConsistency.isPresent() && this.scanConsistency.get() == ScanConsistency.NOT_BOUNDED) {
      this.scanWait = Optional.empty();
    } else {
      this.scanWait = Optional.of(Golang.encodeDurationToMs(wait));
    }
    return this;
  }

  /**
   * Allows to override the default maximum parallelism for the query execution on the server side.
   *
   * @param maxParallelism the maximum parallelism for this query, 0 or negative values disable it.
   * @return this {@link QueryOptions} for chaining.
   */
  public QueryOptions maxParallelism(int maxParallelism) {
    this.maxParallelism = Optional.of(maxParallelism);
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
   * @param readonly true if readonly should be forced, false is the default and will use the server side default.
   * @return this {@link QueryOptions} for chaining.
   */
  public QueryOptions readonly(boolean readonly) {
    this.readonly = Optional.of(readonly);
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
  public QueryOptions scanCap(int scanCap) {
    this.scanCap = Optional.of(scanCap);
    return this;
  }

  /**
   * Advanced: Controls the number of items execution operators can batch for Fetch from the KV.
   *
   * @param pipelineBatch the pipeline_batch param.
   * @return this {@link QueryOptions} for chaining.
   */
  public QueryOptions pipelineBatch(int pipelineBatch) {
    this.pipelineBatch = Optional.of(pipelineBatch);
    return this;
  }

  /**
   * Advanced: Maximum number of items each execution operator can buffer between various operators.
   *
   * @param pipelineCap the pipeline_cap param.
   * @return this {@link QueryOptions} for chaining.
   */
  public QueryOptions pipelineCap(int pipelineCap) {
    this.pipelineCap = Optional.of(pipelineCap);
    return this;
  }

  /**
   * Named parameters if the query is parameterized with custom names
   *
   * @param named {@link JsonObject} with name as key
   * @return this {@link QueryOptions} for chaining.
   */
  public QueryOptions parameters(final JsonObject named) {
    this.parameters = Optional.of(named);
    return this;
  }

  /**
   * Positional parameters if the query is parameterized with position numbers
   *
   * @param positional {@link JsonArray} in the same order as positions
   * @return this {@link QueryOptions} for chaining.
   */
  public QueryOptions parameters(final JsonArray positional) {
    this.parameters = Optional.of(positional);
    return this;
  }

  /**
   * Set to true if the query is already prepared/to be prepared
   *
   * @param prepared true if prepared, else false
   * @return this {@link QueryOptions} for chaining.
   */
  public QueryOptions prepared(final boolean prepared) {
    this.prepared = Optional.of(prepared);
    return this;
  }

  /**
   * Sets the {@link MutationToken}s this query should be consistent with.  These tokens are returned from mutations.
   *
   * @param tokens the mutation tokens
   * @return this {@link QueryOptions} for chaining.
   */
  public QueryOptions consistentWith(MutationToken... tokens) {
    this.consistentWith = Optional.of(Arrays.asList(tokens));
    return this;
  }

  /**
   * Sets the {@link MutationToken}s this query should be consistent with.  These tokens are returned from mutations.
   *
   * @param tokens the mutation tokens
   * @return this {@link QueryOptions} for chaining.
   */
  public QueryOptions consistentWith(List<MutationToken> tokens) {
    this.consistentWith = Optional.of(tokens);
    return this;
  }

  @Stability.Internal
  public Built build() {
    return new Built();
  }

  public class Built extends BuiltCommonOptions {

    public boolean adhoc() {
      return adhoc.orElse(true);
    }

    public boolean readonly() {
      return readonly.orElse(false);
    }

    @Stability.Internal
    public void injectParams(JsonObject queryJson) {
      queryJson.put("client_context_id", clientContextId.orElse(UUID.randomUUID().toString()));

      credentials.ifPresent(cr -> {
        if (!cr.isEmpty()) {
          JsonArray creds = JsonArray.create();

          for (Map.Entry<String, String> c : cr.entrySet()) {
            if (c.getKey() != null && !c.getKey().isEmpty()) {
              creds.add(JsonObject.create().put("user", c.getKey()).put("pass", c.getValue()));
            }
          }

          if (!creds.isEmpty()) {
            queryJson.put("creds", creds);
          }
        }
      });

      parameters.ifPresent(params -> {
        if (params instanceof JsonArray && !((JsonArray) params).isEmpty()) {
          queryJson.put("args", (JsonArray) params);
        }
        else if (params instanceof JsonObject && !((JsonObject) params).isEmpty()) {
          JsonObject namedParams = (JsonObject) params;

          namedParams.getNames().forEach(key -> {
            Object value = namedParams.get(key);
            if (key.charAt(0) != '$') {
              queryJson.put('$' + key, value);
            } else {
              queryJson.put(key, value);
            }
          });
        }
      });

      scanConsistency.ifPresent(sc -> queryJson.put("scan_consistency", sc.export()));

      consistentWith.ifPresent(cw -> {
        if (scanConsistency.isPresent()) {
          throw new IllegalArgumentException("`withScanConsistency(...)` cannot be used "
                  + "together with `consistentWith(...)`");
        }

        JsonObject mutationState = JsonObject.create();
        for (MutationToken token : cw) {
          JsonObject bucket = mutationState.getObject(token.bucket());
          if (bucket == null) {
            bucket = JsonObject.create();
            mutationState.put(token.bucket(), bucket);
          }

          bucket.put(
                  String.valueOf(token.vbucketID()),
                  JsonArray.from(token.sequenceNumber(), String.valueOf(token.vbucketUUID()))
          );
        }
        queryJson.put("scan_vectors", mutationState);
        queryJson.put("scan_consistency", "at_plus");
      });

      queryProfile.ifPresent(v -> queryJson.put("profile", v.toString()));

      scanWait.ifPresent(v -> {
        if (!scanConsistency.isPresent() || ScanConsistency.NOT_BOUNDED != scanConsistency.get()) {
          queryJson.put("scan_wait", v);
        }
      });

      maxParallelism.ifPresent(v -> queryJson.put("max_parallelism", v.toString()));

      pipelineCap.ifPresent(v -> queryJson.put("pipeline_cap", v.toString()));

      pipelineBatch.ifPresent(v -> queryJson.put("pipeline_batch", v.toString()));

      scanCap.ifPresent(v -> queryJson.put("scan_cap", v.toString()));

      metricsDisabled.ifPresent(v -> queryJson.put("metrics", !v));

      readonly.ifPresent(v -> queryJson.put("readonly", v));

      boolean autoPrepare = Boolean.parseBoolean(System.getProperty("com.couchbase.client.query.autoprepared", "false"));
      if (autoPrepare) {
        queryJson.put("auto_prepare", true);
      }

      rawParams.ifPresent(v -> {
      for (Map.Entry<String, Object> entry : v.entrySet()) {
          queryJson.put(entry.getKey(), entry.getValue());
        }
      });
    }
  }
}
