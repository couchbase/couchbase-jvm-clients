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
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.MutationState;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * N1QL query request options.
 *
 * @since 3.0.0
 */
public class QueryOptions extends CommonOptions<QueryOptions> {

  private boolean adhoc = true;
  private String clientContextId;
  private MutationState consistentWith;
  private Integer maxParallelism;
  private boolean metrics = false;
  private JsonObject namedParameters;
  private Integer pipelineBatch;
  private Integer pipelineCap;
  private JsonArray positionalParameters;
  private QueryProfile profile;
  private Map<String, Object> raw;
  private boolean readonly = false;
  private String scanWait;
  private Integer scanCap;
  private QueryScanConsistency scanConsistency;
  private JsonSerializer serializer;

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
  public QueryOptions raw(final String param, final Object value) {
    if (raw == null) {
      raw = new HashMap<>();
    }
    raw.put(param, value);
    return this;
  }

  /**
   * If set to false, enables transparent prepared statement support.
   *
   * @param adhoc if this is an ad-hoc query.
   * @return {@link QueryOptions} for further chaining.
   */
  public QueryOptions adhoc(final boolean adhoc) {
    this.adhoc = adhoc;
    return this;
  }

  /**
   * Scan consistency for the query
   *
   * @param scanConsistency the index scan consistency to be used
   * @return {@link QueryOptions} for further chaining
   */
  public QueryOptions scanConsistency(final QueryScanConsistency scanConsistency) {
    this.scanConsistency = scanConsistency;
    consistentWith = null;
    return this;
  }

  public QueryOptions serializer(final JsonSerializer serializer) {
    this.serializer = serializer;
    return this;
  }

  /**
   * Set the profiling information level for query execution
   *
   * @param profile the query profile level to be used
   * @return {@link QueryOptions} for further chaining
   */
  public QueryOptions profile(final QueryProfile profile) {
    this.profile = profile;
    return this;
  }

  /**
   * Adds a client context ID to the request, that will be sent back in the response, allowing clients
   * to meaningfully trace requests/responses when many are exchanged.
   *
   * @param clientContextId the client context ID (null to send none)
   * @return this {@link QueryOptions} for chaining.
   */
  public QueryOptions clientContextId(final String clientContextId) {
    this.clientContextId = clientContextId;
    return this;
  }

  /**
   * If set to true (false being the default), the metrics object will be returned from N1QL and
   * as a result provide insight into timings.
   *
   * @param metrics true if enabled, false otherwise (true = default).
   * @return this {@link QueryOptions} for chaining.
   */
  public QueryOptions metrics(boolean metrics) {
    this.metrics = metrics;
    return this;
  }

  /**
   * If the {@link QueryScanConsistency#NOT_BOUNDED scan consistency} has been chosen, does nothing.
   *
   * Otherwise, sets the maximum time the client is willing to wait for an index to catch up to the
   * vector timestamp in the request.
   *
   * @param wait the duration.
   * @return this {@link QueryOptions} for chaining.
   */
  public QueryOptions scanWait(final Duration wait) {
    if (this.scanConsistency != null && this.scanConsistency == QueryScanConsistency.NOT_BOUNDED) {
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
  public QueryOptions maxParallelism(int maxParallelism) {
    this.maxParallelism = maxParallelism;
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
    this.readonly = readonly;
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
  public QueryOptions parameters(final JsonObject named) {
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
  public QueryOptions parameters(final JsonArray positional) {
    this.positionalParameters = positional;
    namedParameters = null;
    return this;
  }

  /**
   * Sets the {@link MutationToken}s this query should be consistent with. These tokens are returned from mutations.
   *
   * @param mutationState the mutation tokens
   * @return this {@link QueryOptions} for chaining.
   */
  public QueryOptions consistentWith(final MutationState mutationState) {
    this.consistentWith = mutationState;
    scanConsistency = null;
    return this;
  }

  @Stability.Internal
  public Built build() {
    return new Built();
  }

  public class Built extends BuiltCommonOptions {

    boolean adhoc() {
      return adhoc;
    }

    public boolean readonly() {
      return readonly;
    }

    public JsonSerializer serializer() {
      return serializer;
    }

    @Stability.Internal
    public void injectParams(final JsonObject queryJson) {
      queryJson.put("client_context_id", clientContextId == null ? UUID.randomUUID().toString() : clientContextId);

      boolean positionalPresent = positionalParameters != null && !positionalParameters.isEmpty();
      if (namedParameters != null && !namedParameters.isEmpty()) {
        if (positionalPresent) {
          throw new IllegalArgumentException("Both positional and named parameters cannot be present at the same time!");
        }

        namedParameters.getNames().forEach(key -> {
          Object value = namedParameters.get(key);
          if (key.charAt(0) != '$') {
            queryJson.put('$' + key, value);
          } else {
            queryJson.put(key, value);
          }
        });
      }

      if (positionalPresent) {
        queryJson.put("args", positionalParameters);
      }

      if (scanConsistency != null && scanConsistency != QueryScanConsistency.NOT_BOUNDED) {
        queryJson.put("scan_consistency", scanConsistency.toString());
      }

      if (consistentWith != null) {
        JsonObject mutationState = JsonObject.create();
        for (MutationToken token : consistentWith) {
          JsonObject bucket = mutationState.getObject(token.bucketName());
          if (bucket == null) {
            bucket = JsonObject.create();
            mutationState.put(token.bucketName(), bucket);
          }

          bucket.put(
            String.valueOf(token.partitionID()),
            JsonArray.from(token.sequenceNumber(), String.valueOf(token.partitionUUID()))
          );
        }
        queryJson.put("scan_vectors", mutationState);
        queryJson.put("scan_consistency", "at_plus");
      }

      if (profile != null && profile != QueryProfile.OFF) {
        queryJson.put("profile", profile.toString());
      }

      if (scanWait != null && !scanWait.isEmpty()) {
        if (scanConsistency == null || QueryScanConsistency.NOT_BOUNDED != scanConsistency) {
          queryJson.put("scan_wait", scanWait);
        }
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

      if (!metrics) {
        queryJson.put("metrics", false);
      }

      if (readonly) {
        queryJson.put("readonly", true);
      }

      if (raw != null) {
        for (Map.Entry<String, Object> entry : raw.entrySet()) {
          queryJson.put(entry.getKey(), entry.getValue());
        }
      }
    }
  }

}
