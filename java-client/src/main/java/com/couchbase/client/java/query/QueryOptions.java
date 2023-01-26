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

import com.couchbase.client.core.annotation.SinceCouchbase;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.JsonProcessingException;
import com.couchbase.client.core.deps.com.google.protobuf.ByteString;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.msg.kv.MutationToken;
import com.couchbase.client.core.transaction.config.CoreSingleQueryTransactionOptions;
import com.couchbase.client.core.util.Golang;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CommonOptions;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.MutationState;
import com.couchbase.client.java.transactions.config.SingleQueryTransactionOptions;
import com.couchbase.client.protostellar.query.v1.QueryRequest;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;

/**
 * Allows customizing various N1QL query options.
 * <p>
 * This object is NOT thread safe and must be constructed on a single thread and then passed to the
 * {@link Cluster#query(String, QueryOptions)} method.
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
  private Duration scanWaitOriginal;
  private Integer scanCap;
  private QueryScanConsistency scanConsistency;
  private JsonSerializer serializer;
  private boolean flexIndex = false;
  private Boolean preserveExpiry = null;
  private boolean asTransaction = false;
  private CoreSingleQueryTransactionOptions asTransactionOptions;

  /**
   * The options should only be instantiated through the {@link #queryOptions()} static method.
   */
  protected QueryOptions() {}

  /**
   * Creates new {@link QueryOptions} with all default params set.
   *
   * @return constructed {@link QueryOptions} with its default values.
   */
  public static QueryOptions queryOptions() {
    return new QueryOptions();
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
   * @return the same {@link QueryOptions} for chaining purposes.
   */
  public QueryOptions raw(final String key, final Object value) {
    notNullOrEmpty(key, "Key");
    if (raw == null) {
      raw = new HashMap<>();
    }
    raw.put(key, value);
    return this;
  }

  /**
   * Allows turning this request into a prepared statement query.
   * <p>
   * If set to <pre>false</pre>, the SDK will transparently perform "prepare and execute" logic the first time this
   * query is seen and then subsequently reuse the prepared statement name when sending it to the server. If a query
   * is executed frequently, this is a good way to speed it up since it will save the server the task of re-parsing
   * and analyzing the query.
   * <p>
   * If you are using prepared statements, make sure that if certain parts of the query string change you are using
   * {@link #parameters(JsonObject) named} or {@link #parameters(JsonArray) positional} parameters. If the statement
   * string itself changes it cannot be cached.
   *
   * @param adhoc if set to false this query will be turned into a prepared statement query.
   * @return the same {@link QueryOptions} for chaining purposes.
   */
  public QueryOptions adhoc(final boolean adhoc) {
    this.adhoc = adhoc;
    return this;
  }

  /**
   * Customizes the consistency guarantees for this query.
   * <p>
   * Tuning the scan consistency allows to trade data "freshness" for latency and vice versa. By default
   * {@link QueryScanConsistency#NOT_BOUNDED} is used, which means that the server returns the data it has in the
   * index right away. This is fast, but might not include the most recent mutations. If you want to include all
   * the mutations up to the point of the query, use {@link QueryScanConsistency#REQUEST_PLUS}.
   * <p>
   * Note that you cannot use this method and {@link #consistentWith(MutationState)} at the same time, since they
   * are mutually exclusive. As a rule of thumb, if you only care to be consistent with the mutation you just wrote
   * on the same thread/app, use {@link #consistentWith(MutationState)}. If you need "global" scan consistency, use
   * {@link QueryScanConsistency#REQUEST_PLUS} on this method.
   *
   * @param scanConsistency the index scan consistency to be used for this query.
   * @return the same {@link QueryOptions} for chaining purposes.
   */
  public QueryOptions scanConsistency(final QueryScanConsistency scanConsistency) {
    notNull(scanConsistency, "QueryScanConsistency");
    this.scanConsistency = scanConsistency;
    consistentWith = null;
    return this;
  }

  /**
   * Provides a custom {@link JsonSerializer} to be used for decoding the rows as they return from the server.
   * <p>
   * If no serializer is provided, the default one from the {@link ClusterEnvironment} will be used (which is
   * sufficient for most tasks at hand).
   *
   * @param serializer a custom serializer for this request.
   * @return the same {@link QueryOptions} for chaining purposes.
   */
  public QueryOptions serializer(final JsonSerializer serializer) {
    notNull(serializer, "JsonSerializer");
    this.serializer = serializer;
    return this;
  }

  /**
   * Customizes the server profiling level for this query.
   * <p>
   * Note that you only want to tune this if you want to gather profiling/performance metrics for debugging. Turning
   * this on in production (depending on the level) will likely have performance impact on the server query engine
   * as a whole and on this query in particular!
   * <p>
   * This is an Enterprise Edition feature.  On Community Edition the parameter will be accepted, but no profiling
   * information returned.
   *
   * @param profile the custom query profile level for this query.
   * @return the same {@link QueryOptions} for chaining purposes.
   */
  public QueryOptions profile(final QueryProfile profile) {
    notNull(profile, "QueryProfile");
    this.profile = profile;
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
   * @return the same {@link QueryOptions} for chaining purposes.
   */
  public QueryOptions clientContextId(final String clientContextId) {
    if (clientContextId == null || clientContextId.isEmpty()) {
      this.clientContextId = null;
    } else {
      this.clientContextId = clientContextId;
    }
    return this;
  }

  /**
   * Enables per-request metrics in the trailing section of the query.
   * <p>
   * If this method is set to true, the server will send metrics back to the client which are available through the
   * {@link QueryMetaData#metrics()} section. As opposed to {@link #profile(QueryProfile)}, returning metrics is rather
   * cheap and can also be enabled in production if needed.
   *
   * @param metrics set to true if the server should return simple query metrics.
   * @return the same {@link QueryOptions} for chaining purposes.
   */
  public QueryOptions metrics(final boolean metrics) {
    this.metrics = metrics;
    return this;
  }

  /**
   * Allows customizing how long the query engine is willing to wait until the index catches up to whatever scan
   * consistency is asked for in this query.
   * <p>
   * Note that if {@link QueryScanConsistency#NOT_BOUNDED} is used, this method doesn't do anything
   * at all. If no value is provided to this method, the server default is used.
   *
   * @param wait the maximum duration the query engine is willing to wait before failing.
   * @return the same {@link QueryOptions} for chaining purposes.
   */
  public QueryOptions scanWait(final Duration wait) {
    notNull(wait, "Wait Duration");
    if (this.scanConsistency != null && this.scanConsistency == QueryScanConsistency.NOT_BOUNDED) {
      this.scanWait = null;
      this.scanWaitOriginal = null;
    } else {
      this.scanWait = Golang.encodeDurationToMs(wait);
      this.scanWaitOriginal = wait;
    }
    return this;
  }

  /**
   * Allows overriding the default maximum parallelism for the query execution on the server side.
   * <p>
   * If 0 or a negative value is set, the parallelism is disabled. If not provided, the server default will
   * be used.
   *
   * @param maxParallelism the maximum parallelism for this query, 0 or negative values disable it.
   * @return the same {@link QueryOptions} for chaining purposes.
   */
  public QueryOptions maxParallelism(final int maxParallelism) {
    this.maxParallelism = maxParallelism;
    return this;
  }

  /**
   * Allows explicitly marking a query as being readonly and not mutating and documents on the server side.
   * <p>
   * In addition to providing some security in that you are not accidentally modifying data, setting this flag to true
   * also helps the client to more proactively retry and re-dispatch a query since then it can be sure it is idempotent.
   * As a result, if your query is readonly then it is a good idea to set this flag.
   * <p>
   * If set to true, then (at least) the following statements are not allowed:
   * <ol>
   *  <li>CREATE INDEX</li>
   *  <li>DROP INDEX</li>
   *  <li>INSERT</li>
   *  <li>MERGE</li>
   *  <li>UPDATE</li>
   *  <li>UPSERT</li>
   *  <li>DELETE</li>
   * </ol>
   *
   * @param readonly true if readonly should be set, false is the default and will use the server side default.
   * @return the same {@link QueryOptions} for chaining purposes.
   */
  public QueryOptions readonly(final boolean readonly) {
    this.readonly = readonly;
    return this;
  }

  /**
   * Supports customizing the maximum buffered channel size between the indexer and the query service.
   * <p>
   * This is an advanced API and should only be tuned with care. Use 0 or a negative number to disable.
   *
   * @param scanCap the scan cap size, use 0 or negative number to disable.
   * @return the same {@link QueryOptions} for chaining purposes.
   */
  public QueryOptions scanCap(final int scanCap) {
    this.scanCap = scanCap;
    return this;
  }

  /**
   * Supports customizing the number of items execution operators can batch for fetch from the KV layer on the server.
   * <p>
   * This is an advanced API and should only be tuned with care.
   *
   * @param pipelineBatch the pipeline batch size.
   * @return the same {@link QueryOptions} for chaining purposes.
   */
  public QueryOptions pipelineBatch(final int pipelineBatch) {
    this.pipelineBatch = pipelineBatch;
    return this;
  }

  /**
   * Allows customizing the maximum number of items each execution operator can buffer between various operators on the
   * server.
   * <p>
   * This is an advanced API and should only be tuned with care.
   *
   * @param pipelineCap the pipeline cap size.
   * @return the same {@link QueryOptions} for chaining purposes.
   */
  public QueryOptions pipelineCap(final int pipelineCap) {
    this.pipelineCap = pipelineCap;
    return this;
  }

  /**
   * Sets named parameters for this query.
   * <p>
   * Note that named and positional parameters cannot be used at the same time. If one is set, the other one is "nulled"
   * out and not being sent to the server.
   *
   * @param named {@link JsonObject} the named parameters.
   * @return the same {@link QueryOptions} for chaining purposes.
   */
  public QueryOptions parameters(final JsonObject named) {
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
   * @return the same {@link QueryOptions} for chaining purposes.
   */
  public QueryOptions parameters(final JsonArray positional) {
    notNull(positional, "Positional Parameters");
    this.positionalParameters = positional;
    namedParameters = null;
    return this;
  }

  /**
   * Sets the {@link MutationToken}s this query should be consistent with.
   * <p>
   * These mutation tokens are returned from mutations (i.e. as part of a {@link MutationResult}) and if you want your
   * N1QL query to include those you need to pass the mutation tokens into a {@link MutationState}.
   * <p>
   * Note that you cannot use this method and {@link #scanConsistency(QueryScanConsistency)} at the same time, since
   * they are mutually exclusive. As a rule of thumb, if you only care to be consistent with the mutation you just wrote
   * on the same thread/app, use this method. If you need "global" scan consistency, use
   * {@link QueryScanConsistency#REQUEST_PLUS} on {@link #scanConsistency(QueryScanConsistency)}.
   *
   * @param mutationState the mutation state containing the mutation tokens.
   * @return the same {@link QueryOptions} for chaining purposes.
   */
  public QueryOptions consistentWith(final MutationState mutationState) {
    notNull(mutationState, "MutationState");
    this.consistentWith = mutationState;
    scanConsistency = null;
    return this;
  }

  /**
   * Tells the query engine to use a flex index (utilizing the search service).
   *
   * @param flexIndex if a flex index should be used, false is the default.
   * @return the same {@link QueryOptions} for chaining purposes.
   */
  public QueryOptions flexIndex(final boolean flexIndex) {
    this.flexIndex = flexIndex;
    return this;
  }

  /**
   * Tells the query engine to preserve expiration values set on any documents modified by this query.
   * <p>
   * This feature works from Couchbase Server 7.1.0 onwards.
   *
   * @param preserveExpiry if expiration values should be preserved, the default is false.
   * @return the same {@link QueryOptions} for chaining purposes.
   */
  @SinceCouchbase("7.1")
  public QueryOptions preserveExpiry(final boolean preserveExpiry) {
    this.preserveExpiry = preserveExpiry;
    return this;
  }

  @Stability.Internal
  public Built build() {
    return new Built();
  }

  /**
   * Performs this as a single query transaction, with default options.
   * <p>
   * @return the same {@link QueryOptions} for chaining purposes.
   */
  @SinceCouchbase("7.0")
  public QueryOptions asTransaction() {
    asTransaction = true;
    return this;
  }

  /**
   * Performs this as a single query transaction.
   * <p>
   * @param options configuration options to use.
   * @return the same {@link QueryOptions} for chaining purposes.
   */
  @SinceCouchbase("7.0")
  public QueryOptions asTransaction(final SingleQueryTransactionOptions options) {
    notNull(options, "asTransaction");
    asTransaction = true;
    asTransactionOptions = options.build();
    return this;
  }

  @Stability.Internal
  public class Built extends BuiltCommonOptions {

    Built() { }

    boolean adhoc() {
      return adhoc;
    }

    public boolean readonly() {
      return readonly;
    }

    public JsonSerializer serializer() {
      return serializer;
    }

    public String clientContextId() {
      return clientContextId;
    }

    public boolean asTransaction() {
      return asTransaction;
    }

    public void injectParams(final com.couchbase.client.protostellar.query.v1.QueryRequest.Builder input) {
      input.setClientContextId(clientContextId == null ? UUID.randomUUID().toString() : clientContextId);

      if (scanConsistency != null) {
        input.setScanConsistency(QueryRequest.QueryScanConsistency.valueOf(scanConsistency.name()));
      }

      boolean positionalPresent = positionalParameters != null && !positionalParameters.isEmpty();
      if (namedParameters != null && !namedParameters.isEmpty()) {
        if (positionalPresent) {
          throw InvalidArgumentException.fromMessage("Both positional and named parameters cannot be present at the same time!");
        }

        namedParameters.getNames().forEach(key -> {
          Object value = namedParameters.get(key);
          try {
            ByteString bs = ByteString.copyFrom(Mapper.writer().writeValueAsBytes(value));
            input.putNamedParameters(key, bs);
          } catch (JsonProcessingException e) {
            throw new InvalidArgumentException("Unable to JSON encode named parameter " + key, e, null);
          }
        });
      }

      if (positionalPresent) {
        positionalParameters.iterator().forEachRemaining(it -> {
          try {
            input.addPositionalParameters(ByteString.copyFrom(Mapper.writer().writeValueAsBytes(it)));
          } catch (JsonProcessingException e) {
            throw new InvalidArgumentException("Unable to JSON encode positional parameter " + it, e, null);
          }
        });
      }


      if (scanConsistency == QueryScanConsistency.REQUEST_PLUS) {
        input.setScanConsistency(QueryRequest.QueryScanConsistency.REQUEST_PLUS);
      }

      if (consistentWith != null) {
        for (MutationToken token : consistentWith) {
          input.addConsistentWith(com.couchbase.client.protostellar.kv.v1.MutationToken.newBuilder()
              .setSeqNo(token.sequenceNumber())
              .setVbucketId(token.partitionID())
              .setVbucketUuid(token.partitionUUID())
              .setBucketName(token.bucketName())
              .build());
        }
      }

      if (profile != null && profile != QueryProfile.OFF) {
        switch (profile) {
          case TIMINGS:
            input.setProfileMode(QueryRequest.QueryProfileMode.TIMINGS);
            break;
          case PHASES:
            input.setProfileMode(QueryRequest.QueryProfileMode.PHASES);
            break;
        }
      }

      QueryRequest.TuningOptions.Builder tuning = null;

      if (scanWait != null && !scanWait.isEmpty()) {
        if (scanConsistency == null || QueryScanConsistency.NOT_BOUNDED != scanConsistency) {
          if (tuning == null) {
            tuning = QueryRequest.TuningOptions.newBuilder();
          }
          tuning.setScanWait(com.couchbase.client.core.deps.com.google.protobuf.Duration.newBuilder().setSeconds(TimeUnit.NANOSECONDS.toSeconds(scanWaitOriginal.toNanos())));
        }
      }

      if (maxParallelism != null) {
        if (tuning == null) {
          tuning = QueryRequest.TuningOptions.newBuilder();
        }
        tuning.setMaxParallelism(maxParallelism);
      }

      if (pipelineCap != null) {
        if (tuning == null) {
          tuning = QueryRequest.TuningOptions.newBuilder();
        }
        tuning.setPipelineCap(pipelineCap);
      }

      if (pipelineBatch != null) {
        if (tuning == null) {
          tuning = QueryRequest.TuningOptions.newBuilder();
        }
        tuning.setPipelineBatch(pipelineBatch);
      }

      if (scanCap != null) {
        if (tuning == null) {
          tuning = QueryRequest.TuningOptions.newBuilder();
        }
        tuning.setScanCap(scanCap);
      }

      if (!metrics) {
        if (tuning == null) {
          tuning = QueryRequest.TuningOptions.newBuilder();
        }
        tuning.setDisableMetrics(!metrics);
      }

      if (readonly) {
        input.setReadOnly(readonly);
      }

      if (flexIndex) {
        input.setFlexIndex(flexIndex);
      }

      if (preserveExpiry != null) {
        input.setPreserveExpiry(preserveExpiry);
      }

      if (!adhoc) {
        input.setPrepared(true);
      }

      if (raw != null) {
        throw new UnsupportedOperationException("Raw options cannot be used together with Protostellar");
      }
    }

    @Stability.Internal
    public void injectParams(final JsonObject queryJson) {
      queryJson.put("client_context_id", clientContextId == null ? UUID.randomUUID().toString() : clientContextId);

      boolean positionalPresent = positionalParameters != null && !positionalParameters.isEmpty();
      if (namedParameters != null && !namedParameters.isEmpty()) {
        if (positionalPresent) {
          throw InvalidArgumentException.fromMessage("Both positional and named parameters cannot be present at the same time!");
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

      if (flexIndex) {
        queryJson.put("use_fts", true);
      }

      if (preserveExpiry != null) {
        queryJson.put("preserve_expiry", preserveExpiry);
      }

      if (raw != null) {
        for (Map.Entry<String, Object> entry : raw.entrySet()) {
          queryJson.put(entry.getKey(), entry.getValue());
        }
      }
    }

    public CoreSingleQueryTransactionOptions asTransactionOptions() {
      return asTransactionOptions;
    }
  }

}
