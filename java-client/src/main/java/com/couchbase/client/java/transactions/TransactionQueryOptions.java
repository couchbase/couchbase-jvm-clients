/*
 * Copyright 2022 Couchbase, Inc.
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
package com.couchbase.client.java.transactions;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryProfile;
import com.couchbase.client.java.query.QueryScanConsistency;

import java.time.Duration;

/**
 * Allows customizing various N1QL query options.
 * <p>
 * This object is NOT thread safe and must be constructed on a single thread.
 */
public class TransactionQueryOptions {
  private QueryOptions builder = QueryOptions.queryOptions()
          // Metrics are currently always exposed as they add invaluable debugging
          .metrics(true);

  /**
   * The options should only be instantiated through the {@link #queryOptions()} static method.
   */
  protected TransactionQueryOptions() {}

  /**
   * Creates new {@link QueryOptions} with all default params set.
   *
   * @return constructed {@link QueryOptions} with its default values.
   */
  public static TransactionQueryOptions queryOptions() {
    return new TransactionQueryOptions();
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
  public TransactionQueryOptions raw(final String key, final Object value) {
    builder.raw(key, value);
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
  public TransactionQueryOptions adhoc(final boolean adhoc) {
    builder.adhoc(adhoc);
    return this;
  }

  /**
   * Customizes the consistency guarantees for this query.
   * <p>
   * Tuning the scan consistency allows to trade data "freshness" for latency and vice versa. By default
   * {@link QueryScanConsistency#REQUEST_PLUS} is used for any queries inside a transaction, which means that the
   * indexer will wait until any indexes used are consistent with all mutations at the time of the query.
   * If this level of consistency is not required, use {@link QueryScanConsistency#NOT_BOUNDED} which will execute
   * the query immediately with whatever data are in the index.
   * <p>
   * @param scanConsistency the index scan consistency to be used for this query.
   * @return this, for chaining purposes.
   */
  public TransactionQueryOptions scanConsistency(final QueryScanConsistency scanConsistency) {
    builder.scanConsistency(scanConsistency);
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
  public TransactionQueryOptions serializer(final JsonSerializer serializer) {
    builder.serializer(serializer);
    return this;
  }

  /**
   * Customizes the server profiling level for this query.
   * <p>
   * Note that you only want to tune this if you want to gather profiling/performance metrics for debugging. Turning
   * this on in production (depending on the level) will likely have performance impact on the server query engine
   * as a whole and on this query in particular!
   *
   * @param profile the custom query profile level for this query.
   * @return the same {@link QueryOptions} for chaining purposes.
   */
  public TransactionQueryOptions profile(final QueryProfile profile) {
    builder.profile(profile);
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
  public TransactionQueryOptions clientContextId(final String clientContextId) {
    builder.clientContextId(clientContextId);
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
  public TransactionQueryOptions scanWait(final Duration wait) {
    builder.scanWait(wait);
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
  public TransactionQueryOptions readonly(final boolean readonly) {
    builder.readonly(readonly);
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
  public TransactionQueryOptions scanCap(final int scanCap) {
    builder.scanCap(scanCap);
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
  public TransactionQueryOptions pipelineBatch(final int pipelineBatch) {
    builder.pipelineBatch(pipelineBatch);
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
  public TransactionQueryOptions pipelineCap(final int pipelineCap) {
    builder.pipelineCap(pipelineCap);
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
  public TransactionQueryOptions parameters(final JsonObject named) {
    builder.parameters(named);
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
  public TransactionQueryOptions parameters(final JsonArray positional) {
    builder.parameters(positional);
    return this;
  }

  /**
   * Tells the query engine to use a flex index (utilizing the search service).
   *
   * @param flexIndex if a flex index should be used, false is the default.
   * @return the same {@link QueryOptions} for chaining purposes.
   */
  public TransactionQueryOptions flexIndex(final boolean flexIndex) {
    builder.flexIndex(flexIndex);
    return this;
  }

  @Stability.Internal
  QueryOptions builder() {
    return builder;
  }
}
