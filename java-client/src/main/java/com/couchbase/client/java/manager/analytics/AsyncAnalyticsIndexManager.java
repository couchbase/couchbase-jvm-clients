/*
 * Copyright 2019 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.java.manager.analytics;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.type.TypeReference;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.DefaultFullHttpRequest;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpResponseStatus;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpVersion;
import com.couchbase.client.core.error.AnalyticsException;
import com.couchbase.client.core.error.DataverseExistsException;
import com.couchbase.client.core.error.DataverseNotFoundException;
import com.couchbase.client.core.error.FeatureNotAvailableException;
import com.couchbase.client.core.error.HttpStatusCodeException;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.msg.analytics.GenericAnalyticsRequest;
import com.couchbase.client.core.msg.analytics.GenericAnalyticsResponse;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.java.AsyncCluster;
import com.couchbase.client.java.CommonOptions;
import com.couchbase.client.java.analytics.AnalyticsOptions;
import com.couchbase.client.java.analytics.AnalyticsResult;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static com.couchbase.client.core.util.CbThrowables.findCause;
import static com.couchbase.client.java.analytics.AnalyticsOptions.analyticsOptions;
import static com.couchbase.client.java.manager.analytics.ConnectLinkAnalyticsOptions.connectLinkAnalyticsOptions;
import static com.couchbase.client.java.manager.analytics.CreateDatasetAnalyticsOptions.createDatasetAnalyticsOptions;
import static com.couchbase.client.java.manager.analytics.CreateDataverseAnalyticsOptions.createDataverseAnalyticsOptions;
import static com.couchbase.client.java.manager.analytics.CreateIndexAnalyticsOptions.createIndexAnalyticsOptions;
import static com.couchbase.client.java.manager.analytics.DisconnectLinkAnalyticsOptions.disconnectLinkAnalyticsOptions;
import static com.couchbase.client.java.manager.analytics.DropDatasetAnalyticsOptions.dropDatasetAnalyticsOptions;
import static com.couchbase.client.java.manager.analytics.DropDataverseAnalyticsOptions.dropDataverseAnalyticsOptions;
import static com.couchbase.client.java.manager.analytics.DropIndexAnalyticsOptions.dropIndexAnalyticsOptions;
import static com.couchbase.client.java.manager.analytics.GetAllDatasetsAnalyticsOptions.getAllDatasetsAnalyticsOptions;
import static com.couchbase.client.java.manager.analytics.GetAllIndexesAnalyticsOptions.getAllIndexesAnalyticsOptions;
import static com.couchbase.client.java.manager.analytics.GetPendingMutationsAnalyticsOptions.getPendingMutationsAnalyticsOptions;
import static java.util.Collections.singletonMap;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class AsyncAnalyticsIndexManager {
  private static final int DATAVERSE_NOT_FOUND = 24034;
  private static final int DATAVERSE_ALREADY_EXISTS = 24039;
  private static final int DATASET_NOT_FOUND = 24025;
  private static final int DATASET_ALREADY_EXISTS = 24040;
  private static final int INDEX_NOT_FOUND = 24047;
  private static final int INDEX_ALREADY_EXISTS = 24048;
  private static final int LINK_NOT_FOUND = 24006;

  private final AsyncCluster cluster;
  private final Core core;

  private static final String DEFAULT_DATAVERSE = "Default";
  private static final String DEFAULT_LINK = "Local";

  public AsyncAnalyticsIndexManager(AsyncCluster cluster) {
    this.cluster = requireNonNull(cluster);
    this.core = cluster.core();
  }

  public CompletableFuture<Void> createDataverse(String dataverseName) {
    return createDataverse(dataverseName, createDataverseAnalyticsOptions());
  }

  /**
   * @throws DataverseExistsException
   */
  public CompletableFuture<Void> createDataverse(String dataverseName, CreateDataverseAnalyticsOptions options) {
    requireNonNull(dataverseName);

    final CreateDataverseAnalyticsOptions.Built builtOpts = options.build();

    String statement = "CREATE DATAVERSE " + quote(dataverseName);
    if (builtOpts.ignoreIfExists()) {
      statement += " IF NOT EXISTS";
    }

    return exec(statement, builtOpts)
        .thenApply(result -> null);
  }


  public CompletableFuture<List<AnalyticsDataverse>> getAllDataverses() {
    return cluster.analyticsQuery("SELECT DataverseName from Metadata.`Dataverse`")
        .thenApply(result -> result.rowsAsObject().stream()
            .map((dv) -> dv.put("DataverseName",((String)dv.get("DataverseName")).replace("@.",".")))
            .map(AnalyticsDataverse::new)
            .collect(toList()));
  }

  /**
   * @throws DataverseNotFoundException
   */
  public CompletableFuture<Void> dropDataverse(String dataverseName) {
    return dropDataverse(dataverseName, dropDataverseAnalyticsOptions());
  }

  /**
   * @throws DataverseNotFoundException
   */
  public CompletableFuture<Void> dropDataverse(String dataverseName, DropDataverseAnalyticsOptions options) {
    requireNonNull(dataverseName);

    final DropDataverseAnalyticsOptions.Built builtOpts = options.build();

    String statement = "DROP DATAVERSE " + quote(dataverseName);
    if (builtOpts.ignoreIfNotExists()) {
      statement += " IF EXISTS";
    }

    return exec(statement, builtOpts)
        .thenApply(result -> null);
  }

  public CompletableFuture<Void> createDataset(String datasetName, String bucketName) {
    return createDataset(datasetName, bucketName, createDatasetAnalyticsOptions());
  }

  public CompletableFuture<Void> createDataset(String datasetName, String bucketName, CreateDatasetAnalyticsOptions options) {
    requireNonNull(datasetName);
    requireNonNull(bucketName);

    final CreateDatasetAnalyticsOptions.Built builtOpts = options.build();
    final String dataverseName = builtOpts.dataverseName().orElse(DEFAULT_DATAVERSE);
    final String condition = builtOpts.condition().orElse(null);

    String statement = "CREATE DATASET ";
    if (builtOpts.ignoreIfExists()) {
      statement += "IF NOT EXISTS ";
    }

    statement += quote(dataverseName, datasetName) + " ON " + quote(bucketName);

    if (condition != null) {
      statement += " WHERE " + condition;
    }

    return exec(statement, builtOpts)
        .thenApply(result -> null);
  }

  public CompletableFuture<Void> dropDataset(String datasetName) {
    return dropDataset(datasetName, dropDatasetAnalyticsOptions());
  }

  public CompletableFuture<Void> dropDataset(String datasetName, DropDatasetAnalyticsOptions options) {
    requireNonNull(datasetName);

    final DropDatasetAnalyticsOptions.Built builtOpts = options.build();
    final String dataverseName = builtOpts.dataverseName().orElse(DEFAULT_DATAVERSE);

    String statement = "DROP DATASET " + quote(dataverseName, datasetName);
    if (builtOpts.ignoreIfNotExists()) {
      statement += " IF EXISTS";
    }

    return exec(statement, builtOpts)
        .thenApply(result -> null);
  }

  public CompletableFuture<List<AnalyticsDataset>> getAllDatasets() {
    return getAllDatasets(getAllDatasetsAnalyticsOptions());
  }

  public CompletableFuture<List<AnalyticsDataset>> getAllDatasets(GetAllDatasetsAnalyticsOptions options) {
    final GetAllDatasetsAnalyticsOptions.Built builtOpts = options.build();
    String statement = "SELECT d.* FROM Metadata.`Dataset` d WHERE d.DataverseName <> \"Metadata\"";

    return exec(statement, builtOpts)
        .thenApply(result -> result.rowsAsObject().stream()
            .map(AnalyticsDataset::new)
            .collect(toList()));
  }

  public CompletableFuture<Void> createIndex(String indexName, String datasetName, Map<String, AnalyticsDataType> fields) {
    return createIndex(indexName, datasetName, fields, createIndexAnalyticsOptions());
  }

  public CompletableFuture<Void> createIndex(String indexName, String datasetName, Map<String, AnalyticsDataType> fields, CreateIndexAnalyticsOptions options) {
    requireNonNull(indexName);
    requireNonNull(datasetName);

    final CreateIndexAnalyticsOptions.Built builtOpts = options.build();
    final String dataverseName = builtOpts.dataverseName().orElse(DEFAULT_DATAVERSE);

    String statement = "CREATE INDEX " + quote(indexName);

    if (builtOpts.ignoreIfExists()) {
      statement += " IF NOT EXISTS";
    }

    statement += " ON " + quote(dataverseName, datasetName) + " " + formatIndexFields(fields);

    return exec(statement, builtOpts)
        .thenApply(result -> null);
  }

  public CompletableFuture<List<AnalyticsIndex>> getAllIndexes() {
    return getAllIndexes(getAllIndexesAnalyticsOptions());
  }

  public CompletableFuture<List<AnalyticsIndex>> getAllIndexes(GetAllIndexesAnalyticsOptions options) {
    final GetAllIndexesAnalyticsOptions.Built builtOpts = options.build();
    String statement = "SELECT d.* FROM Metadata.`Index` d WHERE d.DataverseName <> \"Metadata\"";

    return exec(statement, builtOpts)
        .thenApply(result -> result.rowsAsObject().stream()
            .map(AnalyticsIndex::new)
            .collect(toList()));
  }

  private static String formatIndexFields(Map<String, AnalyticsDataType> fields) {
    List<String> result = new ArrayList<>();
    fields.forEach((k, v) -> result.add(k + ":" + v.value()));
    return "(" + String.join(",", result) + ")";
  }

  public CompletableFuture<Void> dropIndex(String indexName, String datasetName) {
    return dropIndex(indexName, datasetName, dropIndexAnalyticsOptions());
  }

  public CompletableFuture<Void> dropIndex(String indexName, String datasetName, DropIndexAnalyticsOptions options) {
    final DropIndexAnalyticsOptions.Built builtOpts = options.build();
    final String dataverseName = builtOpts.dataverseName().orElse(DEFAULT_DATAVERSE);

    String statement = "DROP INDEX " + quote(dataverseName, datasetName, indexName);
    if (builtOpts.ignoreIfNotExists()) {
      statement += " IF EXISTS";
    }

    return exec(statement, builtOpts)
        .thenApply(result -> null);
  }

  public CompletableFuture<Void> connectLink() {
    return connectLink(connectLinkAnalyticsOptions());
  }

  public CompletableFuture<Void> connectLink(ConnectLinkAnalyticsOptions options) {
    final ConnectLinkAnalyticsOptions.Built builtOpts = options.build();
    String statement = "CONNECT LINK " + quote(
        builtOpts.dataverseName().orElse(DEFAULT_DATAVERSE),
        builtOpts.linkName().orElse(DEFAULT_LINK));

    if (builtOpts.force()) {
      statement += " WITH " + Mapper.encodeAsString(singletonMap("force", true));
    }

    return exec(statement, builtOpts)
        .thenApply(result -> null);
  }

  public CompletableFuture<Void> disconnectLink() {
    return disconnectLink(disconnectLinkAnalyticsOptions());
  }

  public CompletableFuture<Void> disconnectLink(DisconnectLinkAnalyticsOptions options) {
    final DisconnectLinkAnalyticsOptions.Built builtOpts = options.build();
    String statement = "DISCONNECT LINK " + quote(
        builtOpts.dataverseName().orElse(DEFAULT_DATAVERSE),
        builtOpts.linkName().orElse(DEFAULT_LINK));

    return exec(statement, builtOpts)
        .thenApply(result -> null);
  }

  public CompletableFuture<Map<String, Long>> getPendingMutations() {
    return getPendingMutations(getPendingMutationsAnalyticsOptions());
  }

  public CompletableFuture<Map<String, Long>> getPendingMutations(GetPendingMutationsAnalyticsOptions options) {
    return sendRequest(HttpMethod.GET, "/analytics/node/agg/stats/remaining", options.build())
        .exceptionally(t -> {
          throw translateException(t);
        })
        .thenApply(response ->
            Mapper.decodeInto(response.content(), new TypeReference<Map<String, Long>>() {
            }));
  }

  private CompletableFuture<GenericAnalyticsResponse> sendRequest(HttpMethod method, String path, CommonOptions<?>.BuiltCommonOptions options) {
    return sendRequest(new GenericAnalyticsRequest(timeout(options), core.context(), retryStrategy(options),
        () -> new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, path), method == HttpMethod.GET));
  }

  private CompletableFuture<GenericAnalyticsResponse> sendRequest(GenericAnalyticsRequest request) {
    core.send(request);
    return request.response();
  }

  private Duration timeout(CommonOptions<?>.BuiltCommonOptions options) {
    // Even though most of the requests are dispatched to the analytics service,
    // these are management operations so use the manager timeout.
    return options.timeout().orElse(core.context().environment().timeoutConfig().managementTimeout());
  }

  private RetryStrategy retryStrategy(CommonOptions<?>.BuiltCommonOptions options) {
    return options.retryStrategy().orElse(core.context().environment().retryStrategy());
  }

  private CompletableFuture<AnalyticsResult> exec(String statement, CommonOptions<?>.BuiltCommonOptions options) {
    return cluster.analyticsQuery(statement, toAnalyticsOptions(options))
        .exceptionally(t -> {
          throw translateException(t);
        });
  }

  private static AnalyticsOptions toAnalyticsOptions(CommonOptions<?>.BuiltCommonOptions options) {
    AnalyticsOptions result = analyticsOptions();
    options.timeout().ifPresent(result::timeout);
    options.retryStrategy().ifPresent(result::retryStrategy);
    return result;
  }

  private static final Map<Integer, Function<AnalyticsException, ? extends AnalyticsException>> errorMap = new HashMap<>();

  private RuntimeException translateException(Throwable t) {
    final HttpStatusCodeException httpException = findCause(t, HttpStatusCodeException.class).orElse(null);
    if (httpException != null && httpException.code() == HttpResponseStatus.NOT_FOUND.code()) {
      return new FeatureNotAvailableException(t);
    }

    if (t instanceof AnalyticsException) {
      final AnalyticsException e = ((AnalyticsException) t);
      for (Integer code : errorMap.keySet()) {
        if (e.hasErrorCode(code)) {
          return errorMap.get(code).apply(e);
        }
      }
    }
    return (t instanceof RuntimeException) ? (RuntimeException) t : new RuntimeException(t);
  }

  private static String quote(String s) {
    if (s.contains("`")) {
      throw InvalidArgumentException.fromMessage("Value [" + redactMeta(s) + "] may not contain backticks.");
    }
    return "`" + s + "`";
  }

  private static String quote(String... components) {
    return Arrays.stream(components)
        .map(AsyncAnalyticsIndexManager::quote)
        .collect(Collectors.joining("."));
  }
}
