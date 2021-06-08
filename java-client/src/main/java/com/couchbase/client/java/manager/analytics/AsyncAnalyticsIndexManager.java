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
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.CbTracing;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.type.TypeReference;
import com.couchbase.client.core.endpoint.http.CoreHttpClient;
import com.couchbase.client.core.error.DataverseExistsException;
import com.couchbase.client.core.error.DataverseNotFoundException;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.manager.CoreAnalyticsLinkManager;
import com.couchbase.client.core.msg.RequestTarget;
import com.couchbase.client.java.AsyncCluster;
import com.couchbase.client.java.CommonOptions;
import com.couchbase.client.java.analytics.AnalyticsOptions;
import com.couchbase.client.java.analytics.AnalyticsResult;
import com.couchbase.client.java.manager.analytics.link.AnalyticsLink;
import com.couchbase.client.java.manager.analytics.link.AnalyticsLinkType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.couchbase.client.core.endpoint.http.CoreHttpPath.path;
import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static com.couchbase.client.java.analytics.AnalyticsOptions.analyticsOptions;
import static com.couchbase.client.java.manager.analytics.ConnectLinkAnalyticsOptions.connectLinkAnalyticsOptions;
import static com.couchbase.client.java.manager.analytics.CreateDatasetAnalyticsOptions.createDatasetAnalyticsOptions;
import static com.couchbase.client.java.manager.analytics.CreateDataverseAnalyticsOptions.createDataverseAnalyticsOptions;
import static com.couchbase.client.java.manager.analytics.CreateIndexAnalyticsOptions.createIndexAnalyticsOptions;
import static com.couchbase.client.java.manager.analytics.CreateLinkAnalyticsOptions.createLinkAnalyticsOptions;
import static com.couchbase.client.java.manager.analytics.DisconnectLinkAnalyticsOptions.disconnectLinkAnalyticsOptions;
import static com.couchbase.client.java.manager.analytics.DropDatasetAnalyticsOptions.dropDatasetAnalyticsOptions;
import static com.couchbase.client.java.manager.analytics.DropDataverseAnalyticsOptions.dropDataverseAnalyticsOptions;
import static com.couchbase.client.java.manager.analytics.DropIndexAnalyticsOptions.dropIndexAnalyticsOptions;
import static com.couchbase.client.java.manager.analytics.DropLinkAnalyticsOptions.dropLinkAnalyticsOptions;
import static com.couchbase.client.java.manager.analytics.GetAllDatasetsAnalyticsOptions.getAllDatasetsAnalyticsOptions;
import static com.couchbase.client.java.manager.analytics.GetAllDataversesAnalyticsOptions.getAllDataversesAnalyticsOptions;
import static com.couchbase.client.java.manager.analytics.GetAllIndexesAnalyticsOptions.getAllIndexesAnalyticsOptions;
import static com.couchbase.client.java.manager.analytics.GetLinksAnalyticsOptions.getLinksAnalyticsOptions;
import static com.couchbase.client.java.manager.analytics.GetPendingMutationsAnalyticsOptions.getPendingMutationsAnalyticsOptions;
import static com.couchbase.client.java.manager.analytics.ReplaceLinkAnalyticsOptions.replaceLinkAnalyticsOptions;
import static java.util.Collections.singletonMap;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class AsyncAnalyticsIndexManager {

  private final AsyncCluster cluster;
  private final Core core;
  private final CoreHttpClient httpClient;
  private final CoreAnalyticsLinkManager linkManager;

  private static final String DEFAULT_DATAVERSE = "Default";
  private static final String DEFAULT_LINK = "Local";

  public AsyncAnalyticsIndexManager(AsyncCluster cluster) {
    this.cluster = requireNonNull(cluster);
    this.core = cluster.core();
    this.httpClient = core.httpClient(RequestTarget.analytics());
    this.linkManager = new CoreAnalyticsLinkManager(core);
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

    String statement = "CREATE DATAVERSE " + quoteDataverse(dataverseName);
    if (builtOpts.ignoreIfExists()) {
      statement += " IF NOT EXISTS";
    }

    return exec(statement, builtOpts, TracingIdentifiers.SPAN_REQUEST_MA_CREATE_DATAVERSE)
        .thenApply(result -> null);
  }

  @Stability.Uncommitted
  public CompletableFuture<List<AnalyticsDataverse>> getAllDataverses() {
    return getAllDataverses(getAllDataversesAnalyticsOptions());
  }

  @Stability.Uncommitted
  public CompletableFuture<List<AnalyticsDataverse>> getAllDataverses(final GetAllDataversesAnalyticsOptions options) {
    final GetAllDataversesAnalyticsOptions.Built builtOpts = options.build();

    String statement = "SELECT DataverseName from Metadata.`Dataverse`";
    return exec(statement, builtOpts, TracingIdentifiers.SPAN_REQUEST_MA_GET_ALL_DATAVERSES)
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

    String statement = "DROP DATAVERSE " + quoteDataverse(dataverseName);
    if (builtOpts.ignoreIfNotExists()) {
      statement += " IF EXISTS";
    }

    return exec(statement, builtOpts, TracingIdentifiers.SPAN_REQUEST_MA_DROP_DATAVERSE)
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

    statement += quoteDataverse(dataverseName, datasetName) + " ON " + quote(bucketName);

    if (condition != null) {
      statement += " WHERE " + condition;
    }

    return exec(statement, builtOpts, TracingIdentifiers.SPAN_REQUEST_MA_CREATE_DATASET)
        .thenApply(result -> null);
  }

  public CompletableFuture<Void> dropDataset(String datasetName) {
    return dropDataset(datasetName, dropDatasetAnalyticsOptions());
  }

  public CompletableFuture<Void> dropDataset(String datasetName, DropDatasetAnalyticsOptions options) {
    requireNonNull(datasetName);

    final DropDatasetAnalyticsOptions.Built builtOpts = options.build();
    final String dataverseName = builtOpts.dataverseName().orElse(DEFAULT_DATAVERSE);

    String statement = "DROP DATASET " + quoteDataverse(dataverseName, datasetName);
    if (builtOpts.ignoreIfNotExists()) {
      statement += " IF EXISTS";
    }

    return exec(statement, builtOpts, TracingIdentifiers.SPAN_REQUEST_MA_DROP_DATASET)
        .thenApply(result -> null);
  }

  public CompletableFuture<List<AnalyticsDataset>> getAllDatasets() {
    return getAllDatasets(getAllDatasetsAnalyticsOptions());
  }

  public CompletableFuture<List<AnalyticsDataset>> getAllDatasets(GetAllDatasetsAnalyticsOptions options) {
    final GetAllDatasetsAnalyticsOptions.Built builtOpts = options.build();
    String statement = "SELECT d.* FROM Metadata.`Dataset` d WHERE d.DataverseName <> \"Metadata\"";

    return exec(statement, builtOpts, TracingIdentifiers.SPAN_REQUEST_MA_GET_ALL_DATASETS)
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

    statement += " ON " + quoteDataverse(dataverseName, datasetName) + " " + formatIndexFields(fields);

    return exec(statement, builtOpts, TracingIdentifiers.SPAN_REQUEST_MA_CREATE_INDEX)
        .thenApply(result -> null);
  }

  public CompletableFuture<List<AnalyticsIndex>> getAllIndexes() {
    return getAllIndexes(getAllIndexesAnalyticsOptions());
  }

  public CompletableFuture<List<AnalyticsIndex>> getAllIndexes(GetAllIndexesAnalyticsOptions options) {
    final GetAllIndexesAnalyticsOptions.Built builtOpts = options.build();
    String statement = "SELECT d.* FROM Metadata.`Index` d WHERE d.DataverseName <> \"Metadata\"";

    return exec(statement, builtOpts, TracingIdentifiers.SPAN_REQUEST_MA_GET_ALL_INDEXES)
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

    String statement = "DROP INDEX " + quoteDataverse(dataverseName, datasetName, indexName);
    if (builtOpts.ignoreIfNotExists()) {
      statement += " IF EXISTS";
    }

    return exec(statement, builtOpts, TracingIdentifiers.SPAN_REQUEST_MA_DROP_INDEX)
        .thenApply(result -> null);
  }

  public CompletableFuture<Void> connectLink() {
    return connectLink(connectLinkAnalyticsOptions());
  }

  public CompletableFuture<Void> connectLink(ConnectLinkAnalyticsOptions options) {
    final ConnectLinkAnalyticsOptions.Built builtOpts = options.build();
    String statement = "CONNECT LINK " + quoteDataverse(
        builtOpts.dataverseName().orElse(DEFAULT_DATAVERSE),
        builtOpts.linkName().orElse(DEFAULT_LINK));

    if (builtOpts.force()) {
      statement += " WITH " + Mapper.encodeAsString(singletonMap("force", true));
    }

    return exec(statement, builtOpts, TracingIdentifiers.SPAN_REQUEST_MA_CONNECT_LINK)
        .thenApply(result -> null);
  }

  public CompletableFuture<Void> disconnectLink() {
    return disconnectLink(disconnectLinkAnalyticsOptions());
  }

  public CompletableFuture<Void> disconnectLink(DisconnectLinkAnalyticsOptions options) {
    final DisconnectLinkAnalyticsOptions.Built builtOpts = options.build();
    String statement = "DISCONNECT LINK " + quoteDataverse(
        builtOpts.dataverseName().orElse(DEFAULT_DATAVERSE),
        builtOpts.linkName().orElse(DEFAULT_LINK));

    return exec(statement, builtOpts, TracingIdentifiers.SPAN_REQUEST_MA_DISCONNECT_LINK)
        .thenApply(result -> null);
  }

  public CompletableFuture<Map<String, Map<String, Long>>> getPendingMutations() {
    return getPendingMutations(getPendingMutationsAnalyticsOptions());
  }

  public CompletableFuture<Map<String, Map<String, Long>>> getPendingMutations(final GetPendingMutationsAnalyticsOptions options) {
    return httpClient.get(path("/analytics/node/agg/stats/remaining"), options.build())
        .trace(TracingIdentifiers.SPAN_REQUEST_MA_GET_PENDING_MUTATIONS)
        .exec(core)
        .thenApply(response ->
          Mapper.decodeInto(response.content(), new TypeReference<Map<String, Map<String, Long>> >() {
        }));
  }

  public CompletableFuture<Void> createLink(AnalyticsLink link) {
    return createLink(link, createLinkAnalyticsOptions());
  }

  public CompletableFuture<Void> createLink(AnalyticsLink link, CreateLinkAnalyticsOptions options) {
    return linkManager.createLink(link.toMap(), options.build());
  }

  public CompletableFuture<Void> replaceLink(AnalyticsLink link) {
    return replaceLink(link, replaceLinkAnalyticsOptions());
  }

  public CompletableFuture<Void> replaceLink(AnalyticsLink link, ReplaceLinkAnalyticsOptions options) {
    return linkManager.replaceLink(link.toMap(), options.build());
  }

  public CompletableFuture<Void> dropLink(String linkName, String dataverse) {
    return dropLink(linkName, dataverse, dropLinkAnalyticsOptions());
  }

  public CompletableFuture<Void> dropLink(String linkName, String dataverse, DropLinkAnalyticsOptions options) {
    return linkManager.dropLink(linkName, dataverse, options.build());
  }

  public CompletableFuture<List<AnalyticsLink>> getLinks() {
    return getLinks(getLinksAnalyticsOptions());
  }

  public CompletableFuture<List<AnalyticsLink>> getLinks(GetLinksAnalyticsOptions options) {
    GetLinksAnalyticsOptions.Built opts = options.build();
    String dataverseName = opts.dataverseName().orElse(null);
    String linkType = opts.linkType().map(AnalyticsLinkType::wireName).orElse(null);
    String linkName = opts.name().orElse(null);
    return linkManager.getLinks(dataverseName, linkType, linkName, opts)
        .thenApply(responseBytes ->
            Mapper.decodeInto(responseBytes, new TypeReference<List<AnalyticsLink>>() {
            }));
  }

  private CompletableFuture<AnalyticsResult> exec(String statement, CommonOptions<?>.BuiltCommonOptions options, String spanName) {
    RequestSpan parent = CbTracing.newSpan(cluster.environment().requestTracer(), spanName, options.parentSpan().orElse(null));
    final AnalyticsOptions analyticsOptions = toAnalyticsOptions(options)
        .parentSpan(parent);

    return cluster
      .analyticsQuery(statement, analyticsOptions)
      .whenComplete((r, t) -> parent.end());
  }

  private static AnalyticsOptions toAnalyticsOptions(final CommonOptions<?>.BuiltCommonOptions options) {
    AnalyticsOptions result = analyticsOptions();
    options.timeout().ifPresent(result::timeout);
    options.retryStrategy().ifPresent(result::retryStrategy);
    result.clientContext(options.clientContext());
    return result;
  }

  /**
   * Quotes an individual string/component.
   *
   * @param s the string to quote.
   * @return the quoted string.
   * @throws InvalidArgumentException if the string already contains backticks.
   */
  private static String quote(final String s) {
    if (s.contains("`")) {
      throw InvalidArgumentException.fromMessage("Value [" + redactMeta(s) + "] may not contain backticks.");
    }
    return "`" + s + "`";
  }

  /**
   * Quotes a list of components starting with a dataverse name,
   * and returns them .-separated as a string.
   *
   * @param dataverseName dataverse name; slashes are interpreted as component delimiters
   * @param otherComponents the components to be quoted.
   * @return the fully quoted string (from the individual components).
   * @throws InvalidArgumentException if an individual component already contains backticks.
   */
  static String quoteDataverse(String dataverseName, String... otherComponents) {
    List<String> components = new ArrayList<>();
    components.addAll(Arrays.asList(dataverseName.split("/", -1)));
    components.addAll(Arrays.asList(otherComponents));

    return components.stream()
        .map(AsyncAnalyticsIndexManager::quote)
        .collect(Collectors.joining("."));
  }
}
