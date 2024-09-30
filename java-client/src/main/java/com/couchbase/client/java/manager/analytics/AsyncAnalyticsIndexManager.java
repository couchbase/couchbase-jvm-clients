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
import com.couchbase.client.core.error.AuthenticationFailureException;
import com.couchbase.client.core.error.CompilationFailureException;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.DatasetExistsException;
import com.couchbase.client.core.error.DatasetNotFoundException;
import com.couchbase.client.core.error.DataverseExistsException;
import com.couchbase.client.core.error.DataverseNotFoundException;
import com.couchbase.client.core.error.IndexExistsException;
import com.couchbase.client.core.error.IndexNotFoundException;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.error.LinkExistsException;
import com.couchbase.client.core.error.LinkNotFoundException;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.manager.CoreAnalyticsLinkManager;
import com.couchbase.client.core.msg.RequestTarget;
import com.couchbase.client.java.AsyncCluster;
import com.couchbase.client.java.CommonOptions;
import com.couchbase.client.java.analytics.AnalyticsOptions;
import com.couchbase.client.java.analytics.AnalyticsResult;
import com.couchbase.client.java.manager.analytics.link.AnalyticsLink;
import com.couchbase.client.java.manager.analytics.link.AnalyticsLinkType;
import com.couchbase.client.java.manager.analytics.link.CouchbaseRemoteAnalyticsLink;
import com.couchbase.client.java.manager.analytics.link.S3ExternalAnalyticsLink;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.couchbase.client.core.endpoint.http.CoreHttpPath.path;
import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;
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

/**
 * Performs management operations on analytics indexes.
 */
public class AsyncAnalyticsIndexManager {

  private final AsyncCluster cluster;
  private final Core core;
  private final CoreHttpClient httpClient;
  private final CoreAnalyticsLinkManager linkManager;

  private static final String DEFAULT_DATAVERSE = "Default";
  private static final String DEFAULT_LINK = "Local";

  /**
   * Creates a new {@link AsyncAnalyticsIndexManager}.
   * <p>
   * This API is not intended to be called by the user directly, use {@link AsyncCluster#analyticsIndexes()}
   * instead.
   *
   * @param cluster the async cluster to perform the analytics queries on.
   */
  @Stability.Internal
  public AsyncAnalyticsIndexManager(final AsyncCluster cluster) {
    this.cluster = requireNonNull(cluster);
    this.core = cluster.core();
    this.httpClient = core.httpClient(RequestTarget.analytics());
    this.linkManager = new CoreAnalyticsLinkManager(core);
  }

  /**
   * Creates a new dataverse (analytics scope) if it does not already exist.
   *
   * @param dataverseName the name of the dataverse to create.
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws DataverseExistsException (async) if the dataverse already exists.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> createDataverse(final String dataverseName) {
    return createDataverse(dataverseName, createDataverseAnalyticsOptions());
  }

  /**
   * Creates a new dataverse (analytics scope) if it does not already exist with custom options.
   *
   * @param dataverseName the name of the dataverse to create.
   * @param options the custom options to apply.
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws DataverseExistsException (async) if the dataverse already exists.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> createDataverse(final String dataverseName,
                                                 final CreateDataverseAnalyticsOptions options) {
    notNullOrEmpty(dataverseName, "DataverseName");
    notNull(options, "Options");

    final CreateDataverseAnalyticsOptions.Built builtOpts = options.build();

    String statement = "CREATE DATAVERSE " + quoteDataverse(dataverseName);
    if (builtOpts.ignoreIfExists()) {
      statement += " IF NOT EXISTS";
    }

    return exec(statement, builtOpts, TracingIdentifiers.SPAN_REQUEST_MA_CREATE_DATAVERSE)
        .thenApply(result -> null);
  }

  /**
   * Fetches all dataverses (analytics scopes) from the analytics service.
   *
   * @return a {@link CompletableFuture} completing with a (potentially empty) list of dataverses or failed with an error.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  @Stability.Uncommitted
  public CompletableFuture<List<AnalyticsDataverse>> getAllDataverses() {
    return getAllDataverses(getAllDataversesAnalyticsOptions());
  }

  /**
   * Fetches all dataverses (analytics scopes) from the analytics service with custom options.
   *
   * @param options the custom options to apply.
   * @return a {@link CompletableFuture} completing with a (potentially empty) list of dataverses or failed with an error.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  @Stability.Uncommitted
  public CompletableFuture<List<AnalyticsDataverse>> getAllDataverses(final GetAllDataversesAnalyticsOptions options) {
    notNull(options, "Options");

    final GetAllDataversesAnalyticsOptions.Built builtOpts = options.build();

    String statement = "SELECT DataverseName from Metadata.`Dataverse`";
    return exec(statement, builtOpts, TracingIdentifiers.SPAN_REQUEST_MA_GET_ALL_DATAVERSES)
      .thenApply(result -> result.rowsAsObject().stream()
        .map((dv) -> dv.put("DataverseName",((String)dv.get("DataverseName")).replace("@.",".")))
        .map(AnalyticsDataverse::new)
        .collect(toList())
      );
  }

  /**
   * Drops (deletes) a dataverse.
   *
   * @param dataverseName the name of the dataverse to drop.
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws DataverseNotFoundException (async) if the dataverse does not exist.
   * @throws CompilationFailureException (async) if a dataverse that cannot be dropped (i.e. Default) is attempted.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> dropDataverse(final String dataverseName) {
    return dropDataverse(dataverseName, dropDataverseAnalyticsOptions());
  }

  /**
   * Drops (deletes) a dataverse with custom options.
   *
   * @param dataverseName the name of the dataverse to drop.
   * @param options the custom options to apply.
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws DataverseNotFoundException (async) if the dataverse does not exist.
   * @throws CompilationFailureException (async) if a dataverse that cannot be dropped (i.e. Default) is attempted.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> dropDataverse(final String dataverseName,
                                               final DropDataverseAnalyticsOptions options) {
    notNullOrEmpty(dataverseName, "DataverseName");
    notNull(options, "Options");

    final DropDataverseAnalyticsOptions.Built builtOpts = options.build();

    String statement = "DROP DATAVERSE " + quoteDataverse(dataverseName);
    if (builtOpts.ignoreIfNotExists()) {
      statement += " IF EXISTS";
    }

    return exec(statement, builtOpts, TracingIdentifiers.SPAN_REQUEST_MA_DROP_DATAVERSE)
        .thenApply(result -> null);
  }

  /**
   * Creates a new dataset (analytics collection) if it does not already exist.
   *
   * @param datasetName the name of the dataset to create.
   * @param bucketName the name of the bucket where the dataset is stored inside.
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws DatasetExistsException (async) if the dataset already exists.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> createDataset(final String datasetName, final String bucketName) {
    return createDataset(datasetName, bucketName, createDatasetAnalyticsOptions());
  }

  /**
   * Creates a new dataset (analytics collection) if it does not already exist with custom options.
   *
   * @param datasetName the name of the dataset to create.
   * @param bucketName the name of the bucket where the dataset is stored inside.
   * @param options the custom options to apply.
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws DatasetExistsException (async) if the dataset already exists.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> createDataset(final String datasetName, final String bucketName,
                                               final CreateDatasetAnalyticsOptions options) {
    notNullOrEmpty(datasetName, "DatasetName");
    notNullOrEmpty(bucketName, "BucketName");
    notNull(options, "Options");

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

  /**
   * Drops (deletes) a dataset.
   *
   * @param datasetName the name of the dataset to create.
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws DatasetNotFoundException (async) if the dataset to drop does not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> dropDataset(final String datasetName) {
    return dropDataset(datasetName, dropDatasetAnalyticsOptions());
  }

  /**
   * Drops (deletes) a dataset with custom options.
   *
   * @param datasetName the name of the dataset to create.
   * @param options the custom options to apply.
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws DatasetNotFoundException (async) if the dataset to drop does not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> dropDataset(final String datasetName, final DropDatasetAnalyticsOptions options) {
    notNullOrEmpty(datasetName, "DatasetName");
    notNull(options, "Options");

    final DropDatasetAnalyticsOptions.Built builtOpts = options.build();
    final String dataverseName = builtOpts.dataverseName().orElse(DEFAULT_DATAVERSE);

    String statement = "DROP DATASET " + quoteDataverse(dataverseName, datasetName);
    if (builtOpts.ignoreIfNotExists()) {
      statement += " IF EXISTS";
    }

    return exec(statement, builtOpts, TracingIdentifiers.SPAN_REQUEST_MA_DROP_DATASET)
        .thenApply(result -> null);
  }

  /**
   * Fetches all datasets (analytics collections) from the analytics service.
   *
   * @return a {@link CompletableFuture} completing with a (potentially empty) list of datasets or failed with an error.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<List<AnalyticsDataset>> getAllDatasets() {
    return getAllDatasets(getAllDatasetsAnalyticsOptions());
  }

  /**
   * Fetches all datasets (analytics collections) from the analytics service with custom options.
   *
   * @param options the custom options to apply.
   * @return a {@link CompletableFuture} completing with a (potentially empty) list of datasets or failed with an error.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<List<AnalyticsDataset>> getAllDatasets(final GetAllDatasetsAnalyticsOptions options) {
    notNull(options, "Options");
    final GetAllDatasetsAnalyticsOptions.Built builtOpts = options.build();
    String statement = "SELECT d.* FROM Metadata.`Dataset` d WHERE d.DataverseName <> \"Metadata\"";

    return exec(statement, builtOpts, TracingIdentifiers.SPAN_REQUEST_MA_GET_ALL_DATASETS)
        .thenApply(result -> result.rowsAsObject().stream()
            .map(AnalyticsDataset::new)
            .collect(toList())
        );
  }

  /**
   * Creates a new analytics index if it does not exist.
   *
   * @param indexName the name of the index to create.
   * @param datasetName the name of the dataset in which the index should be created.
   * @param fields the fields that should be indexed.
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws IndexExistsException (async) if the index already exists and not ignored in the options.
   * @throws DataverseNotFoundException (async) if a dataverse is provided in the options that does not exist.
   * @throws DatasetNotFoundException (async) if a dataset is provided which does not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> createIndex(final String indexName, final String datasetName,
                                             final Map<String, AnalyticsDataType> fields) {
    return createIndex(indexName, datasetName, fields, createIndexAnalyticsOptions());
  }

  /**
   * Creates a new analytics index if it does not exist with custom options.
   *
   * @param indexName the name of the index to create.
   * @param datasetName the name of the dataset in which the index should be created.
   * @param fields the fields that should be indexed.
   * @param options the custom options to apply.
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws IndexExistsException (async) if the index already exists and not ignored in the options.
   * @throws DataverseNotFoundException (async) if a dataverse is provided in the options that does not exist.
   * @throws DatasetNotFoundException (async) if a dataset is provided which does not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> createIndex(final String indexName, final String datasetName,
                                             final Map<String, AnalyticsDataType> fields,
                                             final CreateIndexAnalyticsOptions options) {
    notNullOrEmpty(indexName, "IndexName");
    notNullOrEmpty(datasetName, "DatasetName");
    notNull(fields, "Fields");
    notNull(options, "Options");

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

  /**
   * Lists all analytics indexes.
   *
   * @return a {@link CompletableFuture} completing with a list of indexes or failed with an error.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<List<AnalyticsIndex>> getAllIndexes() {
    return getAllIndexes(getAllIndexesAnalyticsOptions());
  }

  /**
   * Lists all analytics indexes with custom options.
   *
   * @param options the custom options to apply.
   * @return a {@link CompletableFuture} completing with a (potentially empty) list of indexes or failed with an error.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<List<AnalyticsIndex>> getAllIndexes(final GetAllIndexesAnalyticsOptions options) {
    notNull(options, "Options");
    final GetAllIndexesAnalyticsOptions.Built builtOpts = options.build();
    String statement = "SELECT d.* FROM Metadata.`Index` d WHERE d.DataverseName <> \"Metadata\"";

    return exec(statement, builtOpts, TracingIdentifiers.SPAN_REQUEST_MA_GET_ALL_INDEXES)
        .thenApply(result -> result.rowsAsObject().stream()
            .map(AnalyticsIndex::new)
            .collect(toList())
        );
  }

  /**
   * Drops (removes) an index if it exists.
   *
   * @param indexName the name of the index to drop.
   * @param datasetName the dataset in which the index exists.
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws IndexNotFoundException (async) if the index does not exist and not ignored via options.
   * @throws DataverseNotFoundException (async) if a dataverse is provided in the options that does not exist.
   * @throws DatasetNotFoundException (async) if a dataset is provided which does not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> dropIndex(final String indexName, final String datasetName) {
    return dropIndex(indexName, datasetName, dropIndexAnalyticsOptions());
  }

  /**
   * Drops (removes) an index if it exists with custom options.
   *
   * @param indexName the name of the index to drop.
   * @param datasetName the dataset in which the index exists.
   * @param options the custom options to apply.
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws IndexNotFoundException (async) if the index does not exist and not ignored via options.
   * @throws DataverseNotFoundException (async) if a dataverse is provided in the options that does not exist.
   * @throws DatasetNotFoundException (async) if a dataset is provided which does not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> dropIndex(final String indexName, final String datasetName,
                                           final DropIndexAnalyticsOptions options) {
    notNullOrEmpty(indexName, "IndexName");
    notNullOrEmpty(datasetName, "DatasetName");
    notNull(options, "Options");
    final DropIndexAnalyticsOptions.Built builtOpts = options.build();
    final String dataverseName = builtOpts.dataverseName().orElse(DEFAULT_DATAVERSE);

    String statement = "DROP INDEX " + quoteDataverse(dataverseName, datasetName, indexName);
    if (builtOpts.ignoreIfNotExists()) {
      statement += " IF EXISTS";
    }

    return exec(statement, builtOpts, TracingIdentifiers.SPAN_REQUEST_MA_DROP_INDEX)
        .thenApply(result -> null);
  }

  /**
   * Returns the pending mutations for different dataverses.
   *
   * @return a {@link CompletableFuture} completing with the pending mutations or failed with an error.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Map<String, Map<String, Long>>> getPendingMutations() {
    return getPendingMutations(getPendingMutationsAnalyticsOptions());
  }

  /**
   * Returns the pending mutations for different dataverses with custom options.
   *
   * @param options the custom options to apply.
   * @return a {@link CompletableFuture} completing with the pending mutations or failed with an error.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Map<String, Map<String, Long>>> getPendingMutations(
    final GetPendingMutationsAnalyticsOptions options) {
    notNull(options, "Options");
    return httpClient.get(path("/analytics/node/agg/stats/remaining"), options.build())
      .trace(TracingIdentifiers.SPAN_REQUEST_MA_GET_PENDING_MUTATIONS)
      .exec(core)
      .thenApply(response ->
        Mapper.decodeInto(response.content(), new TypeReference<Map<String, Map<String, Long>> >() {})
      );
  }

  /**
   * Connects the analytics link for the default dataverse (Default.Local).
   *
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> connectLink() {
    return connectLink(connectLinkAnalyticsOptions());
  }

  /**
   * Connects the analytics link for the default dataverse with custom options.
   *
   * @param options the custom options to apply.
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws LinkNotFoundException (async) if the link does not exist.
   * @throws DataverseNotFoundException (async) if a dataverse is provided in the options that does not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> connectLink(final ConnectLinkAnalyticsOptions options) {
    notNull(options, "Options");
    final ConnectLinkAnalyticsOptions.Built builtOpts = options.build();
    String statement = "CONNECT LINK " + quoteDataverse(
        builtOpts.dataverseName().orElse(DEFAULT_DATAVERSE),
        builtOpts.linkName().orElse(DEFAULT_LINK)
    );

    if (builtOpts.force()) {
      statement += " WITH " + Mapper.encodeAsString(singletonMap("force", true));
    }

    return exec(statement, builtOpts, TracingIdentifiers.SPAN_REQUEST_MA_CONNECT_LINK).thenApply(result -> null);
  }

  /**
   * Disconnects the analytics link for the default dataverse (Default.Local).
   *
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> disconnectLink() {
    return disconnectLink(disconnectLinkAnalyticsOptions());
  }

  /**
   * Disconnects the analytics link for the default dataverse with custom options.
   *
   * @param options the custom options to apply.
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws LinkNotFoundException (async) if the link does not exist.
   * @throws DataverseNotFoundException (async) if a dataverse is provided in the options that does not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> disconnectLink(final DisconnectLinkAnalyticsOptions options) {
    notNull(options, "Options");
    final DisconnectLinkAnalyticsOptions.Built builtOpts = options.build();
    String statement = "DISCONNECT LINK " + quoteDataverse(
        builtOpts.dataverseName().orElse(DEFAULT_DATAVERSE),
        builtOpts.linkName().orElse(DEFAULT_LINK)
    );

    return exec(statement, builtOpts, TracingIdentifiers.SPAN_REQUEST_MA_DISCONNECT_LINK).thenApply(result -> null);
  }

  /**
   * Creates a new analytics link.
   *
   * @param link the name of the link that should be created.
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws InvalidArgumentException (async) if required parameters are not supplied or are invalid.
   * @throws AuthenticationFailureException (async) if the remote link cannot be authenticated on creation.
   * @throws LinkExistsException (async) if the link with the name already exists.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> createLink(final AnalyticsLink link) {
    return createLink(link, createLinkAnalyticsOptions());
  }

  /**
   * Creates a new analytics link with custom options.
   *
   * @param link the name of the link that should be created.
   * @param options the custom options to apply.
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws InvalidArgumentException (async) if required parameters are not supplied or are invalid.
   * @throws AuthenticationFailureException (async) if the remote link cannot be authenticated on creation.
   * @throws LinkExistsException (async) if the link with the name already exists.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> createLink(final AnalyticsLink link, final CreateLinkAnalyticsOptions options) {
    return linkManager.createLink(link.toMap(), options.build());
  }

  /**
   * Replaces an existing analytics link.
   *
   * @param link the name of the link that should be replaced.
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws InvalidArgumentException (async) if required parameters are not supplied or are invalid.
   * @throws AuthenticationFailureException (async) if the remote link cannot be authenticated on replace.
   * @throws LinkNotFoundException (async) if the link does not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> replaceLink(final AnalyticsLink link) {
    return replaceLink(link, replaceLinkAnalyticsOptions());
  }

  /**
   * Replaces an analytics link with custom options.
   *
   * @param link the name of the link that should be replaced.
   * @param options the custom options to apply.
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws InvalidArgumentException (async) if required parameters are not supplied or are invalid.
   * @throws AuthenticationFailureException (async) if the remote link cannot be authenticated on replace.
   * @throws LinkNotFoundException (async) if the link does not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> replaceLink(final AnalyticsLink link, final ReplaceLinkAnalyticsOptions options) {
    return linkManager.replaceLink(link.toMap(), options.build());
  }

  /**
   * Drops (removes) a link if it exists.
   *
   * @param linkName the name of the link that should be dropped.
   * @param dataverse the name of the dataverse in which the link exists.
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws LinkNotFoundException (async) if the link does not exist.
   * @throws DataverseNotFoundException (async) if a dataverse is provided that does not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> dropLink(final String linkName, final String dataverse) {
    return dropLink(linkName, dataverse, dropLinkAnalyticsOptions());
  }

  /**
   * Drops (removes) a link if it exists with custom options.
   *
   * @param linkName the name of the link that should be dropped.
   * @param dataverse the name of the dataverse in which the link exists.
   * @param options the custom options to apply.
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws LinkNotFoundException (async) if the link does not exist.
   * @throws DataverseNotFoundException (async) if a dataverse is provided that does not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> dropLink(final String linkName, final String dataverse,
                                          final DropLinkAnalyticsOptions options) {
    notNullOrEmpty(linkName, "LinkName");
    notNullOrEmpty(dataverse, "Dataverse");
    notNull(options, "Options");
    return linkManager.dropLink(linkName, dataverse, options.build());
  }

  /**
   * Returns a (potentially empty) list of current analytics links.
   * <p>
   * Links describe connections between an external data source and the analytics engine. Note that
   * {@link AnalyticsLink} is an abstract class and has implementations depending on the type of link configured. See
   * and cast into {@link S3ExternalAnalyticsLink}, or {@link CouchbaseRemoteAnalyticsLink} for specific attributes. In
   * the future, more external link types might be supported - please consult the server documentation for more
   * information.
   *
   * @return a {@link CompletableFuture} completing with a (potentially empty) list of links or failed with an error.
   * @throws DataverseNotFoundException (async) if a dataverse is provided in the options that does not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<List<AnalyticsLink>> getLinks() {
    return getLinks(getLinksAnalyticsOptions());
  }

  /**
   * Returns a (potentially empty) list of current analytics links with custom options.
   * <p>
   * Links describe connections between an external data source and the analytics engine. Note that
   * {@link AnalyticsLink} is an abstract class and has implementations depending on the type of link configured. See
   * and cast into {@link S3ExternalAnalyticsLink}, or {@link CouchbaseRemoteAnalyticsLink} for specific attributes. In
   * the future, more external link types might be supported - please consult the server documentation for more
   * information.
   *
   * @param options the custom options to apply.
   * @return a {@link CompletableFuture} completing with a (potentially empty) list of links or failed with an error.
   * @throws DataverseNotFoundException (async) if a dataverse is provided in the options that does not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<List<AnalyticsLink>> getLinks(final GetLinksAnalyticsOptions options) {
    notNull(options, "Options");
    GetLinksAnalyticsOptions.Built opts = options.build();

    String dataverseName = opts.dataverseName().orElse(null);
    String linkType = opts.linkType().map(AnalyticsLinkType::wireName).orElse(null);
    String linkName = opts.name().orElse(null);
    return linkManager
      .getLinks(dataverseName, linkType, linkName, opts)
      .thenApply(responseBytes -> Mapper.decodeInto(responseBytes, new TypeReference<List<AnalyticsLink>>() {}));
  }

  /**
   * Formats a map of index fields and their datatype into the representation analytics needs on the wire.
   *
   * @param fields the fields to convert.
   * @return the converted and stringified fields, ready to be included in a statement.
   */
  static String formatIndexFields(final Map<String, AnalyticsDataType> fields) {
    List<String> result = new ArrayList<>();
    fields.forEach((k, v) -> result.add(quote(k) + ":" + v.value()));
    return "(" + String.join(",", result) + ")";
  }

  /**
   * Executes a statement with options against analytics.
   *
   * @param statement the statement of the query.
   * @param options the options that should be passed along.
   * @param spanName the name of the span as the outer parent.
   * @return a future eventually containing the analytics result once complete, or a failure.
   */
  private CompletableFuture<AnalyticsResult> exec(final String statement,
                                                  final CommonOptions<?>.BuiltCommonOptions options,
                                                  final String spanName) {
    RequestSpan parent = core.coreResources().requestTracer().requestSpan(spanName, options.parentSpan().orElse(null));

    final AnalyticsOptions analyticsOptions = toAnalyticsOptions(options).parentSpan(parent);

    return cluster
      .analyticsQuery(statement, analyticsOptions)
      .whenComplete((r, t) -> parent.end());
  }

  /**
   * Converts the common options into a single {@link AnalyticsOptions} so it can be passed down to the query.
   *
   * @param options the options to convert.
   * @return the converted {@link AnalyticsOptions}.
   */
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
