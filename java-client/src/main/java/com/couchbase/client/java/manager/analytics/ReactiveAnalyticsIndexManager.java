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

import com.couchbase.client.core.annotation.Stability;
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
import com.couchbase.client.java.AsyncCluster;
import com.couchbase.client.java.ReactiveCluster;
import com.couchbase.client.java.manager.analytics.link.AnalyticsLink;
import com.couchbase.client.java.manager.analytics.link.CouchbaseRemoteAnalyticsLink;
import com.couchbase.client.java.manager.analytics.link.S3ExternalAnalyticsLink;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;

import static com.couchbase.client.core.Reactor.toFlux;
import static com.couchbase.client.core.Reactor.toMono;

/**
 * Performs management operations on analytics indexes.
 */
public class ReactiveAnalyticsIndexManager {

  private final AsyncAnalyticsIndexManager async;

  /**
   * Creates a new {@link ReactiveAnalyticsIndexManager}.
   * <p>
   * This API is not intended to be called by the user directly, use {@link ReactiveCluster#analyticsIndexes()}
   * instead.
   *
   * @param cluster the async cluster to perform the analytics queries on.
   */
  @Stability.Internal
  public ReactiveAnalyticsIndexManager(final AsyncCluster cluster) {
    this.async = new AsyncAnalyticsIndexManager(cluster);
  }

  /**
   * Returns the async version of this index manager.
   *
   * @return the async version of this index manager.
   */
  public AsyncAnalyticsIndexManager async() {
    return async;
  }

  /**
   * Creates a new dataverse (analytics scope) if it does not already exist.
   *
   * @param dataverseName the name of the dataverse to create.
   * @return a {@link Mono} completing when the operation is applied or failed with an error.
   * @throws DataverseExistsException (async) if the dataverse already exists.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> createDataverse(final String dataverseName) {
    return toMono(() -> async.createDataverse(dataverseName));
  }

  /**
   * Creates a new dataverse (analytics scope) if it does not already exist with custom options.
   *
   * @param dataverseName the name of the dataverse to create.
   * @param options the custom options to apply.
   * @return a {@link Mono} completing when the operation is applied or failed with an error.
   * @throws DataverseExistsException (async) if the dataverse already exists.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> createDataverse(String dataverseName, CreateDataverseAnalyticsOptions options) {
    return toMono(() -> async.createDataverse(dataverseName, options));
  }

  /**
   * Drops (deletes) a dataverse.
   *
   * @param dataverseName the name of the dataverse to drop.
   * @return a {@link Mono} completing when the operation is applied or failed with an error.
   * @throws DataverseNotFoundException (async) if the dataverse does not exist.
   * @throws CompilationFailureException (async) if a dataverse that cannot be dropped (i.e. Default) is attempted.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> dropDataverse(String dataverseName) {
    return toMono(() -> async.dropDataverse(dataverseName));
  }

  /**
   * Drops (deletes) a dataverse with custom options.
   *
   * @param dataverseName the name of the dataverse to drop.
   * @param options the custom options to apply.
   * @return a {@link Mono} completing when the operation is applied or failed with an error.
   * @throws DataverseNotFoundException (async) if the dataverse does not exist.
   * @throws CompilationFailureException (async) if a dataverse that cannot be dropped (i.e. Default) is attempted.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> dropDataverse(String dataverseName, DropDataverseAnalyticsOptions options) {
    return toMono(() -> async.dropDataverse(dataverseName, options));
  }

  /**
   * Fetches all dataverses (analytics scopes) from the analytics service.
   *
   * @return a {@link Flux} completing with a (potentially empty) list of dataverses or failed with an error.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  @Stability.Uncommitted
  public Flux<AnalyticsDataverse> getAllDataverses() {
    return toFlux(async::getAllDataverses);
  }

  /**
   * Fetches all dataverses (analytics scopes) from the analytics service with custom options.
   *
   * @param options the custom options to apply.
   * @return a {@link Flux} completing with a (potentially empty) list of dataverses or failed with an error.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  @Stability.Uncommitted
  public Flux<AnalyticsDataverse> getAllDataverses(GetAllDataversesAnalyticsOptions options) {
    return toFlux(() -> async.getAllDataverses(options));
  }

  /**
   * Creates a new dataset (analytics collection) if it does not already exist.
   *
   * @param datasetName the name of the dataset to create.
   * @param bucketName the name of the bucket where the dataset is stored inside.
   * @return a {@link Mono} completing when the operation is applied or failed with an error.
   * @throws DatasetExistsException (async) if the dataset already exists.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> createDataset(String datasetName, String bucketName) {
    return toMono(() -> async.createDataset(datasetName, bucketName));
  }

  /**
   * Creates a new dataset (analytics collection) if it does not already exist with custom options.
   *
   * @param datasetName the name of the dataset to create.
   * @param bucketName the name of the bucket where the dataset is stored inside.
   * @param options the custom options to apply.
   * @return a {@link Mono} completing when the operation is applied or failed with an error.
   * @throws DatasetExistsException (async) if the dataset already exists.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> createDataset(String datasetName, String bucketName, CreateDatasetAnalyticsOptions options) {
    return toMono(() -> async.createDataset(datasetName, bucketName, options));
  }

  /**
   * Drops (deletes) a dataset.
   *
   * @param datasetName the name of the dataset to create.
   * @return a {@link Mono} completing when the operation is applied or failed with an error.
   * @throws DatasetNotFoundException (async) if the dataset to drop does not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> dropDataset(String datasetName) {
    return toMono(() -> async.dropDataset(datasetName));
  }

  /**
   * Drops (deletes) a dataset with custom options.
   *
   * @param datasetName the name of the dataset to create.
   * @param options the custom options to apply.
   * @return a {@link Mono} completing when the operation is applied or failed with an error.
   * @throws DatasetNotFoundException (async) if the dataset to drop does not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> dropDataset(String datasetName, DropDatasetAnalyticsOptions options) {
    return toMono(() -> async.dropDataset(datasetName, options));
  }

  /**
   * Fetches all datasets (analytics collections) from the analytics service.
   *
   * @return a {@link Flux} completing with a (potentially empty) list of datasets or failed with an error.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Flux<AnalyticsDataset> getAllDatasets() {
    return toFlux(async::getAllDatasets);
  }

  /**
   * Fetches all datasets (analytics collections) from the analytics service with custom options.
   *
   * @param options the custom options to apply.
   * @return a {@link Flux} completing with a (potentially empty) list of datasets or failed with an error.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Flux<AnalyticsDataset> getAllDatasets(GetAllDatasetsAnalyticsOptions options) {
    return toFlux(() -> async.getAllDatasets(options));
  }

  /**
   * Creates a new analytics index if it does not exist.
   *
   * @param indexName the name of the index to create.
   * @param datasetName the name of the dataset in which the index should be created.
   * @param fields the fields that should be indexed.
   * @return a {@link Mono} completing when the operation is applied or failed with an error.
   * @throws IndexExistsException (async) if the index already exists and not ignored in the options.
   * @throws DataverseNotFoundException (async) if a dataverse is provided in the options that does not exist.
   * @throws DatasetNotFoundException (async) if a dataset is provided which does not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> createIndex(String indexName, String datasetName, Map<String, AnalyticsDataType> fields) {
    return toMono(() -> async.createIndex(indexName, datasetName, fields));
  }

  /**
   * Creates a new analytics index if it does not exist with custom options.
   *
   * @param indexName the name of the index to create.
   * @param datasetName the name of the dataset in which the index should be created.
   * @param fields the fields that should be indexed.
   * @param options the custom options to apply.
   * @return a {@link Mono} completing when the operation is applied or failed with an error.
   * @throws IndexExistsException (async) if the index already exists and not ignored in the options.
   * @throws DataverseNotFoundException (async) if a dataverse is provided in the options that does not exist.
   * @throws DatasetNotFoundException (async) if a dataset is provided which does not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> createIndex(String indexName, String datasetName, Map<String, AnalyticsDataType> fields, CreateIndexAnalyticsOptions options) {
    return toMono(() -> async.createIndex(indexName, datasetName, fields, options));
  }

  /**
   * Drops (removes) an index if it exists.
   *
   * @param indexName the name of the index to drop.
   * @param datasetName the dataset in which the index exists.
   * @return a {@link Mono} completing when the operation is applied or failed with an error.
   * @throws IndexNotFoundException (async) if the index does not exist and not ignored via options.
   * @throws DataverseNotFoundException (async) if a dataverse is provided in the options that does not exist.
   * @throws DatasetNotFoundException (async) if a dataset is provided which does not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> dropIndex(String indexName, String datasetName) {
    return toMono(() -> async.dropIndex(indexName, datasetName));
  }

  /**
   * Drops (removes) an index if it exists with custom options.
   *
   * @param indexName the name of the index to drop.
   * @param datasetName the dataset in which the index exists.
   * @param options the custom options to apply.
   * @return a {@link Mono} completing when the operation is applied or failed with an error.
   * @throws IndexNotFoundException (async) if the index does not exist and not ignored via options.
   * @throws DataverseNotFoundException (async) if a dataverse is provided in the options that does not exist.
   * @throws DatasetNotFoundException (async) if a dataset is provided which does not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> dropIndex(String indexName, String datasetName, DropIndexAnalyticsOptions options) {
    return toMono(() -> async.dropIndex(indexName, datasetName, options));
  }

  /**
   * Lists all analytics indexes.
   *
   * @return a {@link Flux} completing with a list of indexes or failed with an error.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Flux<AnalyticsIndex> getAllIndexes() {
    return toFlux(async::getAllIndexes);
  }

  /**
   * Lists all analytics indexes with custom options.
   *
   * @param options the custom options to apply.
   * @return a {@link Flux} completing with a (potentially empty) list of indexes or failed with an error.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Flux<AnalyticsIndex> getAllIndexes(GetAllIndexesAnalyticsOptions options) {
    return toFlux(() -> async.getAllIndexes(options));
  }

  /**
   * Connects the analytics link for the default dataverse (Default.Local).
   *
   * @return a {@link Mono} completing when the operation is applied or failed with an error.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> connectLink() {
    return toMono(async::connectLink);
  }

  /**
   * Connects the analytics link for the default dataverse with custom options.
   *
   * @param options the custom options to apply.
   * @return a {@link Mono} completing when the operation is applied or failed with an error.
   * @throws LinkNotFoundException (async) if the link does not exist.
   * @throws DataverseNotFoundException (async) if a dataverse is provided in the options that does not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> connectLink(ConnectLinkAnalyticsOptions options) {
    return toMono(() -> async.connectLink(options));
  }

  /**
   * Disconnects the analytics link for the default dataverse (Default.Local).
   *
   * @return a {@link Mono} completing when the operation is applied or failed with an error.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> disconnectLink() {
    return toMono(async::disconnectLink);
  }

  /**
   * Disconnects the analytics link for the default dataverse with custom options.
   *
   * @param options the custom options to apply.
   * @return a {@link Mono} completing when the operation is applied or failed with an error.
   * @throws LinkNotFoundException (async) if the link does not exist.
   * @throws DataverseNotFoundException (async) if a dataverse is provided in the options that does not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> disconnectLink(DisconnectLinkAnalyticsOptions options) {
    return toMono(() -> async.disconnectLink(options));
  }

  /**
   * Returns the pending mutations for different dataverses.
   *
   * @return a {@link Mono} completing with the pending mutations or failed with an error.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Mono<Map<String, Map<String, Long>>> getPendingMutations() {
    return toMono(async::getPendingMutations);
  }

  /**
   * Returns the pending mutations for different dataverses with custom options.
   *
   * @param options the custom options to apply.
   * @return a {@link Mono} completing with the pending mutations or failed with an error.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Mono<Map<String, Map<String, Long>>> getPendingMutations(final GetPendingMutationsAnalyticsOptions options) {
    return toMono(() -> async.getPendingMutations(options));
  }

  /**
   * Creates a new analytics link.
   *
   * @param link the name of the link that should be created.
   * @return a {@link Mono} completing when the operation is applied or failed with an error.
   * @throws InvalidArgumentException (async) if required parameters are not supplied or are invalid.
   * @throws AuthenticationFailureException (async) if the remote link cannot be authenticated on creation.
   * @throws LinkExistsException (async) if the link with the name already exists.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> createLink(AnalyticsLink link) {
    return toMono(() -> async.createLink(link));
  }

  /**
   * Creates a new analytics link with custom options.
   *
   * @param link the name of the link that should be created.
   * @param options the custom options to apply.
   * @return a {@link Mono} completing when the operation is applied or failed with an error.
   * @throws InvalidArgumentException (async) if required parameters are not supplied or are invalid.
   * @throws AuthenticationFailureException (async) if the remote link cannot be authenticated on creation.
   * @throws LinkExistsException (async) if the link with the name already exists.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> createLink(AnalyticsLink link, CreateLinkAnalyticsOptions options) {
    return toMono(() -> async.createLink(link, options));
  }

  /**
   * Replaces an existing analytics link.
   *
   * @param link the name of the link that should be replaced.
   * @return a {@link Mono} completing when the operation is applied or failed with an error.
   * @throws InvalidArgumentException (async) if required parameters are not supplied or are invalid.
   * @throws AuthenticationFailureException (async) if the remote link cannot be authenticated on replace.
   * @throws LinkNotFoundException (async) if the link does not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> replaceLink(AnalyticsLink link) {
    return toMono(() -> async.replaceLink(link));
  }

  /**
   * Replaces an analytics link with custom options.
   *
   * @param link the name of the link that should be replaced.
   * @param options the custom options to apply.
   * @return a {@link Mono} completing when the operation is applied or failed with an error.
   * @throws InvalidArgumentException (async) if required parameters are not supplied or are invalid.
   * @throws AuthenticationFailureException (async) if the remote link cannot be authenticated on replace.
   * @throws LinkNotFoundException (async) if the link does not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> replaceLink(AnalyticsLink link, ReplaceLinkAnalyticsOptions options) {
    return toMono(() -> async.replaceLink(link, options));
  }

  /**
   * Drops (removes) a link if it exists.
   *
   * @param linkName the name of the link that should be dropped.
   * @param dataverse the name of the dataverse in which the link exists.
   * @return a {@link Mono} completing when the operation is applied or failed with an error.
   * @throws LinkNotFoundException (async) if the link does not exist.
   * @throws DataverseNotFoundException (async) if a dataverse is provided that does not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> dropLink(String linkName, String dataverse) {
    return toMono(() -> async.dropLink(linkName, dataverse));
  }

  /**
   * Drops (removes) a link if it exists with custom options.
   *
   * @param linkName the name of the link that should be dropped.
   * @param dataverse the name of the dataverse in which the link exists.
   * @param options the custom options to apply.
   * @return a {@link Mono} completing when the operation is applied or failed with an error.
   * @throws LinkNotFoundException (async) if the link does not exist.
   * @throws DataverseNotFoundException (async) if a dataverse is provided that does not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> dropLink(String linkName, String dataverse, DropLinkAnalyticsOptions options) {
    return toMono(() -> async.dropLink(linkName, dataverse, options));
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
   * @return a {@link Flux} completing with a (potentially empty) list of links or failed with an error.
   * @throws DataverseNotFoundException (async) if a dataverse is provided in the options that does not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Flux<AnalyticsLink> getLinks() {
    return toFlux(async::getLinks);
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
   * @return a {@link Flux} completing with a (potentially empty) list of links or failed with an error.
   * @throws DataverseNotFoundException (async) if a dataverse is provided in the options that does not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Flux<AnalyticsLink> getLinks(GetLinksAnalyticsOptions options) {
    return toFlux(() -> async.getLinks(options));
  }

}
