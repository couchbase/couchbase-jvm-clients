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
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.manager.analytics.link.AnalyticsLink;
import com.couchbase.client.java.manager.analytics.link.CouchbaseRemoteAnalyticsLink;
import com.couchbase.client.java.manager.analytics.link.S3ExternalAnalyticsLink;

import java.util.List;
import java.util.Map;

import static com.couchbase.client.java.AsyncUtils.block;

/**
 * Performs management operations on analytics indexes.
 */
public class AnalyticsIndexManager {

  private final AsyncAnalyticsIndexManager async;
  private final ReactiveAnalyticsIndexManager reactive;

  /**
   * Creates a new {@link AnalyticsIndexManager}.
   * <p>
   * This API is not intended to be called by the user directly, use {@link Cluster#analyticsIndexes()}
   * instead.
   *
   * @param cluster the async cluster to perform the analytics queries on.
   */
  @Stability.Internal
  public AnalyticsIndexManager(final Cluster cluster) {
    this.async = new AsyncAnalyticsIndexManager(cluster.async());
    this.reactive = new ReactiveAnalyticsIndexManager(cluster.async());
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
   * Returns the reactive version of this index manager.
   *
   * @return the reactive version of this index manager.
   */
  public ReactiveAnalyticsIndexManager reactive() {
    return reactive;
  }


  /**
   * Creates a new dataverse (analytics scope) if it does not already exist.
   *
   * @param dataverseName the name of the dataverse to create.
   * @throws DataverseExistsException if the dataverse already exists.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public void createDataverse(String dataverseName) {
    block(async.createDataverse(dataverseName));
  }

  /**
   * Creates a new dataverse (analytics scope) if it does not already exist with custom options.
   *
   * @param dataverseName the name of the dataverse to create.
   * @param options the custom options to apply.
   * @throws DataverseExistsException if the dataverse already exists.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public void createDataverse(String dataverseName, CreateDataverseAnalyticsOptions options) {
    block(async.createDataverse(dataverseName, options));
  }

  /**
   * Drops (deletes) a dataverse.
   *
   * @param dataverseName the name of the dataverse to drop.
   * @throws DataverseNotFoundException if the dataverse does not exist.
   * @throws CompilationFailureException if a dataverse that cannot be dropped (i.e. Default) is attempted.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public void dropDataverse(String dataverseName) {
    block(async.dropDataverse(dataverseName));
  }

  /**
   * Drops (deletes) a dataverse with custom options.
   *
   * @param dataverseName the name of the dataverse to drop.
   * @param options the custom options to apply.
   * @throws DataverseNotFoundException if the dataverse does not exist.
   * @throws CompilationFailureException if a dataverse that cannot be dropped (i.e. Default) is attempted.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public void dropDataverse(String dataverseName, DropDataverseAnalyticsOptions options) {
    block(async.dropDataverse(dataverseName, options));
  }

  /**
   * Fetches all dataverses (analytics scopes) from the analytics service with custom options.
   *
   * @param options the custom options to apply.
   * @return a (potentially empty) list of dataverses or failed with an error.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  @Stability.Uncommitted
  public List<AnalyticsDataverse> getAllDataverses(GetAllDataversesAnalyticsOptions options) {
    return block(async.getAllDataverses(options));
  }

  /**
   * Fetches all dataverses (analytics scopes) from the analytics service.
   *
   * @return a (potentially empty) list of dataverses or failed with an error.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  @Stability.Uncommitted
  public List<AnalyticsDataverse> getAllDataverses() {
    return block(async.getAllDataverses());
  }

  /**
   * Creates a new dataset (analytics collection) if it does not already exist.
   *
   * @param datasetName the name of the dataset to create.
   * @param bucketName the name of the bucket where the dataset is stored inside.
   * @throws DatasetExistsException if the dataset already exists.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public void createDataset(String datasetName, String bucketName) {
    block(async.createDataset(datasetName, bucketName));
  }

  /**
   * Creates a new dataset (analytics collection) if it does not already exist with custom options.
   *
   * @param datasetName the name of the dataset to create.
   * @param bucketName the name of the bucket where the dataset is stored inside.
   * @param options the custom options to apply.
   * @throws DatasetExistsException if the dataset already exists.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public void createDataset(String datasetName, String bucketName, CreateDatasetAnalyticsOptions options) {
    block(async.createDataset(datasetName, bucketName, options));
  }

  /**
   * Drops (deletes) a dataset.
   *
   * @param datasetName the name of the dataset to create.
   * @throws DatasetNotFoundException if the dataset to drop does not exist.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public void dropDataset(String datasetName) {
    block(async.dropDataset(datasetName));
  }

  /**
   * Drops (deletes) a dataset with custom options.
   *
   * @param datasetName the name of the dataset to create.
   * @param options the custom options to apply.
   * @throws DatasetNotFoundException if the dataset to drop does not exist.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public void dropDataset(String datasetName, DropDatasetAnalyticsOptions options) {
    block(async.dropDataset(datasetName, options));
  }

  /**
   * Fetches all datasets (analytics collections) from the analytics service.
   *
   * @return a (potentially empty) list of datasets or failed with an error.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public List<AnalyticsDataset> getAllDatasets() {
    return block(async.getAllDatasets());
  }

  /**
   * Fetches all datasets (analytics collections) from the analytics service with custom options.
   *
   * @param options the custom options to apply.
   * @return a (potentially empty) list of datasets or failed with an error.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public List<AnalyticsDataset> getAllDatasets(GetAllDatasetsAnalyticsOptions options) {
    return block(async.getAllDatasets(options));
  }

  /**
   * Creates a new analytics index if it does not exist.
   *
   * @param indexName the name of the index to create.
   * @param datasetName the name of the dataset in which the index should be created.
   * @param fields the fields that should be indexed.
   * @throws IndexExistsException if the index already exists and not ignored in the options.
   * @throws DataverseNotFoundException if a dataverse is provided in the options that does not exist.
   * @throws DatasetNotFoundException if a dataset is provided which does not exist.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public void createIndex(String indexName, String datasetName, Map<String, AnalyticsDataType> fields) {
    block(async.createIndex(indexName, datasetName, fields));
  }

  /**
   * Creates a new analytics index if it does not exist with custom options.
   *
   * @param indexName the name of the index to create.
   * @param datasetName the name of the dataset in which the index should be created.
   * @param fields the fields that should be indexed.
   * @param options the custom options to apply.
   * @throws IndexExistsException if the index already exists and not ignored in the options.
   * @throws DataverseNotFoundException if a dataverse is provided in the options that does not exist.
   * @throws DatasetNotFoundException if a dataset is provided which does not exist.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public void createIndex(String indexName, String datasetName, Map<String, AnalyticsDataType> fields, CreateIndexAnalyticsOptions options) {
    block(async.createIndex(indexName, datasetName, fields, options));
  }

  /**
   * Drops (removes) an index if it exists.
   *
   * @param indexName the name of the index to drop.
   * @param datasetName the dataset in which the index exists.
   * @throws IndexNotFoundException if the index does not exist and not ignored via options.
   * @throws DataverseNotFoundException if a dataverse is provided in the options that does not exist.
   * @throws DatasetNotFoundException if a dataset is provided which does not exist.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public void dropIndex(String indexName, String datasetName) {
    block(async.dropIndex(indexName, datasetName));
  }

  /**
   * Drops (removes) an index if it exists with custom options.
   *
   * @param indexName the name of the index to drop.
   * @param datasetName the dataset in which the index exists.
   * @param options the custom options to apply.
   * @throws IndexNotFoundException if the index does not exist and not ignored via options.
   * @throws DataverseNotFoundException if a dataverse is provided in the options that does not exist.
   * @throws DatasetNotFoundException if a dataset is provided which does not exist.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public void dropIndex(String indexName, String datasetName, DropIndexAnalyticsOptions options) {
    block(async.dropIndex(indexName, datasetName, options));
  }

  /**
   * Lists all analytics indexes.
   *
   * @return a list of indexes or failed with an error.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public List<AnalyticsIndex> getAllIndexes() {
    return block(async.getAllIndexes());
  }

  /**
   * Lists all analytics indexes with custom options.
   *
   * @param options the custom options to apply.
   * @return a (potentially empty) list of indexes or failed with an error.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public List<AnalyticsIndex> getAllIndexes(GetAllIndexesAnalyticsOptions options) {
    return block(async.getAllIndexes(options));
  }

  /**
   * Connects the analytics link for the default dataverse (Default.Local).
   *
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public void connectLink() {
    block(async.connectLink());
  }

  /**
   * Connects the analytics link for the default dataverse with custom options.
   *
   * @param options the custom options to apply.
   * @throws LinkNotFoundException if the link does not exist.
   * @throws DataverseNotFoundException if a dataverse is provided in the options that does not exist.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public void connectLink(ConnectLinkAnalyticsOptions options) {
    block(async.connectLink(options));
  }

  /**
   * Disconnects the analytics link for the default dataverse (Default.Local).
   *
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public void disconnectLink() {
    block(async.disconnectLink());
  }

  /**
   * Disconnects the analytics link for the default dataverse with custom options.
   *
   * @param options the custom options to apply.
   * @throws LinkNotFoundException if the link does not exist.
   * @throws DataverseNotFoundException if a dataverse is provided in the options that does not exist.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public void disconnectLink(DisconnectLinkAnalyticsOptions options) {
    block(async.disconnectLink(options));
  }

  /**
   * Returns the pending mutations for different dataverses.
   *
   * @return the pending mutations or failed with an error.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public Map<String, Map<String, Long>> getPendingMutations() {
    return block(async.getPendingMutations());
  }

  /**
   * Returns the pending mutations for different dataverses with custom options.
   *
   * @param options the custom options to apply.
   * @return the pending mutations or failed with an error.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public Map<String, Map<String, Long>> getPendingMutations(final GetPendingMutationsAnalyticsOptions options) {
    return block(async.getPendingMutations(options));
  }

  /**
   * Creates a new analytics link.
   *
   * @param link the name of the link that should be created.
   * @throws InvalidArgumentException if required parameters are not supplied or are invalid.
   * @throws AuthenticationFailureException if the remote link cannot be authenticated on creation.
   * @throws LinkExistsException if the link with the name already exists.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public void createLink(AnalyticsLink link) {
    block(async.createLink(link));
  }

  /**
   * Creates a new analytics link with custom options.
   *
   * @param link the name of the link that should be created.
   * @param options the custom options to apply.
   * @throws InvalidArgumentException if required parameters are not supplied or are invalid.
   * @throws AuthenticationFailureException if the remote link cannot be authenticated on creation.
   * @throws LinkExistsException if the link with the name already exists.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public void createLink(AnalyticsLink link, CreateLinkAnalyticsOptions options) {
    block(async.createLink(link, options));
  }

  /**
   * Replaces an existing analytics link.
   *
   * @param link the name of the link that should be replaced.
   * @throws InvalidArgumentException if required parameters are not supplied or are invalid.
   * @throws AuthenticationFailureException if the remote link cannot be authenticated on replace.
   * @throws LinkNotFoundException if the link does not exist.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public void replaceLink(AnalyticsLink link) {
    block(async.replaceLink(link));
  }

  /**
   * Replaces an analytics link with custom options.
   *
   * @param link the name of the link that should be replaced.
   * @param options the custom options to apply.
   * @throws InvalidArgumentException if required parameters are not supplied or are invalid.
   * @throws AuthenticationFailureException if the remote link cannot be authenticated on replace.
   * @throws LinkNotFoundException if the link does not exist.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public void replaceLink(AnalyticsLink link, ReplaceLinkAnalyticsOptions options) {
    block(async.replaceLink(link, options));
  }

  /**
   * Drops (removes) a link if it exists.
   *
   * @param linkName the name of the link that should be dropped.
   * @param dataverse the name of the dataverse in which the link exists.
   * @throws LinkNotFoundException if the link does not exist.
   * @throws DataverseNotFoundException if a dataverse is provided that does not exist.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public void dropLink(String linkName, String dataverse) {
    block(async.dropLink(linkName, dataverse));
  }

  /**
   * Drops (removes) a link if it exists with custom options.
   *
   * @param linkName the name of the link that should be dropped.
   * @param dataverse the name of the dataverse in which the link exists.
   * @param options the custom options to apply.
   * @throws LinkNotFoundException if the link does not exist.
   * @throws DataverseNotFoundException if a dataverse is provided that does not exist.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public void dropLink(String linkName, String dataverse, DropLinkAnalyticsOptions options) {
    block(async.dropLink(linkName, dataverse, options));
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
   * @return a (potentially empty) list of links or failed with an error.
   * @throws DataverseNotFoundException if a dataverse is provided in the options that does not exist.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public List<AnalyticsLink> getLinks() {
    return block(async.getLinks());
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
   * @return a (potentially empty) list of links or failed with an error.
   * @throws DataverseNotFoundException if a dataverse is provided in the options that does not exist.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public List<AnalyticsLink> getLinks(GetLinksAnalyticsOptions options) {
    return block(async.getLinks(options));
  }
}
