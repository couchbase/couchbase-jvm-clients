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

import com.couchbase.client.java.AsyncCluster;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;

import static com.couchbase.client.core.Reactor.toMono;

public class ReactiveAnalyticsIndexManager {
  private final AsyncAnalyticsIndexManager async;

  public ReactiveAnalyticsIndexManager(AsyncCluster cluster) {
    this.async = new AsyncAnalyticsIndexManager(cluster);
  }

  /**
   * @throws DataverseAlreadyExistsException if a dataverse with the given name already exists
   */
  public Mono<Void> createDataverse(String dataverseName) {
    return toMono(() -> async.createDataverse(dataverseName));
  }

  /**
   * @throws DataverseAlreadyExistsException if a dataverse with the given name already exist
   *                                         and the options do not specify to ignore this condition.
   */
  public Mono<Void> createDataverse(String dataverseName, CreateDataverseAnalyticsOptions options) {
    return toMono(() -> async.createDataverse(dataverseName, options));
  }

  /**
   * @throws DataverseNotFoundException if no dataverse with the given name exists
   */
  public Mono<Void> dropDataverse(String dataverseName) {
    return toMono(() -> async.dropDataverse(dataverseName));
  }

  /**
   * @throws DataverseNotFoundException if no dataverse with the given name exists
   *                                    and the options do not specify to ignore this condition.
   */
  public Mono<Void> dropDataverse(String dataverseName, DropDataverseAnalyticsOptions options) {
    return toMono(() -> async.dropDataverse(dataverseName, options));
  }

  public Mono<List<AnalyticsDataverse>> getAllDataverses() {
    return toMono(() -> async.getAllDataverses());
  }

  public Mono<Void> createDataset(String datasetName, String bucketName) {
    return toMono(() -> async.createDataset(datasetName, bucketName));
  }

  public Mono<Void> createDataset(String datasetName, String bucketName, CreateDatasetAnalyticsOptions options) {
    return toMono(() -> async.createDataset(datasetName, bucketName, options));
  }

  public Mono<Void> dropDataset(String datasetName) {
    return toMono(() -> async.dropDataset(datasetName));
  }

  public Mono<Void> dropDataset(String datasetName, DropDatasetAnalyticsOptions options) {
    return toMono(() -> async.dropDataset(datasetName, options));
  }

  public Mono<List<AnalyticsDataset>> getAllDatasets() {
    return toMono(() -> async.getAllDatasets());
  }

  public Mono<List<AnalyticsDataset>> getAllDatasets(GetAllDatasetsAnalyticsOptions options) {
    return toMono(() -> async.getAllDatasets(options));
  }

  public Mono<Void> createIndex(String indexName, String datasetName, Map<String, AnalyticsDataType> fields) {
    return toMono(() -> async.createIndex(indexName, datasetName, fields));
  }

  public Mono<Void> createIndex(String indexName, String datasetName, Map<String, AnalyticsDataType> fields, CreateIndexAnalyticsOptions options) {
    return toMono(() -> async.createIndex(indexName, datasetName, fields, options));
  }

  public Mono<Void> dropIndex(String indexName, String datasetName) {
    return toMono(() -> async.dropIndex(indexName, datasetName));
  }

  public Mono<Void> dropIndex(String indexName, String datasetName, DropIndexAnalyticsOptions options) {
    return toMono(() -> async.dropIndex(indexName, datasetName, options));
  }

  public Mono<List<AnalyticsIndex>> getAllIndexes() {
    return toMono(() -> async.getAllIndexes());
  }

  public Mono<List<AnalyticsIndex>> getAllIndexes(GetAllIndexesAnalyticsOptions options) {
    return toMono(() -> async.getAllIndexes(options));
  }

  public Mono<Void> connectLink() {
    return toMono(() -> async.connectLink());
  }

  public Mono<Void> connectLink(ConnectLinkAnalyticsOptions options) {
    return toMono(() -> async.connectLink(options));
  }

  public Mono<Void> disconnectLink() {
    return toMono(() -> async.disconnectLink());
  }

  public Mono<Void> disconnectLink(DisconnectLinkAnalyticsOptions options) {
    return toMono(() -> async.disconnectLink(options));
  }

  public Mono<Map<String, Long>> getPendingMutations() {
    return toMono(() -> async.getPendingMutations());
  }

  public Mono<Map<String, Long>> getPendingMutations(GetPendingMutationsAnalyticsOptions options) {
    return toMono(() -> async.getPendingMutations(options));
  }
}
