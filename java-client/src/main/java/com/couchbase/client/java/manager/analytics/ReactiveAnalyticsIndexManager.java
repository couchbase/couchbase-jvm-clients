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

import com.couchbase.client.core.error.DataverseExistsException;
import com.couchbase.client.core.error.DataverseNotFoundException;
import com.couchbase.client.java.AsyncCluster;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;

import static com.couchbase.client.core.Reactor.toFlux;
import static com.couchbase.client.core.Reactor.toMono;

public class ReactiveAnalyticsIndexManager {
  private final AsyncAnalyticsIndexManager async;

  public ReactiveAnalyticsIndexManager(AsyncCluster cluster) {
    this.async = new AsyncAnalyticsIndexManager(cluster);
  }

  /**
   * @throws DataverseExistsException if a dataverse with the given name already exists
   */
  public Mono<Void> createDataverse(String dataverseName) {
    return Mono.defer(() -> toMono(() -> async.createDataverse(dataverseName)));
  }

  /**
   * @throws DataverseExistsException if a dataverse with the given name already exist
   *                                         and the options do not specify to ignore this condition.
   */
  public Mono<Void> createDataverse(String dataverseName, CreateDataverseAnalyticsOptions options) {
    return Mono.defer(() -> toMono(() -> async.createDataverse(dataverseName, options)));
  }

  /**
   * @throws DataverseNotFoundException if no dataverse with the given name exists
   */
  public Mono<Void> dropDataverse(String dataverseName) {
    return Mono.defer(() -> toMono(() -> async.dropDataverse(dataverseName)));
  }

  /**
   * @throws DataverseNotFoundException if no dataverse with the given name exists
   *                                    and the options do not specify to ignore this condition.
   */
  public Mono<Void> dropDataverse(String dataverseName, DropDataverseAnalyticsOptions options) {
    return Mono.defer(() -> toMono(() -> async.dropDataverse(dataverseName, options)));
  }

  public Flux<AnalyticsDataverse> getAllDataverses() {
    return Flux.defer(() -> toFlux(async::getAllDataverses));
  }

  public Mono<Void> createDataset(String datasetName, String bucketName) {
    return Mono.defer(() -> toMono(() -> async.createDataset(datasetName, bucketName)));
  }

  public Mono<Void> createDataset(String datasetName, String bucketName, CreateDatasetAnalyticsOptions options) {
    return Mono.defer(() -> toMono(() -> async.createDataset(datasetName, bucketName, options)));
  }

  public Mono<Void> dropDataset(String datasetName) {
    return Mono.defer(() -> toMono(() -> async.dropDataset(datasetName)));
  }

  public Mono<Void> dropDataset(String datasetName, DropDatasetAnalyticsOptions options) {
    return Mono.defer(() -> toMono(() -> async.dropDataset(datasetName, options)));
  }

  public Flux<AnalyticsDataset> getAllDatasets() {
    return Flux.defer(() -> toFlux(async::getAllDatasets));
  }

  public Flux<AnalyticsDataset> getAllDatasets(GetAllDatasetsAnalyticsOptions options) {
    return Flux.defer(() -> toFlux(() -> async.getAllDatasets(options)));
  }

  public Mono<Void> createIndex(String indexName, String datasetName, Map<String, AnalyticsDataType> fields) {
    return Mono.defer(() -> toMono(() -> async.createIndex(indexName, datasetName, fields)));
  }

  public Mono<Void> createIndex(String indexName, String datasetName, Map<String, AnalyticsDataType> fields, CreateIndexAnalyticsOptions options) {
    return Mono.defer(() -> toMono(() -> async.createIndex(indexName, datasetName, fields, options)));
  }

  public Mono<Void> dropIndex(String indexName, String datasetName) {
    return Mono.defer(() -> toMono(() -> async.dropIndex(indexName, datasetName)));
  }

  public Mono<Void> dropIndex(String indexName, String datasetName, DropIndexAnalyticsOptions options) {
    return Mono.defer(() -> toMono(() -> async.dropIndex(indexName, datasetName, options)));
  }

  public Flux<AnalyticsIndex> getAllIndexes() {
    return Flux.defer(() -> toFlux(async::getAllIndexes));
  }

  public Flux<AnalyticsIndex> getAllIndexes(GetAllIndexesAnalyticsOptions options) {
    return Flux.defer(() -> toFlux(() -> async.getAllIndexes(options)));
  }

  public Mono<Void> connectLink() {
    return Mono.defer(() -> toMono(async::connectLink));
  }

  public Mono<Void> connectLink(ConnectLinkAnalyticsOptions options) {
    return Mono.defer(() -> toMono(() -> async.connectLink(options)));
  }

  public Mono<Void> disconnectLink() {
    return Mono.defer(() -> toMono(async::disconnectLink));
  }

  public Mono<Void> disconnectLink(DisconnectLinkAnalyticsOptions options) {
    return Mono.defer(() -> toMono(() -> async.disconnectLink(options)));
  }

  public Mono<Map<String, Long>> getPendingMutations() {
    return Mono.defer(() -> toMono(async::getPendingMutations));
  }

  public Mono<Map<String, Long>> getPendingMutations(GetPendingMutationsAnalyticsOptions options) {
    return Mono.defer(() -> toMono(() -> async.getPendingMutations(options)));
  }
}
