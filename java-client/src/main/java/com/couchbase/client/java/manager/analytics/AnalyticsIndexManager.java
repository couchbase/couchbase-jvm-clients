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
import com.couchbase.client.core.error.DataverseExistsException;
import com.couchbase.client.core.error.DataverseNotFoundException;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.manager.analytics.link.AnalyticsLink;

import java.util.List;
import java.util.Map;

import static com.couchbase.client.java.AsyncUtils.block;

public class AnalyticsIndexManager {
  private final AsyncAnalyticsIndexManager async;
  private final ReactiveAnalyticsIndexManager reactive;

  public AnalyticsIndexManager(Cluster cluster) {
    this.async = new AsyncAnalyticsIndexManager(cluster.async());
    this.reactive = new ReactiveAnalyticsIndexManager(cluster.async());
  }

  public AsyncAnalyticsIndexManager async() {
    return async;
  }

  public ReactiveAnalyticsIndexManager reactive() {
    return reactive;
  }

  /**
   * @throws DataverseExistsException if a dataverse with the given name already exists
   */
  public void createDataverse(String dataverseName) {
    block(async.createDataverse(dataverseName));
  }

  /**
   * @throws DataverseExistsException if a dataverse with the given name already exist
   *                                         and the options do not specify to ignore this condition.
   */
  public void createDataverse(String dataverseName, CreateDataverseAnalyticsOptions options) {
    block(async.createDataverse(dataverseName, options));
  }

  /**
   * @throws DataverseNotFoundException if no dataverse with the given name exists
   */
  public void dropDataverse(String dataverseName) {
    block(async.dropDataverse(dataverseName));
  }

  /**
   * @throws DataverseNotFoundException if no dataverse with the given name exists
   *                                    and the options do not specify to ignore this condition.
   */
  public void dropDataverse(String dataverseName, DropDataverseAnalyticsOptions options) {
    block(async.dropDataverse(dataverseName, options));
  }

  @Stability.Uncommitted
  public List<AnalyticsDataverse> getAllDataverses(GetAllDataversesAnalyticsOptions options) {
    return block(async.getAllDataverses(options));
  }

  @Stability.Uncommitted
  public List<AnalyticsDataverse> getAllDataverses() {
    return block(async.getAllDataverses());
  }

  public void createDataset(String datasetName, String bucketName) {
    block(async.createDataset(datasetName, bucketName));
  }

  public void createDataset(String datasetName, String bucketName, CreateDatasetAnalyticsOptions options) {
    block(async.createDataset(datasetName, bucketName, options));
  }

  public void dropDataset(String datasetName) {
    block(async.dropDataset(datasetName));
  }

  public void dropDataset(String datasetName, DropDatasetAnalyticsOptions options) {
    block(async.dropDataset(datasetName, options));
  }

  public List<AnalyticsDataset> getAllDatasets() {
    return block(async.getAllDatasets());
  }

  public List<AnalyticsDataset> getAllDatasets(GetAllDatasetsAnalyticsOptions options) {
    return block(async.getAllDatasets(options));
  }

  public void createIndex(String indexName, String datasetName, Map<String, AnalyticsDataType> fields) {
    block(async.createIndex(indexName, datasetName, fields));
  }

  public void createIndex(String indexName, String datasetName, Map<String, AnalyticsDataType> fields, CreateIndexAnalyticsOptions options) {
    block(async.createIndex(indexName, datasetName, fields, options));
  }

  public void dropIndex(String indexName, String datasetName) {
    block(async.dropIndex(indexName, datasetName));
  }

  public void dropIndex(String indexName, String datasetName, DropIndexAnalyticsOptions options) {
    block(async.dropIndex(indexName, datasetName, options));
  }

  public List<AnalyticsIndex> getAllIndexes() {
    return block(async.getAllIndexes());
  }

  public List<AnalyticsIndex> getAllIndexes(GetAllIndexesAnalyticsOptions options) {
    return block(async.getAllIndexes(options));
  }

  public void connectLink() {
    block(async.connectLink());
  }

  public void connectLink(ConnectLinkAnalyticsOptions options) {
    block(async.connectLink(options));
  }

  public void disconnectLink() {
    block(async.disconnectLink());
  }

  public void disconnectLink(DisconnectLinkAnalyticsOptions options) {
    block(async.disconnectLink(options));
  }

  public Map<String, Map<String, Long>> getPendingMutations() {
    return block(async.getPendingMutations());
  }

  public Map<String, Map<String, Long>> getPendingMutations(final GetPendingMutationsAnalyticsOptions options) {
    return block(async.getPendingMutations(options));
  }

  public void createLink(AnalyticsLink link) {
    block(async.createLink(link));
  }

  public void createLink(AnalyticsLink link, CreateLinkAnalyticsOptions options) {
    block(async.createLink(link, options));
  }

  public void replaceLink(AnalyticsLink link) {
    block(async.replaceLink(link));
  }

  public void replaceLink(AnalyticsLink link, ReplaceLinkAnalyticsOptions options) {
    block(async.replaceLink(link, options));
  }

  public void dropLink(String linkName, String dataverse) {
    block(async.dropLink(linkName, dataverse));
  }

  public void dropLink(String linkName, String dataverse, DropLinkAnalyticsOptions options) {
    block(async.dropLink(linkName, dataverse, options));
  }

  public List<AnalyticsLink> getLinks() {
    return block(async.getLinks());
  }

  public List<AnalyticsLink> getLinks(GetLinksAnalyticsOptions options) {
    return block(async.getLinks(options));
  }
}
