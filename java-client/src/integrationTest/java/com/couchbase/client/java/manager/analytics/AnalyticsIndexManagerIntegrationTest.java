/*
 * Copyright (c) 2018 Couchbase, Inc.
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

package com.couchbase.client.java.manager.analytics;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.Capabilities;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.couchbase.client.core.util.CbCollections.mapOf;
import static com.couchbase.client.core.util.CbCollections.setOf;
import static com.couchbase.client.java.manager.analytics.ConnectLinkAnalyticsOptions.connectLinkAnalyticsOptions;
import static com.couchbase.client.java.manager.analytics.CreateDatasetAnalyticsOptions.createDatasetAnalyticsOptions;
import static com.couchbase.client.java.manager.analytics.CreateDataverseAnalyticsOptions.createDataverseAnalyticsOptions;
import static com.couchbase.client.java.manager.analytics.CreateIndexAnalyticsOptions.createIndexAnalyticsOptions;
import static com.couchbase.client.java.manager.analytics.DropDatasetAnalyticsOptions.dropDatasetAnalyticsOptions;
import static com.couchbase.client.java.manager.analytics.DropDataverseAnalyticsOptions.dropDataverseAnalyticsOptions;
import static com.couchbase.client.java.manager.analytics.DropIndexAnalyticsOptions.dropIndexAnalyticsOptions;
import static com.couchbase.client.test.Capabilities.ANALYTICS;
import static com.couchbase.client.test.ClusterType.MOCKED;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@IgnoreWhen(clusterTypes = MOCKED, missesCapabilities = ANALYTICS)
class AnalyticsIndexManagerIntegrationTest extends JavaIntegrationTest {

  private static final String dataset = "myDataset";
  private static final String dataverse = "myDataverse";
  private static final String index = "myIndex";

  private static Cluster cluster;
  private static ClusterEnvironment environment;
  private static AnalyticsIndexManager analytics;
  private static Bucket bucket;

  @BeforeAll
  static void setup() {
    environment = environment().build();
    cluster = Cluster.connect(environment);
    bucket = cluster.bucket(config().bucketname());
    analytics = cluster.analyticsIndexes();
  }

  @AfterAll
  static void tearDown() {
    cluster.shutdown();
    environment.shutdown();
  }

  @BeforeEach
  void reset() {
    final Set<String> builtIns = setOf("Default", "Metadata");

    getAllDataverseNames().stream()
        .filter(name -> !builtIns.contains(name))
        .forEach(name -> {
          analytics.dropDataverse(name);
        });

    // clean up the Default dataverse
    dropAllDatasets();
    dropAllIndexes();

    assertEquals(builtIns, getAllDataverseNames());

    analytics.disconnectLink();
  }

  private Set<String> getAllDataverseNames() {
    return analytics.getAllDataverses().stream()
        .map(AnalyticsDataverse::name)
        .collect(Collectors.toSet());
  }

  private void dropAllDatasets() {
    analytics.getAllDatasets().forEach(ds ->
        analytics.dropDataset(ds.name(), dropDatasetAnalyticsOptions()
            .dataverseName(ds.dataverseName())));
  }

  private void dropAllIndexes() {
    analytics.getAllIndexes().forEach(idx ->
        analytics.dropIndex(idx.name(), idx.datasetName(), dropIndexAnalyticsOptions()
            .dataverseName(idx.dataverseName())));
  }

  private static final String name = "integration-test dataverse";

  @Test
  void createDataverse() {
    analytics.createDataverse(name);
    assertDataverseExists(name);
  }

  @Test
  void createDataverseFailsIfAlreadyExists() {
    analytics.createDataverse(name);

    assertThrows(DataverseAlreadyExistsException.class, () -> analytics.createDataverse(name));
  }

  @Test
  void createDataverseCanIgoreIfExists() {
    analytics.createDataverse(name);
    analytics.createDataverse(name, createDataverseAnalyticsOptions().ignoreIfExists(true));
  }

  @Test
  void dropDataverse() {
    analytics.createDataverse(name);
    assertDataverseExists(name);

    analytics.dropDataverse(name);
    assertDataverseDoesNotExist(name);
  }

  @Test
  void dropDataverseFailsIfAbsent() {
    assertThrows(DataverseNotFoundException.class, () -> analytics.dropDataverse(name));
  }

  @Test
  void dropDataverseCanIgnoreIfAbsent() {
    analytics.dropDataverse(name, dropDataverseAnalyticsOptions().ignoreIfNotExists(true));
  }

  private void assertDataverseExists(String name) {
    assertTrue(getAllDataverseNames().contains(name));
  }

  private void assertDataverseDoesNotExist(String name) {
    assertFalse(getAllDataverseNames().contains(name));
  }

  @Test
  void createDataset() {
    analytics.createDataset("foo", bucket.name());

    analytics.createDataverse("myDataverse");
    analytics.createDataset("foo", bucket.name(),
        createDatasetAnalyticsOptions()
            .dataverseName("myDataverse"));

    Set<String> actual = analytics.getAllDatasets().stream()
        .map(ds -> ds.dataverseName() + "::" + ds.name())
        .collect(Collectors.toSet());
    assertEquals(setOf("Default::foo", "myDataverse::foo"), actual);
  }

  @Test
  void createDatasetFailsIfAlreadyExists() {
    analytics.createDataset("foo", bucket.name());
    assertThrows(DatasetAlreadyExistsException.class, () -> analytics.createDataset("foo", bucket.name()));
  }

  @Test
  void createDatasetCanIgnoreExistingFailsIfAlreadyExists() {
    analytics.createDataset("foo", bucket.name());
    analytics.createDataset("foo", bucket.name(),
        createDatasetAnalyticsOptions()
            .ignoreIfExists(true));
  }

  @Test
  void dropDatasetFailsIfAbsent() {
    assertThrows(DatasetNotFoundException.class, () -> analytics.dropDataset("foo"));

    assertThrows(DatasetNotFoundException.class, () -> analytics.dropDataset("foo",
        dropDatasetAnalyticsOptions()
            .dataverseName("absentDataverse")));
  }

  @Test
  void dropDatasetCanIgnoreAbsent() {
    analytics.dropDataset("foo",
        dropDatasetAnalyticsOptions()
            .ignoreIfNotExists(true));

    analytics.dropDataset("foo",
        dropDatasetAnalyticsOptions()
            .ignoreIfNotExists(true)
            .dataverseName("absentDataverse"));
  }

  @Test
  void createIndex() {
    analytics.createDataset(dataset, bucket.name());

    analytics.createDataverse(dataverse);
    analytics.createDataset(dataset, bucket.name(),
        createDatasetAnalyticsOptions()
            .dataverseName(dataverse));

    // primary indexes are created automatically with same name as dataset
    Set<String> primaryIndexes = setOf(
        String.join("::", "Default", dataset, dataset),
        String.join("::", dataverse, dataset, dataset));

    assertEquals(primaryIndexes, getIndexIds());

    final Map<String, AnalyticsDataType> fields = mapOf(
        "a", AnalyticsDataType.INT64,
        "b", AnalyticsDataType.DOUBLE,
        "c", AnalyticsDataType.STRING);

    analytics.createIndex(index, dataset, fields);
    analytics.createIndex(index, dataset, fields,
        createIndexAnalyticsOptions()
            .dataverseName(dataverse));

    Set<String> expectedIndexIds = new HashSet<>(primaryIndexes);
    expectedIndexIds.addAll(setOf(
        String.join("::", "Default", dataset, index),
        String.join("::", dataverse, dataset, index)));
    assertEquals(expectedIndexIds, getIndexIds());
  }

  private Set<String> getIndexIds() {
    return analytics.getAllIndexes().stream()
        .map(idx -> idx.dataverseName() + "::" + idx.datasetName() + "::" + idx.name())
        .collect(Collectors.toSet());
  }

  @Test
  void dropIndexFailsIfNotFound() {
    assertThrows(DatasetNotFoundException.class, () -> analytics.dropIndex(index, dataset));
    assertThrows(DatasetNotFoundException.class, () -> analytics.dropIndex(index, dataset,
        dropIndexAnalyticsOptions()
            .dataverseName(dataverse)));

    analytics.createDataset(dataset, bucket.name());

    analytics.createDataverse(dataverse);
    analytics.createDataset(dataset, bucket.name(),
        createDatasetAnalyticsOptions()
            .dataverseName(dataverse));

    assertThrows(IndexNotFoundException.class, () -> analytics.dropIndex(index, dataset));
    assertThrows(IndexNotFoundException.class, () -> analytics.dropIndex(index, dataset,
        dropIndexAnalyticsOptions()
            .dataverseName(dataverse)));
  }

  @Test
  void dropIndexCanIgnoreNotFound() {
    analytics.createDataset(dataset, bucket.name());
    analytics.createDataverse(dataverse);
    analytics.createDataset(dataset, bucket.name(),
        createDatasetAnalyticsOptions()
            .dataverseName(dataverse));

    analytics.dropIndex(index, dataset,
        dropIndexAnalyticsOptions()
            .ignoreIfNotExists(true));

    analytics.dropIndex(index, dataset,
        dropIndexAnalyticsOptions()
            .dataverseName(dataverse)
            .ignoreIfNotExists(true));
  }

  @Test
  void createIndexFailsIfAlreadyExists() {
    analytics.createDataset(dataset, bucket.name());

    analytics.createDataverse(dataverse);
    analytics.createDataset(dataset, bucket.name(),
        createDatasetAnalyticsOptions()
            .dataverseName(dataverse));

    final Map<String, AnalyticsDataType> fields = mapOf(
        "a", AnalyticsDataType.INT64,
        "b", AnalyticsDataType.DOUBLE,
        "c", AnalyticsDataType.STRING);

    analytics.createIndex(index, dataset, fields);

    assertThrows(IndexAlreadyExistsException.class, () -> analytics.createIndex(index, dataset, fields));

    // do the ignoreIfExists check here to, since the setup is a pain
    analytics.createIndex(index, dataset, fields,
        createIndexAnalyticsOptions()
            .ignoreIfExists(true));

    // now again, specifying the dataverse
    analytics.createIndex(index, dataset, fields,
        createIndexAnalyticsOptions()
            .dataverseName(dataverse));

    assertThrows(IndexAlreadyExistsException.class, () -> analytics.createIndex(index, dataset, fields,
        createIndexAnalyticsOptions()
            .dataverseName(dataverse)));

    analytics.createIndex(index, dataset, fields,
        createIndexAnalyticsOptions()
            .ignoreIfExists(true)
            .dataverseName(dataverse));
  }

  @Test
  void connectLinkFailsIfAbsent() {
    assertThrows(AnalyticsLinkNotFoundException.class, () -> analytics.connectLink(
        connectLinkAnalyticsOptions()
            .dataverseName(dataverse)));

    assertThrows(AnalyticsLinkNotFoundException.class, () -> analytics.connectLink(
        connectLinkAnalyticsOptions()
            .linkName("bogusLink")));
  }

  @Test
  void connectLink() {
    try {
      analytics.connectLink();

      analytics.createDataverse(dataverse);
      analytics.connectLink(
          connectLinkAnalyticsOptions()
              .dataverseName(dataverse));

      analytics.connectLink(
          connectLinkAnalyticsOptions()
              .force(true));

    } finally {
      // since the dataverse itself isn't deleted as part of cleanup...
      analytics.disconnectLink();
    }
  }

  @Test
  @IgnoreWhen(missesCapabilities = Capabilities.COLLECTIONS)
  void getPendingMutations() {
    try {
      assertEquals(Collections.<String, Long>emptyMap(), analytics.getPendingMutations());

      analytics.createDataset(dataset, bucket.name());
      analytics.connectLink();

      assertEquals(singletonMap("Default.myDataset", 0L), analytics.getPendingMutations());

    } finally {
      analytics.disconnectLink();
    }
  }
}
