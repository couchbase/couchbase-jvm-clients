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

import com.couchbase.client.core.deps.com.fasterxml.jackson.core.type.TypeReference;
import com.couchbase.client.core.error.DatasetExistsException;
import com.couchbase.client.core.error.DatasetNotFoundException;
import com.couchbase.client.core.error.DataverseExistsException;
import com.couchbase.client.core.error.DataverseNotFoundException;
import com.couchbase.client.core.error.IndexExistsException;
import com.couchbase.client.core.error.IndexNotFoundException;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.error.LinkExistsException;
import com.couchbase.client.core.error.LinkNotFoundException;
import com.couchbase.client.core.error.ParsingFailureException;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.manager.analytics.link.AnalyticsLink;
import com.couchbase.client.java.manager.analytics.link.AnalyticsLinkType;
import com.couchbase.client.java.manager.analytics.link.S3ExternalAnalyticsLink;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.Flaky;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.couchbase.client.core.util.CbCollections.listOf;
import static com.couchbase.client.core.util.CbCollections.mapOf;
import static com.couchbase.client.core.util.CbCollections.setOf;
import static com.couchbase.client.java.manager.analytics.ConnectLinkAnalyticsOptions.connectLinkAnalyticsOptions;
import static com.couchbase.client.java.manager.analytics.CreateDatasetAnalyticsOptions.createDatasetAnalyticsOptions;
import static com.couchbase.client.java.manager.analytics.CreateDataverseAnalyticsOptions.createDataverseAnalyticsOptions;
import static com.couchbase.client.java.manager.analytics.CreateIndexAnalyticsOptions.createIndexAnalyticsOptions;
import static com.couchbase.client.java.manager.analytics.DisconnectLinkAnalyticsOptions.disconnectLinkAnalyticsOptions;
import static com.couchbase.client.java.manager.analytics.DropDatasetAnalyticsOptions.dropDatasetAnalyticsOptions;
import static com.couchbase.client.java.manager.analytics.DropDataverseAnalyticsOptions.dropDataverseAnalyticsOptions;
import static com.couchbase.client.java.manager.analytics.DropIndexAnalyticsOptions.dropIndexAnalyticsOptions;
import static com.couchbase.client.java.manager.analytics.GetLinksAnalyticsOptions.getLinksAnalyticsOptions;
import static com.couchbase.client.test.Capabilities.ANALYTICS;
import static com.couchbase.client.test.Capabilities.COLLECTIONS;
import static com.couchbase.client.test.Capabilities.SUBDOC_REPLACE_BODY_WITH_XATTR;
import static com.couchbase.client.test.Capabilities.SUBDOC_REVIVE_DOCUMENT;
import static com.couchbase.client.test.ClusterType.CAVES;
import static com.couchbase.client.test.ClusterType.MOCKED;
import static com.couchbase.client.test.Util.waitUntilCondition;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Disabled
@Flaky
@IgnoreWhen(clusterTypes = {MOCKED, CAVES},
  missesCapabilities = ANALYTICS,
  isProtostellar = true
)
class AnalyticsIndexManagerIntegrationTest extends JavaIntegrationTest {

  private static final String dataset = "myDataset";
  private static final String dataverse = "integration-test-dataverse";
  private static final String absentDataverse = "absentDataverse";
  private static final String index = "myIndex";

  private static Cluster cluster;
  private static AnalyticsIndexManager analytics;
  private static Bucket bucket;

  @BeforeAll
  static void setup() {
    cluster = createCluster();
    bucket = cluster.bucket(config().bucketname());
    analytics = cluster.analyticsIndexes();
    bucket.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);
    waitForService(bucket, ServiceType.ANALYTICS);
  }

  @AfterAll
  static void tearDown() {
    cluster.disconnect();
  }

  @BeforeEach
  void reset() {
    final Set<String> builtIns = setOf("Default", "Metadata");
    getAllDataverseNames().stream()
        .filter(name -> !builtIns.contains(name))
        .forEach(this::disconnectLocalLink);

    getAllDataverseNames().stream()
        .filter(name -> !builtIns.contains(name))
        .forEach(name -> analytics.dropDataverse(name));

    // clean up the Default dataverse
    dropAllDatasets();
    dropAllIndexes();

    assertEquals(builtIns, getAllDataverseNames());

    analytics.disconnectLink();

    recreateDataverse(dataverse);
  }

  void recreateDataverse(String dataverse) {
    try {
      analytics.dropDataverse(dataverse, dropDataverseAnalyticsOptions().ignoreIfNotExists(true));
      analytics.createDataverse(dataverse);
    } catch (ParsingFailureException e) {
      if (dataverse.contains("/")) {
        Assumptions.assumeFalse(true, "Skipping test because server does not support slash in dataverse name");
      }
      throw e;
    }
  }

  @AfterEach
  void cleanup() {
    disconnectLocalLink(dataverse);
    analytics.dropDataverse(dataverse, dropDataverseAnalyticsOptions()
        .ignoreIfNotExists(true));
  }

  private void disconnectLocalLink(String dvName) {
    try {
      DisconnectLinkAnalyticsOptions opts = disconnectLinkAnalyticsOptions()
          .dataverseName(dvName)
          .linkName("Local");
      analytics.disconnectLink(opts);
    } catch (LinkNotFoundException | DataverseNotFoundException e) {
      // Ignored on purpose.
    }
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

  @Test
  void createDataverse() {
    assertDataverseExists();
  }

  @Test
  void createDataverseFailsIfAlreadyExists() {
    assertThrows(DataverseExistsException.class, () -> analytics.createDataverse(dataverse));
  }

  @Test
  void createDataverseCanIgoreIfExists() {
    analytics.createDataverse(dataverse, createDataverseAnalyticsOptions().ignoreIfExists(true));
  }

  @Test
  void dropDataverse() {
    assertDataverseExists();
    analytics.dropDataverse(dataverse);
    assertDataverseDoesNotExist(dataverse);
  }

  @Test
  void dropDataverseFailsIfAbsent() {
    assertDataverseDoesNotExist(absentDataverse);
    assertThrows(DataverseNotFoundException.class, () -> analytics.dropDataverse(absentDataverse));
  }

  @Test
  void dropDataverseCanIgnoreIfAbsent() {
    assertDataverseDoesNotExist(absentDataverse);
    analytics.dropDataverse(absentDataverse, dropDataverseAnalyticsOptions().ignoreIfNotExists(true));
  }

  private void assertDataverseExists() {
    assertTrue(getAllDataverseNames().contains(AnalyticsIndexManagerIntegrationTest.dataverse));
  }

  private void assertDataverseDoesNotExist(String name) {
    assertFalse(getAllDataverseNames().contains(name));
  }

  @Test
  void createDataset() {
    analytics.createDataset("foo", bucket.name());

    analytics.createDataset("foo", bucket.name(),
        createDatasetAnalyticsOptions()
            .dataverseName(dataverse));

    Set<String> actual = analytics.getAllDatasets().stream()
        .map(ds -> ds.dataverseName() + "::" + ds.name())
        .collect(Collectors.toSet());
    assertEquals(setOf("Default::foo", dataverse + "::foo"), actual);
  }

  @Test
  void createDatasetFailsIfAlreadyExists() {
    analytics.createDataset("foo", bucket.name());
    assertThrows(DatasetExistsException.class, () -> analytics.createDataset("foo", bucket.name()));
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

    try {
      analytics.dropDataset("foo",
              dropDatasetAnalyticsOptions()
                      .dataverseName("absentDataverse"));
      fail();
    }
    catch (DatasetNotFoundException | DataverseNotFoundException e) {
      // MB-40577:
      // DatasetNotFound returned on 6.5 and below
      // DataverseNotFound on 6.6 and above
    }
  }

  // Will fail on 6.6 build that does not include fix for MB-40577
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

    analytics.createDataset(dataset, bucket.name(),
        createDatasetAnalyticsOptions()
            .dataverseName(dataverse));

    // primary indexes are created automatically with same name as dataset
    Set<String> primaryIndexes = setOf(
        String.join("::", "Default", dataset, dataset),
        String.join("::", dataverse, dataset, dataset));

    assertEquals(primaryIndexes, getIndexIds());
    assertTrue(getIndex("Default", dataset).primary());
    assertTrue(getIndex(dataverse, dataset).primary());

    final Map<String, AnalyticsDataType> fields = mapOf(
        "a.foo", AnalyticsDataType.INT64,
        "b.bar", AnalyticsDataType.DOUBLE,
        "c", AnalyticsDataType.STRING);

    analytics.createIndex(index, dataset, fields);
    analytics.createIndex(index, dataset, fields,
        createIndexAnalyticsOptions()
            .dataverseName(dataverse));

    assertFalse(getIndex("Default", index).primary());
    assertFalse(getIndex(dataverse, index).primary());

    Set<String> expectedIndexIds = new HashSet<>(primaryIndexes);
    expectedIndexIds.addAll(setOf(
        String.join("::", "Default", dataset, index),
        String.join("::", dataverse, dataset, index)));
    assertEquals(expectedIndexIds, getIndexIds());

    Set<List<String>> expectedSearchKey = setOf(listOf("a", "foo"), listOf("b", "bar"), listOf("c"));
    assertEquals(expectedSearchKey, getSearchKeys(getIndex("Default", index)));
  }

  private Set<List<String>> getSearchKeys(AnalyticsIndex i) {
    String json = i.raw().getArray("SearchKey").toString();
    return Mapper.decodeInto(json, new TypeReference<Set<List<String>>>() {
    });
  }

  private AnalyticsIndex getIndex(String dataverseName, String indexName) {
    return analytics.getAllIndexes().stream()
        .filter(idx -> indexName.equals(idx.name()))
        .filter(idx -> dataverseName.equals(idx.dataverseName()))
        .findAny().orElseThrow(() -> new AssertionError("index " + indexName + " not found"));
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

    analytics.createDataset(dataset, bucket.name(),
        createDatasetAnalyticsOptions()
            .dataverseName(dataverse));

    final Map<String, AnalyticsDataType> fields = mapOf(
        "a", AnalyticsDataType.INT64,
        "b", AnalyticsDataType.DOUBLE,
        "c", AnalyticsDataType.STRING);

    analytics.createIndex(index, dataset, fields);

    assertThrows(IndexExistsException.class, () -> analytics.createIndex(index, dataset, fields));

    // do the ignoreIfExists check here to, since the setup is a pain
    analytics.createIndex(index, dataset, fields,
        createIndexAnalyticsOptions()
            .ignoreIfExists(true));

    // now again, specifying the dataverse
    analytics.createIndex(index, dataset, fields,
        createIndexAnalyticsOptions()
            .dataverseName(dataverse));

    assertThrows(IndexExistsException.class, () -> analytics.createIndex(index, dataset, fields,
        createIndexAnalyticsOptions()
            .dataverseName(dataverse)));

    analytics.createIndex(index, dataset, fields,
        createIndexAnalyticsOptions()
            .ignoreIfExists(true)
            .dataverseName(dataverse));
  }

  @Test
  void connectLinkFailsIfAbsent() {
    try {
      analytics.connectLink(connectLinkAnalyticsOptions().dataverseName(dataverse));
      analytics.connectLink(connectLinkAnalyticsOptions().linkName("bogusLink"));
    } catch (LinkNotFoundException | DataverseNotFoundException e) {
      // MB-40577:
      // LinkNotFoundException returned on 6.5 and below
      // DataverseNotFound on 6.6 and above
    }
  }

  @Test
  void connectLink() {
    try {
      analytics.connectLink();

      analytics.connectLink(
          connectLinkAnalyticsOptions()
              .dataverseName(dataverse));

      analytics.connectLink(
          connectLinkAnalyticsOptions()
              .force(true));

    } finally {
      // since the default dataverse itself isn't deleted as part of cleanup...
      analytics.disconnectLink();
    }
  }

  // The relevant endpoint was only added in 6.5.  On CI we see failures for 6.5 and 6.6 as we expect an empty
  // map back, but get something.  Needs to be investigated further under JVMCBC-1075.
  // Using SUBDOC_REPLACE_BODY_WITH_XATTR as a stand-in for 7.0 support.
  @IgnoreWhen(missesCapabilities = {SUBDOC_REPLACE_BODY_WITH_XATTR})
  @Test
  void getPendingMutations() {
    try {
      assertEquals(Collections.<String, Map<String, Long>>emptyMap(), analytics.getPendingMutations());

      analytics.createDataset(dataset, bucket.name());
      analytics.connectLink();

      waitUntilCondition(() -> singletonMap("Default", singletonMap(dataset, 0L)).equals(analytics.getPendingMutations()), Duration.ofSeconds(20));
    } finally {
      analytics.disconnectLink();
    }
  }

  @Test
  void createS3RemoteLink() {
    assumeCanManageLinks();

    String linkName = "myS3Link";
    analytics.createLink(newS3Link(linkName, dataverse));

    List<AnalyticsLink> links = analytics.getLinks(getLinksAnalyticsOptions().dataverseName(dataverse));
    assertEquals(1, links.size());
    assertEquals(AnalyticsLinkType.S3_EXTERNAL, links.get(0).type());
    S3ExternalAnalyticsLink link = (S3ExternalAnalyticsLink) links.get(0);
    assertEquals(linkName, link.name());
    assertEquals(dataverse, link.dataverse());
    assertEquals("accessKeyId", link.accessKeyId());
    assertEquals("us-east-1", link.region());
    assertEquals("serviceEndpoint", link.serviceEndpoint());

    assertNull(link.secretAccessKey());
    assertNull(link.sessionToken());
  }

  @Test
  void createLinkFailsIfRequiredPropertyIsMissing() {
    assumeCanManageLinks();

    assertThrows(InvalidArgumentException.class, () ->
        analytics.createLink(new S3ExternalAnalyticsLink("myS3Link", dataverse)));
  }

  @Test
  void createLinkFailsIfAlreadyExists() {
    assumeCanManageLinks();

    String linkName = "myS3Link";
    analytics.createLink(newS3Link(linkName, dataverse));

    assertThrows(LinkExistsException.class, () ->
        analytics.createLink(newS3Link(linkName, dataverse)));
  }

  @Test
  void createRemoteLinkFailsIfDataverseNotFound() {
    assumeCanManageLinks();

    assertThrows(DataverseNotFoundException.class, () ->
        analytics.createLink(newS3Link("myS3Link", absentDataverse)));
  }

  private void assumeCanManageLinks() {
    try {
      analytics.getLinks();
    } catch (Exception e) {
      Assumptions.assumeTrue(false, "Skipping because 'getLinks' failed; assuming this means link management API is not present.");
    }
  }

  @Test
  void getLinksCanFilterByLinkType() {
    assumeCanManageLinks();

    analytics.createLink(newS3Link("myS3Link", dataverse));

    assertFoundLinks(emptySet(), analytics.getLinks(getLinksAnalyticsOptions()
        .dataverseName(dataverse)
        .linkType(AnalyticsLinkType.COUCHBASE_REMOTE)));

    assertFoundLinks(setOf("myS3Link:" + dataverse), analytics.getLinks(getLinksAnalyticsOptions()
        .dataverseName(dataverse)
        .linkType(AnalyticsLinkType.S3_EXTERNAL)));
  }

  @Test
  void getLinksCanFilterByNameAndDataverse() {
    assumeCanManageLinks();

    String scopedDataverse = dataverse + "/foo";
    recreateDataverse(scopedDataverse);

    try {
      analytics.createLink(newS3Link("myS3Link", dataverse));
      analytics.createLink(newS3Link("myOtherS3Link", dataverse));
      analytics.createLink(newS3Link("myS3Link", scopedDataverse));
      analytics.createLink(newS3Link("myOtherS3Link", scopedDataverse));

      assertThrows(InvalidArgumentException.class, () -> analytics.getLinks(getLinksAnalyticsOptions()
          .name("myS3Link")));

      assertFoundLinks(setOf(
          "myS3Link:" + dataverse,
          "myOtherS3Link:" + dataverse,
          "myS3Link:" + scopedDataverse,
          "myOtherS3Link:" + scopedDataverse
      ), analytics.getLinks());

      assertFoundLinks(setOf(
          "myS3Link:" + scopedDataverse,
          "myOtherS3Link:" + scopedDataverse
      ), analytics.getLinks(getLinksAnalyticsOptions()
          .dataverseName(scopedDataverse)));

      assertFoundLinks(setOf(
          "myS3Link:" + dataverse,
          "myOtherS3Link:" + dataverse
      ), analytics.getLinks(getLinksAnalyticsOptions()
          .dataverseName(dataverse)));

      assertFoundLinks(setOf(
          "myS3Link:" + dataverse
      ), analytics.getLinks(getLinksAnalyticsOptions()
          .name("myS3Link")
          .dataverseName(dataverse)));

      assertFoundLinks(setOf(
          "myS3Link:" + scopedDataverse
      ), analytics.getLinks(getLinksAnalyticsOptions()
          .name("myS3Link")
          .dataverseName(scopedDataverse)));

    } finally {
      analytics.dropDataverse(scopedDataverse);
    }
  }

  private static void assertFoundLinks(Set<String> expectedLinkIds, List<AnalyticsLink> actualLinks) {
    Set<String> actualLinkIds = actualLinks.stream()
        .map(link -> link.name() + ":" + link.dataverse())
        .collect(Collectors.toSet());
    assertEquals(expectedLinkIds, actualLinkIds);
  }

  @Test
  void dropLinkFailsIfLinkIsNotFound() {
    assumeCanManageLinks();

    assertThrows(LinkNotFoundException.class, () ->
        analytics.dropLink("this-link-does-not-exist", dataverse));
  }

  @Test
  void dropLinkFailsIfDataverseIsNotFound() {
    assumeCanManageLinks();

    assertThrows(DataverseNotFoundException.class, () ->
        analytics.dropLink("this-link-does-not-exist", absentDataverse));
  }

  @Test
  void dropLink() {
    assumeCanManageLinks();

    String linkName = "myS3Link";
    analytics.createLink(newS3Link(linkName, dataverse));
    assertEquals(1, analytics.getLinks(getLinksAnalyticsOptions().dataverseName(dataverse)).size());
    analytics.dropLink(linkName, dataverse);
    assertEquals(emptyList(), analytics.getLinks(getLinksAnalyticsOptions().dataverseName(dataverse)));
  }

  @Test
  void replaceRemoteLink() {
    assumeCanManageLinks();

    S3ExternalAnalyticsLink s3 = newS3Link("myS3Link", dataverse);
    analytics.createLink(s3);
    s3.accessKeyId("newAccessKeyId");
    analytics.replaceLink(s3);

    S3ExternalAnalyticsLink fromServer = (S3ExternalAnalyticsLink) analytics.getLinks(getLinksAnalyticsOptions()
        .dataverseName(dataverse)
    ).get(0);

    assertEquals("newAccessKeyId", fromServer.accessKeyId());
  }

  @Test
  void replaceRemoteLinkFailsIfAbsent() {
    assumeCanManageLinks();

    assertThrows(LinkNotFoundException.class, () -> analytics.replaceLink(newS3Link("myS3Link", dataverse)));
  }

  @Test
  void canManageLinksInDataverseWithScope() {
    assumeCanManageLinks();

    String scopedDataverse = dataverse + "/foo";
    recreateDataverse(scopedDataverse);
    try {
      String linkName = "myS3Link";
      analytics.createLink(newS3Link(linkName, scopedDataverse));
      analytics.replaceLink(newS3Link(linkName, scopedDataverse));
      assertFoundLinks(setOf("myS3Link:" + scopedDataverse), analytics.getLinks(getLinksAnalyticsOptions()
          .dataverseName(scopedDataverse)
          .name(linkName)));

      analytics.dropLink(linkName, scopedDataverse);
      assertFoundLinks(emptySet(), analytics.getLinks(getLinksAnalyticsOptions()
          .dataverseName(scopedDataverse)
          .name(linkName)));

    } finally {
      analytics.dropDataverse(scopedDataverse);
    }
  }

  /**
   * Returns a fully-populated S3 link
   */
  private static S3ExternalAnalyticsLink newS3Link(String name, String dataverse) {
    return new S3ExternalAnalyticsLink(name, dataverse)
        .accessKeyId("accessKeyId")
        .secretAccessKey("secretAccessKey")
        .region("us-east-1")
        .serviceEndpoint("serviceEndpoint")
        //.sessionToken("sessionToken") // Only supported by server 7.0 and later
        ;
  }
}
