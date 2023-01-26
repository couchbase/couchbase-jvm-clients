/*
 * Copyright (c) 2019 Couchbase, Inc.
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

package com.couchbase.client.java;

import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.manager.view.DesignDocument;
import com.couchbase.client.java.manager.view.View;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.java.view.DesignDocumentNamespace;
import com.couchbase.client.java.view.ViewResult;
import com.couchbase.client.java.view.ViewRow;
import com.couchbase.client.java.view.ViewScanConsistency;
import com.couchbase.client.test.Capabilities;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.Flaky;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.couchbase.client.java.view.ViewOptions.viewOptions;
import static com.couchbase.client.test.Util.waitUntilCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@IgnoreWhen(clusterTypes = {ClusterType.MOCKED, ClusterType.CAVES},
  missesCapabilities = {Capabilities.VIEWS},
  isProtostellar = true)
class ViewIntegrationTest extends JavaIntegrationTest {

  private static final String DDOC_NAME = "everything";
  private static final String VIEW_NAME = "all";
  private static final String VIEW_WITH_REDUCE_NAME = "all_red";

  private static Cluster cluster;
  private static Bucket bucket;
  private static Collection collection;

  @BeforeAll
  static void setup() {
    cluster = createCluster();
    bucket = cluster.bucket(config().bucketname());
    collection = bucket.defaultCollection();

    bucket.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);
    waitForService(bucket, ServiceType.VIEWS);

    createDesignDocument();

    waitUntilCondition(() -> {
      List<DesignDocument> designs = bucket.viewIndexes().getAllDesignDocuments(DesignDocumentNamespace.PRODUCTION);
      for (DesignDocument design : designs) {
        if (design.name().equals(DDOC_NAME)) {
          return true;
        }
      }

      return false;
    });
    //Extra wait for service for CI test stability
    waitForService(bucket, ServiceType.VIEWS);
  }

  @AfterAll
  static void tearDown() {
    cluster.disconnect();
  }

  private static void createDesignDocument() {
    Map<String, View> views = new HashMap<>();
    views.put(VIEW_NAME, new View("function(doc,meta) { emit(meta.id, doc) }"));
    views.put(VIEW_WITH_REDUCE_NAME, new View("function(doc,meta) { emit(meta.id, doc) }", "_count"));

    DesignDocument designDocument = new DesignDocument(DDOC_NAME, views);
    bucket.viewIndexes().upsertDesignDocument(designDocument, DesignDocumentNamespace.PRODUCTION);
  }

  @Disabled
  @Flaky // Intermittently timing out on CI after 75s with ViewNotFoundException, which should not be possible
  @Test
  void succeedsWithNoRowsReturned() {
    ViewResult viewResult = bucket.viewQuery(DDOC_NAME, VIEW_NAME, viewOptions().limit(0));
    assertTrue(viewResult.rows().isEmpty());
    assertFalse(viewResult.metaData().debug().isPresent());
  }

  /**
   * Regression test for JCBC-1798
   */
  @Disabled @Flaky // Intermittently timing out on CI after 75s with ViewNotFoundException, which should not be possible
  @Test
  void canReadDebugInfo() {
    ViewResult viewResult = bucket.viewQuery(DDOC_NAME, VIEW_NAME, viewOptions().debug(true));
    assertTrue(viewResult.metaData().debug().isPresent());
  }

  // See this fail intermittently on CI as the returned docs don't equal what was just written - which should not be
  // possible with RequestPlus.
  @Disabled @Flaky
  @Test
  void returnsDataJustWritten() {
    int docsToWrite = 10;
    for (int i = 0; i < docsToWrite; i++) {
      collection.upsert("viewdoc-"+i, JsonObject.create());
    }

    ViewResult viewResult = bucket.viewQuery(
      DDOC_NAME,
      VIEW_NAME,
      viewOptions().scanConsistency(ViewScanConsistency.REQUEST_PLUS)
    );

    int found = 0;
    for (ViewRow row : viewResult.rows()) {
      if (row.id().get().startsWith("viewdoc-")) {
        found++;
        assertEquals(JsonObject.create(), row.valueAs(JsonObject.class).get());
      }
    }
    assertTrue(found >= docsToWrite);
  }

  /**
   * Regression test for JCBC-1664
   */
  @Disabled @Flaky // Intermittently timing out on CI after 75s with ViewNotFoundException, which should not be possible
  @Test
  void canQueryWithKeysPresent() {
    int docsToWrite = 2;
    for (int i = 0; i < docsToWrite; i++) {
      collection.upsert("keydoc-"+i, JsonObject.create());
    }

    ViewResult viewResult = bucket.viewQuery(
      DDOC_NAME,
      VIEW_NAME,
      viewOptions()
        .scanConsistency(ViewScanConsistency.REQUEST_PLUS)
        .keys(JsonArray.from("keydoc-0", "keydoc-1"))
    );

    assertEquals(2, viewResult.rows().size());
  }

  /**
   * Regression test for JVMCBC-870
   */
  @Disabled @Flaky // Intermittently timing out on CI after 75s with ViewNotFoundException, which should not be possible
  @Test
  void canQueryWithReduceEnabled() {
    int docsToWrite = 2;
    for (int i = 0; i < docsToWrite; i++) {
      collection.upsert("reddoc-"+i, JsonObject.create());
    }

    ViewResult viewResult = bucket.viewQuery(
      DDOC_NAME,
      VIEW_WITH_REDUCE_NAME,
      viewOptions().scanConsistency(ViewScanConsistency.REQUEST_PLUS)
    );

    // total rows is always 0 on a reduce response
    assertEquals(0, viewResult.metaData().totalRows());
    // since we just wrote docs, the _count value should be > 0
    assertTrue(viewResult.rows().get(0).valueAs(Integer.class).get() > 0);

    viewResult = bucket.viewQuery(
      DDOC_NAME,
      VIEW_WITH_REDUCE_NAME,
      viewOptions().scanConsistency(ViewScanConsistency.REQUEST_PLUS).limit(0)
    );

    assertEquals(0, viewResult.metaData().totalRows());
    assertTrue(viewResult.rows().isEmpty());
  }

}
