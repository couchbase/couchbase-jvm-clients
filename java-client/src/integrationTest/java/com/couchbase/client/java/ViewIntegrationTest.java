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

import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.manager.view.DesignDocument;
import com.couchbase.client.java.manager.view.View;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.java.view.DesignDocumentNamespace;
import com.couchbase.client.java.view.ViewResult;
import com.couchbase.client.java.view.ViewRow;
import com.couchbase.client.java.view.ViewScanConsistency;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.couchbase.client.java.view.ViewOptions.viewOptions;
import static com.couchbase.client.test.Util.waitUntilCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@IgnoreWhen(clusterTypes = ClusterType.MOCKED)
class ViewIntegrationTest extends JavaIntegrationTest {

  private static String DDOC_NAME = "everything";
  private static String VIEW_NAME = "all";

  private static Cluster cluster;
  private static Bucket bucket;
  private static Collection collection;

  @BeforeAll
  static void setup() {
    cluster = Cluster.connect(seedNodes(), clusterOptions());
    bucket = cluster.bucket(config().bucketname());
    collection = bucket.defaultCollection();

    waitUntilCondition(() -> cluster.core().clusterConfig().hasClusterOrBucketConfig());

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
  }

  @AfterAll
  static void tearDown() {
    cluster.disconnect();
  }

  private static void createDesignDocument() {
    Map<String, View> views = new HashMap<>();
    views.put(VIEW_NAME, new View("function(doc,meta) { emit(meta.id, doc) }"));
    DesignDocument designDocument = new DesignDocument(DDOC_NAME, views);
    bucket.viewIndexes().upsertDesignDocument(designDocument, DesignDocumentNamespace.PRODUCTION);
  }

  @Test
  void succeedsWithNoRowsReturned() {
    ViewResult viewResult = bucket.viewQuery(DDOC_NAME, VIEW_NAME, viewOptions().limit(0));
    assertTrue(viewResult.rows().isEmpty());
    assertFalse(viewResult.metaData().debug().isPresent());
  }

  @Test
  void returnsDataJustWritten() {
    int docsToWrite = 10;
    for (int i = 0; i < docsToWrite; i++) {
      collection.upsert("viewdoc-"+i, JsonObject.empty());
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
        assertEquals(JsonObject.empty(), row.valueAs(JsonObject.class).get());
      }
    }
    assertTrue(found >= docsToWrite);
  }

}
