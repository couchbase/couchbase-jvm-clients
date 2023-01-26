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

package com.couchbase.client.java.manager.view;

import com.couchbase.client.core.error.DesignDocumentNotFoundException;
import com.couchbase.client.core.error.ViewServiceException;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.java.view.DesignDocumentNamespace;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.IgnoreWhen;
import com.couchbase.client.test.Util;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.rnorth.ducttape.unreliables.Unreliables;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;

import static com.couchbase.client.core.util.CbCollections.setOf;
import static com.couchbase.client.java.view.DesignDocumentNamespace.DEVELOPMENT;
import static com.couchbase.client.java.view.DesignDocumentNamespace.PRODUCTION;
import static com.couchbase.client.test.Util.waitUntilCondition;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@IgnoreWhen(clusterTypes = ClusterType.MOCKED,
  isProtostellar = true
)
@Disabled // JCBC-1529
class ViewManagerIntegrationTest extends JavaIntegrationTest {

  private static Cluster cluster;

  private static ViewIndexManager views;

  @BeforeAll
  static void setup() {
    cluster = createCluster();
    Bucket bucket = cluster.bucket(config().bucketname());
    views = bucket.viewIndexes();
    bucket.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);
    waitForService(bucket, ServiceType.VIEWS);
  }

  @AfterAll
  static void tearDown() {
    cluster.disconnect();
  }

  @AfterEach
  void dropDesignDocs() {
    forEachNamespace(namespace ->
        views.getAllDesignDocuments(namespace).forEach(ddoc ->
            views.dropDesignDocument(ddoc.name(), namespace)));
    forEachNamespace(namespace ->
        views.getAllDesignDocuments(namespace).forEach(ddoc ->
            waitUntilDesignDocDropped(ddoc.name(), namespace)));
  }

  private void waitUntilDesignDocPresent(String name, DesignDocumentNamespace ns) {
    Util.waitUntilCondition(() -> {
      try {
        views.getDesignDocument(name, ns);
        return true;
      } catch (DesignDocumentNotFoundException err) {
        return false;
      }
    });
  }

  private void waitUntilDesignDocDropped(String name, DesignDocumentNamespace ns) {
    Util.waitUntilCondition(() -> {
      try {
        views.getDesignDocument(name, ns);
        return false;
      } catch (DesignDocumentNotFoundException err) {
        return true;
      }
    });
  }

  @Test
  void upsertReplacesPreviousVersion() {
    forEachNamespace(namespace -> {
          exampleDocuments().forEach(doc ->
              Unreliables.retryUntilSuccess(30, SECONDS, () -> {
                assertRoundTrip(doc, namespace);

                DesignDocument newVersion = new DesignDocument(doc.name(), doc.views())
                    .putView("anotherView", "function (doc, meta) { emit(doc.foo, doc.bar); }");

                views.upsertDesignDocument(newVersion, namespace);

                Util.waitUntilCondition(() ->
                    views.getDesignDocument(doc.name(), namespace).views().containsKey("anotherView"));

                return null;
              }));
        }
    );
  }

  @Test
  void canDrop() {
    forEachNamespace(namespace -> {
          DesignDocument doc = oneExampleDocument();
          assertRoundTrip(doc, namespace);
          views.dropDesignDocument(doc.name(), namespace);
          waitUntilDesignDocDropped(doc.name(), namespace);
          assertThrows(DesignDocumentNotFoundException.class, () ->
              views.getDesignDocument(doc.name(), namespace));
        }
    );
  }

  @Test
  void dropAbsentDesignDoc() {
    DesignDocumentNotFoundException e = assertThrows(DesignDocumentNotFoundException.class,
        () -> views.dropDesignDocument("doesnotexist", DEVELOPMENT));
    assertEquals("doesnotexist", e.name());
  }

  @Test
  void publishAbsentDesignDoc() {
    DesignDocumentNotFoundException e = assertThrows(DesignDocumentNotFoundException.class,
        () -> views.publishDesignDocument("doesnotexist"));
    assertEquals("doesnotexist", e.name());
  }

  @Test
  void getAbsentDesignDoc() {
    DesignDocumentNotFoundException e = assertThrows(DesignDocumentNotFoundException.class,
        () -> views.getDesignDocument("doesnotexist", DEVELOPMENT));
    assertEquals("doesnotexist", e.name());
  }

  @Test
  void upsertBadSyntax() {
    ViewServiceException e = assertThrows(ViewServiceException.class, () ->
        views.upsertDesignDocument(
            new DesignDocument("invalid")
                .putView("x", "not javascript"),
            DEVELOPMENT));
    assertTrue(e.content().contains("invalid_design_document"));
  }

  @Test
  void getAllDesignDocuments() {
    forEachNamespace(namespace -> {
      Set<DesignDocument> docs = exampleDocuments();
      docs.forEach(doc -> views.upsertDesignDocument(doc, namespace));
      docs.forEach(doc -> waitUntilDesignDocPresent(doc.name(), namespace));
      assertEquals(docs, new HashSet<>(views.getAllDesignDocuments(namespace)));
    });
  }

  @Test
  void publish() {
    DesignDocument doc = oneExampleDocument();

    views.upsertDesignDocument(doc, DEVELOPMENT);
    waitUntilDesignDocPresent(doc.name(), DEVELOPMENT);
    views.publishDesignDocument(doc.name());
    waitUntilDesignDocPresent(doc.name(), PRODUCTION);

    assertEquals(doc, views.getDesignDocument(doc.name(), PRODUCTION));
  }

  private static void forEachNamespace(Consumer<DesignDocumentNamespace> consumer) {
    for (DesignDocumentNamespace namespace : DesignDocumentNamespace.values()) {
      consumer.accept(namespace);
    }
  }

  private void assertRoundTrip(DesignDocument doc, DesignDocumentNamespace namespace) {
    views.upsertDesignDocument(doc, namespace);
    waitUntilDesignDocPresent(doc.name(), namespace);

    waitUntilCondition(() -> {
      DesignDocument roundTrip = views.getDesignDocument(doc.name(), namespace);
      return doc.equals(roundTrip);
    });
  }

  private static DesignDocument oneExampleDocument() {
    return new DesignDocument("foo")
        .putView("a", "function (doc, meta) { emit(doc.city, doc.sales); }", "_sum")
        .putView("x", "function (doc, meta) { emit(doc.a, doc.b); }");
  }

  private static Set<DesignDocument> exampleDocuments() {
    return setOf(
        oneExampleDocument(),
        new DesignDocument("noViews"),
        new DesignDocument("bar")
            .putView("b", "function (doc, meta) { emit(doc.foo, doc.bar); }")
    );
  }
}
