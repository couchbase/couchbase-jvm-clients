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

import com.couchbase.client.core.error.ViewServiceException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;

import static com.couchbase.client.core.util.CbCollections.setOf;
import static com.couchbase.client.java.manager.view.DesignDocumentNamespace.DEVELOPMENT;
import static com.couchbase.client.java.manager.view.DesignDocumentNamespace.PRODUCTION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@IgnoreWhen(clusterTypes = ClusterType.MOCKED)
class ViewManagerIntegrationTest extends JavaIntegrationTest {

  private static Cluster cluster;
  private static ClusterEnvironment environment;

  private static ViewIndexManager views;

  @BeforeAll
  static void setup() {
    environment = environment().build();
    cluster = Cluster.connect(environment);
    Bucket bucket = cluster.bucket(config().bucketname());
    views = bucket.viewIndexes();
  }

  @AfterAll
  static void tearDown() {
    cluster.shutdown();
    environment.shutdown();
  }

  @AfterEach
  void dropDesignDocs() {
    forEachNamespace(namespace ->
        views.getAllDesignDocuments(namespace).forEach(ddoc ->
            views.dropDesignDocument(ddoc.name(), namespace)));
  }

  @Test
  void upsertReplacesPreviousVersion() {
    forEachNamespace(namespace -> {
          exampleDocuments().forEach(doc -> {
            assertRoundTrip(doc, namespace);

            DesignDocument newVersion = new DesignDocument(doc.name(), doc.views())
                .putView("anotherView", "function (doc, meta) { emit(doc.foo, doc.bar); }");
            assertRoundTrip(newVersion, namespace);
          });
        }
    );
  }

  @Test
  void canDrop() {
    forEachNamespace(namespace -> {
          DesignDocument doc = oneExampleDocument();
          assertRoundTrip(doc, namespace);
          views.dropDesignDocument(doc.name(), namespace);
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
      assertEquals(docs, new HashSet<>(views.getAllDesignDocuments(namespace)));
    });
  }

  @Test
  void publish() {
    DesignDocument doc = oneExampleDocument();

    views.upsertDesignDocument(doc, DEVELOPMENT);
    views.publishDesignDocument(doc.name());

    assertEquals(doc, views.getDesignDocument(doc.name(), PRODUCTION));
  }

  private static void forEachNamespace(Consumer<DesignDocumentNamespace> consumer) {
    for (DesignDocumentNamespace namespace : DesignDocumentNamespace.values()) {
      consumer.accept(namespace);
    }
  }

  private static void assertRoundTrip(DesignDocument doc, DesignDocumentNamespace namespace) {
    views.upsertDesignDocument(doc, namespace);
    DesignDocument roundTrip = views.getDesignDocument(doc.name(), namespace);
    assertEquals(doc, roundTrip);
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
