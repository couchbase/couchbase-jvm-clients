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
package com.couchbase.client.scala.manager

import java.util.concurrent.TimeUnit

import com.couchbase.client.core.error.{DesignDocumentNotFoundException, ViewServiceException}
import com.couchbase.client.core.service.ServiceType
import com.couchbase.client.scala.{Cluster, TestUtils}
import com.couchbase.client.scala.manager.view._
import com.couchbase.client.scala.util.ScalaIntegrationTest
import com.couchbase.client.scala.view.DesignDocumentNamespace
import com.couchbase.client.test._
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api._

import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

@TestInstance(Lifecycle.PER_CLASS)
@IgnoreWhen(clusterTypes = Array(ClusterType.MOCKED))
@Disabled("to be fixed in SCBC-161")
@ManagementApiTest
class ViewIndexManagerSpec extends ScalaIntegrationTest {
  private var cluster: Cluster        = _
  private var views: ViewIndexManager = _
  private var bucketName: String      = _
  private val Namespaces =
    Seq(DesignDocumentNamespace.Development, DesignDocumentNamespace.Production)
  private val OneExampleDesignDoc = DesignDocument("foo")
    .putView("a", View("function (doc, meta) { emit(doc.city, doc.sales); }", Some("_sum")))
    .putView("x", View("function (doc, meta) { emit(doc.a, doc.b); }"))
  private val ExampleDesignDocuments = Seq(
    OneExampleDesignDoc,
    DesignDocument("noViews"),
    DesignDocument("bar")
      .putView("b", View("function (doc, meta) { emit(doc.foo, doc.bar); }"))
  )

  @BeforeAll
  def setup(): Unit = {
    cluster = connectToCluster()
    bucketName = ClusterAwareIntegrationTest.config().bucketname()
    val bucket = cluster.bucket(bucketName)
    views = bucket.viewIndexes
    bucket.waitUntilReady(WaitUntilReadyDefault)
    TestUtils.waitForService(bucket, ServiceType.VIEWS)
  }

  @AfterAll
  def tearDown(): Unit = {
    cluster.disconnect()
  }

  @AfterEach
  def afterEach(): Unit = {
    Namespaces.foreach(namespace => {
      ExampleDesignDocuments.foreach(doc => {
        views.dropDesignDocument(doc.name, namespace) match {
          case Success(_)                                    =>
          case Failure(err: DesignDocumentNotFoundException) => // ignore
          case _                                             => assert(false)
        }
        waitUntilDesignDocDropped(doc.name, namespace)
      })
    })
  }

  private def waitUntilDesignDocPresent(name: String, namespace: DesignDocumentNamespace): Unit = {
    Util.waitUntilCondition(() => {
      views.getDesignDocument(name, namespace) match {
        case Success(_) => true
        case _          => false
      }
    })
  }

  private def waitUntilDesignDocDropped(name: String, namespace: DesignDocumentNamespace): Unit = {
    Util.waitUntilCondition(() => {
      views.getDesignDocument(name, namespace) match {
        case Failure(err: DesignDocumentNotFoundException) => true
        case _                                             => false
      }
    })
  }

  @Test
  def dropAbsentDesignDoc(): Unit = {
    val x = views.dropDesignDocument("doesNotExist", DesignDocumentNamespace.Development)

    x match {
      case Success(_)                                    => assert(false)
      case Failure(err: DesignDocumentNotFoundException) =>
      case _                                             => assert(false)
    }
  }

  @Test
  def publishAbsentDesignDoc(): Unit = {
    views.publishDesignDocument("doesNotExist") match {
      case Success(_)                                    => assert(false)
      case Failure(err: DesignDocumentNotFoundException) =>
      case _                                             => assert(false)
    }
  }

  @Test
  def getAbsentDesignDoc(): Unit = {
    views.getDesignDocument("doesNotExist", DesignDocumentNamespace.Development) match {
      case Success(_)                                    => assert(false)
      case Failure(err: DesignDocumentNotFoundException) =>
      case _                                             => assert(false)
    }
  }

  @Test
  def upsertBadSyntax(): Unit = {
    val invalid = DesignDocument("invalid").putView("x", View("not javascript"))

    views.upsertDesignDocument(invalid, DesignDocumentNamespace.Development) match {
      case Success(_) => assert(false)
      case Failure(err: ViewServiceException) =>
        assert(err.content.contains("invalid_design_document"))
      case _ => assert(false)
    }
  }

  @Test
  def getAllDesignDocuments(): Unit = {
    Namespaces.foreach(namespace => {
      ExampleDesignDocuments.foreach(doc => {
        views.upsertDesignDocument(doc, namespace).get
        waitUntilDesignDocPresent(doc.name, namespace)
      })
      val all = views.getAllDesignDocuments(namespace).get.toSet
      assert(ExampleDesignDocuments.toSet == all)
    })
  }

  @Test
  def getProd(): Unit = {
    views.upsertDesignDocument(OneExampleDesignDoc, DesignDocumentNamespace.Production).get
    waitUntilDesignDocPresent(OneExampleDesignDoc.name, DesignDocumentNamespace.Production)

    val all = views.getAllDesignDocuments(DesignDocumentNamespace.Production).get
    assert(all.contains(OneExampleDesignDoc))
  }

  @Test
  def upsertAndGet(): Unit = {
    views.upsertDesignDocument(OneExampleDesignDoc, DesignDocumentNamespace.Development).get
    waitUntilDesignDocPresent(OneExampleDesignDoc.name, DesignDocumentNamespace.Development)
    val doc =
      views.getDesignDocument(OneExampleDesignDoc.name, DesignDocumentNamespace.Development).get

    assert(doc == OneExampleDesignDoc)
  }
}
