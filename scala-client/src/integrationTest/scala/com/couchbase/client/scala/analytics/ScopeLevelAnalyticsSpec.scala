/*
 * Copyright (c) 2021 Couchbase, Inc.
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
package com.couchbase.client.scala.analytics

import com.couchbase.client.core.error.{DataverseExistsException, ScopeNotFoundException}
import com.couchbase.client.core.service.ServiceType
import com.couchbase.client.core.util.ConsistencyUtil
import com.couchbase.client.scala.json.JsonObject
import com.couchbase.client.scala.manager.analytics.AnalyticsIndexManager
import com.couchbase.client.scala.manager.collection.{CollectionManager, CollectionSpec}
import com.couchbase.client.scala.util.ScalaIntegrationTest
import com.couchbase.client.scala.{Cluster, Collection, Scope, TestUtils}
import com.couchbase.client.test.Util.waitUntilCondition
import com.couchbase.client.test.{Capabilities, IgnoreWhen, Util}
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api._

import java.util.UUID
import scala.concurrent.duration._
import scala.util.{Failure, Success}

@TestInstance(Lifecycle.PER_CLASS)
@IgnoreWhen(missesCapabilities = Array(Capabilities.ANALYTICS, Capabilities.COLLECTIONS))
class ScopeLevelAnalyticsSpec extends ScalaIntegrationTest {

  private var cluster: Cluster                     = _
  private var scope: Scope                         = _
  private var coll: Collection                     = _
  private var bucketName: String                   = _
  private val scopeName                            = "myScope" + randomString
  private val collectionName                       = "myCollection" + randomString
  private var analytics: AnalyticsIndexManager     = _
  private val dataset                              = "myDataset"
  private val index                                = "myIndex"
  private val dataverse                            = "myDataverse"
  private var collectionManager: CollectionManager = _
  private val FooContent                           = JsonObject.create.put("foo", "bar")
  private val DefaultContent                       = JsonObject.create.put("some", "stuff")

  @BeforeAll
  def beforeAll(): Unit = {
    cluster = connectToCluster()
    val bucket = cluster.bucket(config.bucketname)
    bucket.waitUntilReady(WaitUntilReadyDefault)

    println("Creating scope and waiting for it to exist..")

    bucket.collections.createScope(scopeName).get
    ConsistencyUtil.waitUntilScopePresent(cluster.async.core, bucket.name, scopeName)

    println("Creating collection and waiting for it to exist")

    val collSpec = CollectionSpec(collectionName, scopeName)
    bucket.collections.createCollection(collSpec).get
    ConsistencyUtil.waitUntilCollectionPresent(cluster.async.core, bucket.name, scopeName, collectionName)

    scope = bucket.scope(scopeName)
    coll = scope.collection(collectionName)

    TestUtils.waitForService(bucket, ServiceType.ANALYTICS)
    bucketName = config.bucketname()
    analytics = cluster.analyticsIndexes
    collectionManager = bucket.collections

    insertDoc(bucket.scope(scopeName).collection(collectionName), FooContent)
    insertDoc(bucket.defaultCollection, DefaultContent)

    println("Ready to run tests")
  }

  @AfterAll
  def afterAll(): Unit = {
    cluster.disconnect()
  }

  @BeforeEach
  def reset(): Unit = {
    dropAllDatasets()
    dropAllIndexes()
  }

  private def dropAllDatasets(): Unit = {
    analytics
      .getAllDatasets()
      .get
      .foreach(ds => analytics.dropDataset(ds.name, Some(ds.dataverseName)).get)
  }

  private def dropAllIndexes(): Unit = {
    analytics
      .getAllIndexes()
      .get
      .foreach(idx => analytics.dropIndex(idx.name, idx.datasetName, Some(idx.dataverseName)).get)
  }

  private def dataverseExists(cluster: Cluster, dataverse: String): Boolean = {
    val statement =
      s"""SELECT DataverseName FROM Metadata.`Dataverse` where DataverseName="$dataverse" """
    cluster.analyticsQuery(statement) match {
      case Failure(e: ScopeNotFoundException) => false
      case Success(value)                     => value.rows.nonEmpty
      case Failure(err)                       => throw err
    }
  }

  @Test
  def performsDataverseCollectionQueryWithQueryContext(): Unit = {
    val dataverseName = config.bucketname + "." + scopeName
    val r             = analytics.createDataverse(dataverseName)
    r match {
      case Failure(err: DataverseExistsException) =>
      case Failure(err)                           => throw err
      case Success(_)                             =>
    }
    waitUntilCondition(() => dataverseExists(cluster, dataverseName))

    cluster
      .analyticsQuery(
        s"ALTER COLLECTION `$bucketName`.`$scopeName`.`$collectionName` ENABLE ANALYTICS"
      )
      .get

    val scope = cluster.bucket(config.bucketname).scope(scopeName)

    // Cluster-level query
    {
      val st =
        s"""SELECT * FROM `$bucketName`.`$scopeName`.`$collectionName` where `$collectionName`.foo="bar" """
      cluster.analyticsQuery(st).get
    }

    // Scope-level query
    {
      val st = s"""SELECT * FROM `$collectionName` where `$collectionName`.foo="bar" """
      scope.analyticsQuery(st).get
    }
  }

  private def randomString: String = UUID.randomUUID.toString.substring(0, 10)

  /**
    * Inserts a document into the collection and returns the ID of it
    */
  def insertDoc(collection: Collection, content: JsonObject): String = {
    val id: String = UUID.randomUUID.toString
    collection.insert(id, content).get
    id
  }
}
