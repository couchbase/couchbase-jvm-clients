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
import java.nio.charset.StandardCharsets

import com.couchbase.client.core.error._
import com.couchbase.client.core.service.ServiceType
import com.couchbase.client.scala.{Cluster, TestUtils}
import com.couchbase.client.scala.manager.analytics._
import com.couchbase.client.scala.util.ScalaIntegrationTest
import com.couchbase.client.test.{Capabilities, ClusterAwareIntegrationTest, IgnoreWhen}
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api._
import scala.concurrent.duration._

import scala.util.{Failure, Success}

@TestInstance(Lifecycle.PER_CLASS)
@IgnoreWhen(missesCapabilities = Array(Capabilities.ANALYTICS))
class AnalyticsIndexManagerSpec extends ScalaIntegrationTest {
  private var cluster: Cluster                 = _
  private var bucketName: String               = _
  private var analytics: AnalyticsIndexManager = _
  private val DataverseName                    = "integration-test-dataverse"
  private val DatasetName                      = "foo"
  private val IndexName                        = "bar"

  @BeforeAll
  def setup(): Unit = {
    cluster = connectToCluster()
    // Need to open a bucket until we have GCCCP support
    bucketName = ClusterAwareIntegrationTest.config().bucketname()
    val bucket = cluster.bucket(bucketName)
    bucket.waitUntilReady(30 seconds)
    TestUtils.waitForService(bucket, ServiceType.ANALYTICS)
    analytics = cluster.analyticsIndexes
  }

  @AfterAll
  def tearDown(): Unit = {
    analytics.dropDataverse(DataverseName) // ignore result
    cluster.disconnect()
  }

  @BeforeEach
  def beforeEach(): Unit = {
    val result = analytics.dropDataverse(DataverseName)

    result match {
      case Success(_)                               =>
      case Failure(err: DataverseNotFoundException) =>
      case Failure(err) =>
        assert(false)
    }

    analytics
      .getAllDatasets()
      .get
      .foreach(ds => analytics.dropDataset(ds.name, Some(ds.dataverseName)).get)

    analytics
      .getAllIndexes()
      .get
      .foreach(
        index => analytics.dropIndex(index.name, index.datasetName, Some(index.dataverseName)).get
      )
  }

  @Test
  def createDataverse(): Unit = {
    analytics.createDataverse(DataverseName).get
  }

  @Test
  def createDataverseTwice(): Unit = {
    analytics.createDataverse(DataverseName).get

    analytics.createDataverse(DataverseName) match {
      case Failure(err: DataverseExistsException) =>
      case _                                      => assert(false)
    }
  }

  @Test
  def createDataverseTwiceIgnore(): Unit = {
    analytics.createDataverse(DataverseName).get
    analytics.createDataverse(DataverseName, ignoreIfExists = true).get
  }

  @Test
  def dropDataverse(): Unit = {
    analytics.createDataverse(DataverseName).get
    analytics.dropDataverse(DataverseName).get
    analytics.dropDataverse(DataverseName) match {
      case Failure(err: DataverseNotFoundException) =>
      case _                                        => assert(false)
    }
    analytics.dropDataverse(DataverseName, ignoreIfNotExists = true).get
  }

  @Test
  def createDataset(): Unit = {
    analytics.createDataset(DatasetName, bucketName).get
  }

  @Test
  def createDatasetTwice(): Unit = {
    analytics.createDataset(DatasetName, bucketName).get
    analytics.createDataset(DatasetName, bucketName) match {
      case Failure(err: DatasetExistsException) =>
      case _                                    => assert(false)
    }
    analytics.createDataset(DatasetName, bucketName, ignoreIfExists = true).get
  }

  @Test
  def dropAbsentDataset(): Unit = {
    analytics.dropDataset(DatasetName) match {
      case Failure(err: DatasetNotFoundException) =>
      case _                                      => assert(false)
    }

    analytics.dropDataset(DatasetName, ignoreIfNotExists = true).get
  }

  @Test
  def dropDataset(): Unit = {
    analytics.createDataset(DatasetName, bucketName).get
    analytics.dropDataset(DatasetName).get

    assert(!analytics.getAllDatasets().get.exists(_.name == DatasetName))
  }

  @Test
  def createIndex(): Unit = {
    analytics.createDataverse(DataverseName).get
    analytics.createDataset(DatasetName, bucketName).get

    val fields = Map(
      "a" -> AnalyticsDataType.AnalyticsInt64,
      "b" -> AnalyticsDataType.AnalyticsDouble,
      "c" -> AnalyticsDataType.AnalyticsString
    )

    analytics.createIndex(IndexName, DatasetName, fields).get

    analytics.createIndex(IndexName, DatasetName, fields) match {
      case Failure(err: IndexExistsException) =>
      case _                                  => assert(false)
    }

    analytics.createIndex(IndexName, DatasetName, fields, ignoreIfExists = true).get
  }

  @Test
  def parse() = {
    val raw =
      """{ "DataverseName": "Default", "DatasetName": "foo", "DatatypeDataverseName": "Metadata", "DatatypeName": "AnyObject", "DatasetType": "INTERNAL", "GroupName": "Default.foo", "CompactionPolicy": "prefix", "CompactionPolicyProperties": [ { "Name": "max-mergable-component-size", "Value": "1073741824" }, { "Name": "max-tolerance-component-count", "Value": "5" } ], "InternalDetails": { "FileStructure": "BTREE", "PartitioningStrategy": "HASH", "PartitioningKey": [ [ "id" ] ], "PrimaryKey": [ [ "id" ] ], "Autogenerated": false, "KeySourceIndicator": [ 1 ] }, "Hints": [  ], "Timestamp": "Fri Nov 01 11:35:42 UTC 2019", "DatasetId": 101, "PendingOp": 0, "MetatypeDataverseName": "Metadata", "MetatypeName": "DCPMeta", "BucketDataverseName": "Default", "LinkName": "Local", "BucketName": "1e5a6afb-a7db-43a1-bed4-a3673a08f2be" }"""
    val ad = AnalyticsDataset.codec.deserialize(raw.getBytes(StandardCharsets.UTF_8)).get
    assert(ad.dataverseName == "Default")
  }
}
