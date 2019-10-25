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

import java.util.concurrent.TimeUnit.SECONDS

import com.couchbase.client.core.error.RequestTimeoutException
import com.couchbase.client.scala.manager.query.{
  QueryIndex,
  QueryIndexAlreadyExistsException,
  QueryIndexManager,
  QueryIndexNotFoundException
}
import com.couchbase.client.scala.util.ScalaIntegrationTest
import com.couchbase.client.scala.{Cluster, Collection}
import com.couchbase.client.test._
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api._

import scala.concurrent.TimeoutException
import scala.concurrent.duration._
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

@TestInstance(Lifecycle.PER_CLASS)
@IgnoreWhen(clusterTypes = Array(ClusterType.MOCKED))
class QueryIndexManagerSpec extends ScalaIntegrationTest {
  private var cluster: Cluster           = _
  private var coll: Collection           = _
  private var bucketName: String         = _
  private var indexes: QueryIndexManager = _

  @BeforeAll
  def setup(): Unit = {
    cluster = connectToCluster()
    val bucket = cluster.bucket(config.bucketname)
    coll = bucket.defaultCollection
    bucketName = config.bucketname
    indexes = cluster.queryIndexes
  }

  @AfterAll
  def tearDown(): Unit = {
    cluster.disconnect()
  }

  @BeforeEach
  def cleanup() = {
    cluster.queryIndexes
      .getAllIndexes(config.bucketname)
      .get
      .foreach(index => cluster.queryIndexes.dropIndex(config.bucketname, index.name).get)
  }

  @Test
  def createDuplicatePrimaryIndex(): Unit = {
    cluster.queryIndexes.createPrimaryIndex(config.bucketname()).get

    cluster.queryIndexes.createPrimaryIndex(config.bucketname()) match {
      case Success(value)                                 => assert(false)
      case Failure(err: QueryIndexAlreadyExistsException) =>
      case Failure(err) =>
        assert(false)
    }

    cluster.queryIndexes.createPrimaryIndex(config.bucketname(), ignoreIfExists = true) match {
      case Success(value) =>
      case Failure(err)   => assert(false)
    }
  }

  @Test
  def createDuplicateSecondaryIndex(): Unit = {
    val indexName = "myIndex"
    val fields    = Seq("fieldA", "fieldB")

    cluster.queryIndexes.createIndex(config.bucketname(), indexName, fields)

    cluster.queryIndexes.createIndex(config.bucketname(), indexName, fields) match {
      case Success(value)                                 => assert(false)
      case Failure(err: QueryIndexAlreadyExistsException) =>
      case Failure(err) =>
        assert(false)
    }

    cluster.queryIndexes.createIndex(config.bucketname(), indexName, fields, ignoreIfExists = true) match {
      case Success(value) =>
      case Failure(err)   => assert(false)
    }
  }

  @Test
  def createPrimaryIndex(): Unit = {
    cluster.queryIndexes.createPrimaryIndex(config.bucketname(), numReplicas = Some(0)).get

    val index = getIndex("#primary").get
    assertTrue(index.isPrimary)

  }

  private def getIndex(name: String): Option[QueryIndex] = {
    cluster.queryIndexes
      .getAllIndexes(config.bucketname())
      .get
      .find(index => name == index.name)
  }

  @Test
  def createIndex(): Unit = {
    val indexName = "myIndex"
    val fields    = Seq("fieldB.foo", "`fieldB`.`bar`")

    cluster.queryIndexes.createIndex(config.bucketname(), indexName, fields)

    val index = getIndex(indexName).get

    assert(!index.isPrimary)
    assert(Set("(`fieldB`.`foo`)", "(`fieldB`.`bar`)") == index.indexKey.toSet)
  }

  @Test
  def dropPrimaryIndex() = {
    cluster.queryIndexes.dropPrimaryIndex(config.bucketname()) match {
      case Success(value)                            => assert(false)
      case Failure(err: QueryIndexNotFoundException) =>
      case Failure(err) =>
        assert(false)
    }

    cluster.queryIndexes.dropPrimaryIndex(config.bucketname(), ignoreIfNotExists = true).get

    cluster.queryIndexes.createPrimaryIndex(config.bucketname()).get

    cluster.queryIndexes.dropPrimaryIndex(config.bucketname()).get

    assert(cluster.queryIndexes.getAllIndexes(config.bucketname()).get.isEmpty)
  }

  @Test
  def dropIndex() = {
    cluster.queryIndexes.dropIndex(config.bucketname(), "foo") match {
      case Success(value)                            => assert(false)
      case Failure(err: QueryIndexNotFoundException) =>
      case Failure(err) =>
        assert(false)
    }

    cluster.queryIndexes.dropIndex(config.bucketname(), "foo", ignoreIfNotExists = true).get

    cluster.queryIndexes.createIndex(config.bucketname(), "foo", Seq("a", "b")).get

    cluster.queryIndexes.dropIndex(config.bucketname(), "foo").get

    assert(cluster.queryIndexes.getAllIndexes(config.bucketname()).get.isEmpty)
  }

  @Test
  def dropNamedPrimaryIndex() = {
    cluster.queryIndexes.createPrimaryIndex(config.bucketname, indexName = Some("namedPrimary"))

    assert(getIndex("namedPrimary").get.isPrimary)

    cluster.queryIndexes.dropIndex(config.bucketname(), "namedPrimary").get

    assertNoIndexesPresent()
  }

  private def assertNoIndexesPresent(): Unit = {
    assert(cluster.queryIndexes.getAllIndexes(config.bucketname).get.isEmpty)
  }

  @Test
  def buildZeroDeferredIndexes(): Unit = {
    cluster.queryIndexes.buildDeferredIndexes(config.bucketname)
  }

  @Test
  def buildOneDeferredIndex() = {
    createDeferredIndex("hyphenated-index-name")
    assert("deferred" == getIndex("hyphenated-index-name").get.state)

    cluster.queryIndexes.buildDeferredIndexes(config.bucketname)
    assertAllIndexesComeOnline(config.bucketname)

  }

  private def createDeferredIndex(indexName: String): Unit = {
    cluster.queryIndexes
      .createIndex(config.bucketname, indexName, Seq("someField"), deferred = Some(true))
      .get
  }

  private def createDeferredPrimaryIndex(indexName: String): Unit = {
    cluster.queryIndexes
      .createPrimaryIndex(config.bucketname, indexName = Some(indexName), deferred = Some(true))
      .get
  }

  private def assertAllIndexesComeOnline(bucketName: String): Unit = {
    val states = cluster.queryIndexes
      .getAllIndexes(bucketName)
      .get
      .map(_.state)
      .toSet
    if (states != Set("online")) assertAllIndexesComeOnline(bucketName)
  }

  private def assertAllIndexesAlreadyOnline(bucketName: String): Unit = {
    val states = cluster.queryIndexes
      .getAllIndexes(bucketName)
      .get
      .map(_.state)
      .toSet
    assert(states == Set("online"))
  }

  @Test
  def buildTwoDeferredIndexes(): Unit = {
    createDeferredIndex("indexOne")
    createDeferredIndex("indexTwo")
    assert("deferred" == getIndex("indexOne").get.state)
    assert("deferred" == getIndex("indexTwo").get.state)
    indexes.buildDeferredIndexes(bucketName).get
    assertAllIndexesComeOnline(bucketName)
  }

  @Test
  def buildDeferredIndexOnAbsentBucket(): Unit = { // It would be great to throw
    // BucketNotFoundException, but it's unclear how to detect that condition.
    indexes.buildDeferredIndexes("noSuchBucket").get
  }

  @Test
  def canWatchZeroIndexes(): Unit = {
    indexes.watchIndexes(bucketName, Seq(), 3.seconds).get
  }

  @Test
  def watchingAbsentIndexThrowsException(): Unit = {
    indexes.watchIndexes(bucketName, Seq("doesNotExist"), 3.seconds) match {
      case Success(value)                            => assert(false)
      case Failure(err: QueryIndexNotFoundException) =>
      case Failure(err)                              => assert(false)
    }
  }

  @Test
  def watchingAbsentPrimaryIndexThrowsException(): Unit = {
    indexes.watchIndexes(bucketName, Seq(), 3.seconds, watchPrimary = true) match {
      case Success(value)                            => assert(false)
      case Failure(err: QueryIndexNotFoundException) =>
      case Failure(err)                              => assert(false)
    }
  }

  @Test
  def canWatchAlreadyBuiltIndex(): Unit = {
    indexes.createIndex(bucketName, "myIndex", Seq("someField"))
    assertAllIndexesComeOnline(bucketName)
    indexes.watchIndexes(bucketName, Seq("myIndex"), 3.seconds)
  }

  @Test
  def watchTimesOutIfOneIndexStaysDeferred(): Unit = {
    indexes.createIndex(bucketName, "indexOne", Seq("someField"))
    indexes.watchIndexes(bucketName, Seq("indexOne"), 3.seconds)
    createDeferredIndex("indexTwo")
    indexes.watchIndexes(bucketName, Seq("indexOne", "indexTwo"), 0.seconds) match {
      case Success(value)                        => assert(false)
      case Failure(err: RequestTimeoutException) =>
      case Failure(err) =>
        println(err)
        assert(false)
    }
  }

  @Test
  def watchRetriesUntilIndexesComeOnline(): Unit = {
    createDeferredPrimaryIndex("indexOne")
    createDeferredIndex("indexTwo")
    createDeferredIndex("indexThree")

    new Thread(() => {
      try { // sleep first so the watch operation needs to poll more than once.
        SECONDS.sleep(1)
        println("Building indexes")
        indexes.buildDeferredIndexes(bucketName).get
        assertAllIndexesComeOnline(bucketName)
        println("All indexes online")
      } catch {
        case e: InterruptedException =>
          throw new RuntimeException(e)
        case NonFatal(e) =>
          println(s"Thread failed with ${e}")
      }
    }).start()
    val indexNames = Seq("indexOne", "indexTwo", "indexThree")

    // watchPrimary redundant, since the primary index was explicitly named; make sure it works anyway
    indexes.watchIndexes(bucketName, indexNames, 10.seconds, watchPrimary = true).get

    assertAllIndexesAlreadyOnline(bucketName)
  }

  @Test
  def getAllReactive(): Unit = {
    assert(cluster.reactive.queryIndexes.getAllIndexes(bucketName).collectSeq().block().isEmpty)

    cluster.reactive.queryIndexes.createPrimaryIndex(bucketName).block()

    assert(cluster.reactive.queryIndexes.getAllIndexes(bucketName).collectSeq().block().size == 1)
  }
}
