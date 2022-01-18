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
import java.util.concurrent.TimeUnit.SECONDS
import com.couchbase.client.core.error.{IndexExistsException, IndexNotFoundException, TimeoutException}
import com.couchbase.client.core.service.ServiceType
import com.couchbase.client.scala.manager.query.{QueryIndex, QueryIndexManager}
import com.couchbase.client.scala.util.ScalaIntegrationTest
import com.couchbase.client.scala.{Cluster, Collection, TestUtils}
import com.couchbase.client.test._
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api._
import org.slf4j.{Logger, LoggerFactory}

import java.util.stream.Collectors.toSet
import scala.concurrent.duration._
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

// Disabling against 5.5.  See comment on QueryIndexManagerIntegrationTest for details.
@TestInstance(Lifecycle.PER_CLASS)
@IgnoreWhen(clusterTypes = Array(ClusterType.MOCKED), clusterVersionEquals = "5.5.6")
class QueryIndexManagerSpec extends ScalaIntegrationTest {
  private var cluster: Cluster           = _
  private var coll: Collection           = _
  private var bucketName: String         = _
  private var indexes: QueryIndexManager = _
  private val logger                     = LoggerFactory.getLogger(classOf[QueryIndexManagerSpec])

  @BeforeAll
  def setup(): Unit = {
    cluster = connectToCluster()
    val bucket = cluster.bucket(config.bucketname)
    coll = bucket.defaultCollection
    bucketName = config.bucketname
    indexes = cluster.queryIndexes

    bucket.waitUntilReady(Duration(30, TimeUnit.SECONDS))
    TestUtils.waitForService(bucket, ServiceType.QUERY)
    TestUtils.waitForIndexerToHaveBucket(cluster, config.bucketname())
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
      .foreach(index => {
        cluster.queryIndexes.dropIndex(config.bucketname, index.name).get
      })
  }

  def createPrimaryIndex(bucketName: String,
                         indexName: Option[String] = None,
                         numReplicas: Option[Int] = None,
                         deferred: Option[Boolean] = None) = {
    Util.waitUntilCondition(() => {
      val proceed = cluster.queryIndexes.createPrimaryIndex(bucketName, indexName, numReplicas = numReplicas, deferred = deferred) match {
        case Success(_) => true
        case Failure(_: IndexExistsException) => true
        case Failure(err) =>
          logger.warn(s"createPrimaryIndex reported ${err}")
          false
      }

      // Index must be online too, unless building a deferred index
      if (proceed) {
        if (deferred.contains(true)) true
        else {
          val i = indexes.getAllIndexes(bucketName).get
          logger.info(s"getAllIndexes reported ${i}")
          i.filter(v => v.isPrimary).map(v => v.state).toSet == Set("online")
        }
      }
      else false
    })
  }

  def createIndex(bucketName: String,
                  indexName: String,
                  fields: Iterable[String],
                  deferred: Option[Boolean] = None) = {
    Util.waitUntilCondition(() => {
      val proceed = cluster.queryIndexes.createIndex(bucketName, indexName, fields, deferred = deferred) match {
        case Success(_) => true
        case Failure(_: IndexExistsException) => true
        case Failure(err) =>
          logger.warn(s"createIndex reported ${err}")
          false
      }

      // Index must be online too, unless building a deferred index
      if (proceed) {
        if (deferred.contains(true)) true
        else {
          val i = indexes.getAllIndexes(bucketName).get
          logger.info(s"getAllIndexes reported ${i}")
          i.filter(v => v.name == indexName).map(v => v.state).toSet == Set("online")
        }
      }
      else false
    })
  }

  @Test
  def createDuplicatePrimaryIndex(): Unit = {
    createPrimaryIndex()

    Util.waitUntilCondition(() => {
      cluster.queryIndexes.createPrimaryIndex(config.bucketname()) match {
        case Success(_) =>
          assert(false)
          false
        case Failure(err: IndexExistsException) => true
        case Failure(err) =>
          logger.warn(s"createPrimaryIndex returned ${err}")
          false
      }
    })

    Util.waitUntilCondition(() => {
      cluster.queryIndexes.createPrimaryIndex(config.bucketname(), ignoreIfExists = true) match {
        case Success(_) => true
        case Failure(err) =>
          logger.info(s"createPrimaryIndex returned ${err}")
          false
      }
    })
  }

  @Test
  def createDuplicateSecondaryIndex(): Unit = {
    val indexName = "myIndex"
    val fields    = Seq("fieldA", "fieldB")

    createIndex(config.bucketname(), indexName, fields)

    Util.waitUntilCondition(() => {
      cluster.queryIndexes.createIndex(config.bucketname(), indexName, fields) match {
        case Success(value) =>
          assert(false)
          false
        case Failure(err: IndexExistsException) => true
        case Failure(err) =>
          logger.warn(s"createIndex returned ${err}")
          false
      }
    })

    Util.waitUntilCondition(() => {
      cluster.queryIndexes.createIndex(config.bucketname(), indexName, fields, ignoreIfExists = true) match {
        case Success(value) => true
        case Failure(err) =>
          logger.warn(s"createIndex returned ${err}")
          false
      }
    })
  }

  @Test
  def createPrimaryIndex(): Unit = {
    createPrimaryIndex(config.bucketname(), numReplicas = Some(0))

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

    createIndex(config.bucketname(), indexName, fields)

    val index = getIndex(indexName).get

    assert(!index.isPrimary)
    assert(Set("(`fieldB`.`foo`)", "(`fieldB`.`bar`)") == index.indexKey.toSet)
  }

  @Test
  def dropPrimaryIndex() = {
    cluster.queryIndexes.dropPrimaryIndex(config.bucketname()) match {
      case Success(value)                       => assert(false)
      case Failure(err: IndexNotFoundException) =>
      case Failure(err) =>
        assert(false)
    }

    cluster.queryIndexes.dropPrimaryIndex(config.bucketname(), ignoreIfNotExists = true).get

    createPrimaryIndex(config.bucketname())

    cluster.queryIndexes.dropPrimaryIndex(config.bucketname()).get

    assert(cluster.queryIndexes.getAllIndexes(config.bucketname()).get.isEmpty)
  }

  @Test
  def dropIndex() = {
    cluster.queryIndexes.dropIndex(config.bucketname(), "foo") match {
      case Success(value)                       => assert(false)
      case Failure(err: IndexNotFoundException) =>
      case Failure(err) =>
        logger.warn(err.toString)
        assert(false)
    }

    cluster.queryIndexes.dropIndex(config.bucketname(), "foo", ignoreIfNotExists = true).get

    createIndex(config.bucketname(), "foo", Seq("a", "b"))

    cluster.queryIndexes.dropIndex(config.bucketname(), "foo").get

    assert(cluster.queryIndexes.getAllIndexes(config.bucketname()).get.isEmpty)
  }

  @Test
  def dropNamedPrimaryIndex() = {
    createPrimaryIndex(config.bucketname, indexName = Some("namedPrimary"))

    assert(getIndex("namedPrimary").get.isPrimary)

    cluster.queryIndexes.dropIndex(config.bucketname(), "namedPrimary").get

    assertNoIndexesPresent()
  }

  private def assertNoIndexesPresent(): Unit = {
    assert(cluster.queryIndexes.getAllIndexes(config.bucketname).get.isEmpty)
  }

  @Test
  def buildZeroDeferredIndexes(): Unit = {
    cluster.queryIndexes.buildDeferredIndexes(config.bucketname).get
  }

  @Test
  def buildOneDeferredIndex() = {
    createDeferredIndex("hyphenated-index-name")
    assert("deferred" == getIndex("hyphenated-index-name").get.state)

    cluster.queryIndexes.buildDeferredIndexes(config.bucketname).get
    assertAllIndexesComeOnline(config.bucketname)

  }

  private def createDeferredIndex(indexName: String): Unit = {
    createIndex(config.bucketname, indexName, Seq("someField"), deferred = Some(true))
  }

  private def createDeferredPrimaryIndex(indexName: String): Unit = {
    createPrimaryIndex(config.bucketname, indexName = Some(indexName), deferred = Some(true))
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
      case Success(value)                       => assert(false)
      case Failure(err: IndexNotFoundException) =>
      case Failure(err)                         => assert(false)
    }
  }

  @Test
  def watchingAbsentPrimaryIndexThrowsException(): Unit = {
    indexes.watchIndexes(bucketName, Seq(), 3.seconds, watchPrimary = true) match {
      case Success(value)                       => assert(false)
      case Failure(err: IndexNotFoundException) =>
      case Failure(err)                         => assert(false)
    }
  }

  @Test
  def canWatchAlreadyBuiltIndex(): Unit = {
    createIndex(bucketName, "myIndex", Seq("someField"))
    indexes.watchIndexes(bucketName, Seq("myIndex"), 3.seconds).get
  }

  @Test
  def watchTimesOutIfOneIndexStaysDeferred(): Unit = {
    createIndex(bucketName, "indexOne", Seq("someField"))
    indexes.watchIndexes(bucketName, Seq("indexOne"), 3.seconds).get
    createDeferredIndex("indexTwo")
    indexes.watchIndexes(bucketName, Seq("indexOne", "indexTwo"), 0.seconds) match {
      case Success(value)                 => assert(false)
      case Failure(err: TimeoutException) =>
      case Failure(err) =>
        logger.warn(err.toString)
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
        indexes.buildDeferredIndexes(bucketName).get
        assertAllIndexesComeOnline(bucketName)
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
