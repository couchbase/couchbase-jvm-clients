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

import java.util.UUID

import com.couchbase.client.core.error.{
  BucketExistsException,
  BucketNotFoundException,
  DocumentNotFoundException
}
import com.couchbase.client.scala.manager.bucket._
import com.couchbase.client.scala.util.ScalaIntegrationTest
import com.couchbase.client.scala.{Cluster, Collection}
import com.couchbase.client.test.Util.waitUntilThrows
import com.couchbase.client.test._
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertThrows, assertTrue}
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api._

import scala.util.{Failure, Success}

@TestInstance(Lifecycle.PER_CLASS)
@IgnoreWhen(clusterTypes = Array(ClusterType.MOCKED))
class BucketManagerSpec extends ScalaIntegrationTest {
  private var cluster: Cluster       = _
  private var buckets: BucketManager = _
  private var bucketName: String     = _

  @BeforeAll
  def setup(): Unit = {
    cluster = connectToCluster()
    buckets = cluster.buckets
    bucketName = ClusterAwareIntegrationTest.config().bucketname()
    val bucket = cluster.bucket(bucketName)
  }

  @AfterAll
  def tearDown(): Unit = {
    cluster.disconnect()
  }

  @Test
  def access(): Unit = {
    val buckets: BucketManager          = cluster.buckets
    val reactive: ReactiveBucketManager = cluster.reactive.buckets
    val async: AsyncBucketManager       = cluster.async.buckets
  }

  private def waitUntilHealthy(bucket: String): Unit = {
    Util.waitUntilCondition(() => {
      buckets.getBucket(bucket) match {
        case Success(value) => value.healthy
        case _              => false
      }
    })
  }

  private def waitUntilDropped(bucket: String): Unit = {
    Util.waitUntilCondition(() => {
      buckets.getBucket(bucket) match {
        case Failure(err: BucketNotFoundException) => true
        case _                                     => false
      }
    })
  }

  /**
    * This sanity test is kept intentionally vague on its assertions since it depends how the test-util decide
    * to setup the default bucket when the test is created.
    */
  @Test
  def getBucket(): Unit = {
    assertCreatedBucket(buckets.getBucket(bucketName).get)
  }

  /**
    * Since we don't know how many buckets are in the cluster when the test runs make sure it is at least one and
    * perform some basic assertions on them.
    */
  @Test
  def getAllBuckets(): Unit = {
    val allBucketSettings = buckets.getAllBuckets().get
    assertFalse(allBucketSettings.isEmpty)
    for (entry <- allBucketSettings) {
      if (entry.name == bucketName) assertCreatedBucket(entry)
    }
  }

  @Test
  def createAndDropBucketWithDefaults(): Unit = {
    val name: String = UUID.randomUUID.toString
    val bucket       = CreateBucketSettings(name, 100)
    buckets.create(bucket).get
    waitUntilHealthy(name)

    val found = buckets.getAllBuckets().get.find(_.name == name).get

    assert(!found.flushEnabled)
    assert(found.ramQuotaMB == 100)
    assert(found.numReplicas == 1)
    assert(found.replicaIndexes)
    assert(found.bucketType == BucketType.Couchbase)
    assert(found.ejectionMethod == EjectionMethod.ValueOnly)
    assert(found.maxTTL == 0)
    assert(found.compressionMode == CompressionMode.Passive)
    buckets.dropBucket(name)
    assertFalse(buckets.getAllBuckets().get.exists(_.name == name))
  }

  @Test
  @IgnoreWhen(replicasLessThan = 2)
  def createBucketWithCustomSettings(): Unit = {
    val name: String = UUID.randomUUID.toString
    val bucket = CreateBucketSettings(name, 110)
      .flushEnabled(false)
      .numReplicas(2)
      .replicaIndexes(false)
      .bucketType(BucketType.Couchbase)
      .ejectionMethod(EjectionMethod.FullEviction)
      .maxTTL(500)
      .compressionMode(CompressionMode.Off)
      .conflictResolutionType(ConflictResolutionType.Timestamp)

    buckets.create(bucket).get
    waitUntilHealthy(name)

    val found = buckets.getBucket(name).get

    buckets.dropBucket(name).get
    waitUntilDropped(name)

    assert(found.flushEnabled == bucket.flushEnabled.get)
    assert(found.ramQuotaMB == bucket.ramQuotaMB)
    assert(found.replicaIndexes == bucket.replicaIndexes.get)
    assert(found.bucketType == bucket.bucketType.get)
    assert(found.maxTTL == bucket.maxTTL.get)
    assert(found.ejectionMethod == bucket.ejectionMethod.get)
    assert(found.compressionMode == bucket.compressionMode.get)
  }

  @Test
  def flushBucket(): Unit = {
    val bucket                 = cluster.bucket(bucketName)
    val collection: Collection = bucket.defaultCollection
    val id: String             = UUID.randomUUID.toString
    collection.upsert(id, "value").get
    assert(collection.exists(id).get.exists)
    buckets.flushBucket(bucketName).get

    waitUntilThrows(classOf[DocumentNotFoundException], () => collection.get(id).get)
  }

  @Test
  def createShouldFailWhenPresent(): Unit = {
    assertThrows(
      classOf[BucketExistsException],
      () => {
        buckets.create(CreateBucketSettings(bucketName, 100)).get
        waitUntilHealthy(bucketName)
      }
    )
  }

  @Test
  def upsertShouldOverrideWhenPresent(): Unit = {
    val loaded: BucketSettings = buckets.getBucket(bucketName).get
    val newQuota: Int          = loaded.ramQuotaMB + 10
    val newSettings            = loaded.toCreateBucketSettings.ramQuotaMB(newQuota)
    buckets.updateBucket(newSettings).get
    Util.waitUntilCondition(() => {
      val modified: BucketSettings = buckets.getBucket(bucketName).get
      modified.ramQuotaMB == newQuota
    })
  }

  /**
    * Helper method to assert simple invariants for the bucket which has been created.
    */
  private def assertCreatedBucket(settings: BucketSettings): Unit = {
    assertEquals(bucketName, settings.name)
    assertTrue(settings.ramQuotaMB > 0)
  }

}
