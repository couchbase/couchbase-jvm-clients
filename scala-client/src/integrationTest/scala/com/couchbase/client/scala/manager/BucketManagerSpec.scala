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
import java.util.concurrent.TimeUnit
import com.couchbase.client.core.error.{
  BucketExistsException,
  BucketNotFoundException,
  DocumentNotFoundException,
  InvalidArgumentException
}
import com.couchbase.client.scala.durability.Durability
import com.couchbase.client.scala.durability.Durability.Majority
import com.couchbase.client.scala.manager.bucket.BucketType.{Couchbase, Ephemeral}
import com.couchbase.client.scala.manager.bucket.EjectionMethod.{FullEviction, NotRecentlyUsed}
import com.couchbase.client.scala.manager.bucket._
import com.couchbase.client.scala.util.{CouchbasePickler, ScalaIntegrationTest}
import com.couchbase.client.scala.{Cluster, Collection}
import com.couchbase.client.test.Util.waitUntilThrows
import com.couchbase.client.test._
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertThrows, assertTrue}
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api._
import com.couchbase.client.core.error.FeatureNotAvailableException
import com.couchbase.client.core.util.ConsistencyUtil
import com.couchbase.client.test.Capabilities
import com.couchbase.client.test.IgnoreWhen

import scala.concurrent.duration.Duration
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
    bucket.waitUntilReady(WaitUntilReadyDefault)
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
    ConsistencyUtil.waitUntilBucketPresent(cluster.async.core, bucket)
    Util.waitUntilCondition(() => {
      buckets.getBucket(bucket) match {
        case Success(value) => value.healthy
        case _              => false
      }
    })
  }

  private def waitUntilDropped(bucket: String): Unit = {
    ConsistencyUtil.waitUntilBucketDropped(cluster.async.core, bucket)
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
  def parsing(): Unit = {
    assert(CompressionMode.Passive == CouchbasePickler.read[CompressionMode]("\"passive\""))
    assert(BucketType.Memcached == CouchbasePickler.read[BucketType]("\"memcached\""))
    assert(BucketType.Ephemeral == CouchbasePickler.read[BucketType]("\"ephemeral\""))
  }

  @Test
  def createAndDropBucketWithDefaults(): Unit = {
    val name: String = UUID.randomUUID.toString
    val bucket       = CreateBucketSettings(name, 100)
    val found        = createGetAndDestroy(name, bucket)

    assert(!found.flushEnabled)
    assert(found.ramQuotaMB == 100)
    assert(found.numReplicas == 1)
    assert(found.replicaIndexes)
    assert(found.bucketType == BucketType.Couchbase)
    assert(found.ejectionMethod == EjectionMethod.ValueOnly)
    assert(found.minimumDurabilityLevel == Durability.Disabled)
  }

  @Test
  @IgnoreWhen(missesCapabilities = Array(Capabilities.BUCKET_MINIMUM_DURABILITY))
  def createWithMinimumDurability(): Unit = {
    val name: String = UUID.randomUUID.toString

    val bucket = createGetAndDestroy(
      name,
      CreateBucketSettings(name, 100)
        .minimumDurabilityLevel(Durability.Majority)
        .numReplicas(0)
    )

    assert(bucket.minimumDurabilityLevel == Majority)
  }

  @Test
  def createEphemeral(): Unit = {
    val name: String = UUID.randomUUID.toString
    val bucket = CreateBucketSettings(
      name,
      100,
      bucketType = Some(Ephemeral),
      ejectionMethod = Some(NotRecentlyUsed)
    )
    val found = createGetAndDestroy(name, bucket)

    assert(found.bucketType == BucketType.Ephemeral)
    assert(found.ejectionMethod == EjectionMethod.NotRecentlyUsed)
  }

  @Test
  def createEphemeralWithUnsupportedEjectionMode(): Unit = {
    val name: String = UUID.randomUUID.toString
    val bucket = CreateBucketSettings(
      name,
      100,
      bucketType = Some(Ephemeral),
      ejectionMethod = Some(FullEviction)
    )
    buckets.create(bucket) match {
      case Failure(_: InvalidArgumentException) =>
    }
  }

  @Test
  def createCouchbaseWithUnsupportedEjectionMode(): Unit = {
    val name: String = UUID.randomUUID.toString
    val bucket = CreateBucketSettings(
      name,
      100,
      bucketType = Some(Couchbase),
      ejectionMethod = Some(NotRecentlyUsed)
    )
    buckets.create(bucket) match {
      case Failure(_: InvalidArgumentException) =>
    }
  }

  @Test
  def createDefaultWithUnsupportedEjectionMode(): Unit = {
    val name: String = UUID.randomUUID.toString
    val bucket = CreateBucketSettings(
      name,
      100,
      bucketType = Some(Couchbase),
      ejectionMethod = Some(NotRecentlyUsed)
    )
    buckets.create(bucket) match {
      case Failure(_: InvalidArgumentException) =>
    }
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

    val found: BucketSettings = createGetAndDestroy(name, bucket)

    assert(found.flushEnabled == bucket.flushEnabled.get)
    assert(found.ramQuotaMB == bucket.ramQuotaMB)
    assert(found.replicaIndexes == bucket.replicaIndexes.get)
    assert(found.bucketType == bucket.bucketType.get)
    assert(found.maxTTL == bucket.maxTTL.get)
    assert(found.ejectionMethod == bucket.ejectionMethod.get)
    assert(found.compressionMode == bucket.compressionMode.get)
  }

  @IgnoreWhen(missesCapabilities = Array(Capabilities.STORAGE_BACKEND))
  @Test
  def createCouchbaseBucketWithStorageBackendCouchstore(): Unit = {
    val name: String = UUID.randomUUID.toString
    val bucket = CreateBucketSettings(name, 110)
      .bucketType(BucketType.Couchbase)
      .storageBackend(StorageBackend.Couchstore)

    val found: BucketSettings = createGetAndDestroy(name, bucket)

    assert(found.storageBackend.contains(StorageBackend.Couchstore))
  }

  @IgnoreWhen(missesCapabilities = Array(Capabilities.STORAGE_BACKEND))
  @Test
  def createCouchbaseBucketWithStorageBackendDefault(): Unit = {
    val name: String = UUID.randomUUID.toString
    val bucket = CreateBucketSettings(name, 110)
      .bucketType(BucketType.Couchbase)

    val found: BucketSettings = createGetAndDestroy(name, bucket)

    assert(found.storageBackend.contains(StorageBackend.Couchstore))
  }

  @IgnoreWhen(missesCapabilities = Array(Capabilities.STORAGE_BACKEND))
  @Test
  def createCouchbaseBucketWithStorageBackendMagma(): Unit = {
    val name: String = UUID.randomUUID.toString
    val bucket = CreateBucketSettings(name, 110)
      .bucketType(BucketType.Couchbase)
      // Minimum RAM for Magma
      .ramQuotaMB(1024)
      .storageBackend(StorageBackend.Magma)

    val found: BucketSettings = createGetAndDestroy(name, bucket)

    assert(found.storageBackend.contains(StorageBackend.Magma))
  }

  private def createGetAndDestroy(name: String, bucket: CreateBucketSettings) = {
    buckets.create(bucket).get
    waitUntilHealthy(name)

    val found = buckets.getBucket(name).get

    buckets.dropBucket(name).get
    waitUntilDropped(name)
    found
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

  @Test
  @IgnoreWhen(hasCapabilities = Array(Capabilities.ENTERPRISE_EDITION))
  def createBucketWithCompressionModeShouldFailOnCE(): Unit = {
    val name = UUID.randomUUID.toString
    assertThrows(
      classOf[FeatureNotAvailableException],
      () =>
        buckets.create(CreateBucketSettings(name, 100).compressionMode(CompressionMode.Passive)).get
    )
  }

  @Test
  @IgnoreWhen(hasCapabilities = Array(Capabilities.ENTERPRISE_EDITION))
  def updateBucketWithCompressionModeShouldFailOnCE(): Unit = {
    val name = UUID.randomUUID.toString
    buckets.create(CreateBucketSettings(name, 100)).get
    waitUntilHealthy(name)
    val settings = buckets.getBucket(name).get
    assertThrows(
      classOf[FeatureNotAvailableException],
      () =>
        buckets
          .updateBucket(
            settings.copy(compressionMode = Some(CompressionMode.Passive)).toCreateBucketSettings
          )
          .get
    )
  }

  @Test
  @IgnoreWhen(missesCapabilities = Array(Capabilities.ENTERPRISE_EDITION))
  def createBucketWithCompressionModeShouldSucceedOnEE(): Unit = {
    val name = UUID.randomUUID.toString
    val settings = createGetAndDestroy(
      name,
      CreateBucketSettings(name, 100).compressionMode(CompressionMode.Passive)
    )

    assert(settings.maxTTL.isDefined)
    assert(settings.compressionMode.isDefined)
  }

  @Test
  @IgnoreWhen(missesCapabilities = Array(Capabilities.ENTERPRISE_EDITION))
  def updateBucketWithCompressionModeShouldSucceedOnEE(): Unit = {
    val name     = UUID.randomUUID.toString
    val settings = CreateBucketSettings(name, 100)
    buckets.create(settings).get
    waitUntilHealthy(name)

    buckets
      .updateBucket(
        settings.copy(compressionMode = Some(CompressionMode.Passive))
      )
      .get
  }
}
