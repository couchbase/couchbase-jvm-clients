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
  CollectionExistsException,
  CollectionNotFoundException,
  ScopeExistsException,
  ScopeNotFoundException
}
import com.couchbase.client.core.service.ServiceType
import com.couchbase.client.core.util.ConsistencyUtil
import com.couchbase.client.scala.{Cluster, TestUtils}
import com.couchbase.client.scala.manager.collection._
import com.couchbase.client.scala.util.ScalaIntegrationTest
import com.couchbase.client.test._
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api._

import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

@TestInstance(Lifecycle.PER_CLASS)
@IgnoreWhen(missesCapabilities = Array(Capabilities.COLLECTIONS))
@ManagementApiTest
class CollectionManagerSpec extends ScalaIntegrationTest {
  private var cluster: Cluster               = _
  private var collections: CollectionManager = _
  private var bucketName: String             = _
  @BeforeAll
  def setup(): Unit = {
    cluster = connectToCluster()
    bucketName = ClusterAwareIntegrationTest.config().bucketname()
    val bucket = cluster.bucket(bucketName)
    bucket.waitUntilReady(WaitUntilReadyDefault)
    collections = bucket.collections
    TestUtils.waitForNsServerToBeReady(cluster)
  }

  @AfterAll
  def tearDown(): Unit = {
    cluster.disconnect()
  }

  def waitForScopeToExist(scope: String) = {
    ConsistencyUtil.waitUntilScopePresent(cluster.async.core, bucketName, scope)
  }

  def waitForScopeToNotExist(scope: String) = {
    ConsistencyUtil.waitUntilScopeDropped(cluster.async.core, bucketName, scope)
  }

  def waitForCollectionToExist(collSpec: CollectionSpec): Unit = {
    ConsistencyUtil.waitUntilCollectionPresent(
      cluster.async.core,
      bucketName,
      collSpec.scopeName,
      collSpec.name
    )
  }

  def waitForCollectionToNotExist(collSpec: CollectionSpec): Unit = {
    ConsistencyUtil.waitUntilCollectionDropped(
      cluster.async.core,
      bucketName,
      collSpec.scopeName,
      collSpec.name
    )
  }

  @Test
  def createScope(): Unit = {
    val scope = randomString

    collections.createScope(scope).get

    waitForScopeToExist(scope)
  }

  @IgnoreWhen(isProtostellarWillWorkLater = true) // Needs ING-358
  @Test
  def createCollection(): Unit = {
    val scope      = randomString
    val collection = randomString
    val collSpec   = CollectionSpec(collection, scope)

    collections.createCollection(collSpec) match {
      case Success(_)                           => assert(false)
      case Failure(err: ScopeNotFoundException) =>
      case Failure(_)                           => assert(false)
    }

    collections.createScope(scope).get
    waitForScopeToExist(scope)

    collections.createCollection(collSpec).get

    waitForCollectionToExist(collSpec)
  }

  @IgnoreWhen(isProtostellarWillWorkLater = true) // Needs ING-358
  @Test
  def createScopeTwice(): Unit = {
    val scope = randomString
    collections.createScope(scope).get

    waitForScopeToExist(scope)

    collections.createScope(scope) match {
      case Success(_)                         => assert(false)
      case Failure(err: ScopeExistsException) =>
      case Failure(_)                         => assert(false)
    }
  }

  @IgnoreWhen(isProtostellarWillWorkLater = true) // Needs ING-358
  @Test
  def createCollectionTwice(): Unit = {
    val scope      = randomString
    val collection = randomString
    val collSpec   = CollectionSpec(collection, scope)

    collections.createScope(scope).get

    waitForScopeToExist(scope)

    collections.createCollection(collSpec).get

    waitForCollectionToExist(collSpec)

    collections.createCollection(collSpec) match {
      case Success(_)                              => assert(false)
      case Failure(err: CollectionExistsException) =>
      case Failure(_)                              => assert(false)
    }
  }

  @Test
  def dropScope(): Unit = {
    val scope = randomString

    collections.createScope(scope).get

    waitForScopeToExist(scope)

    collections.dropScope(scope).get

    waitForScopeToNotExist(scope)

    collections.createScope(scope).get
  }

  @Test
  def dropCollection(): Unit = {
    val scope      = randomString
    val collection = randomString
    val collSpec   = CollectionSpec(collection, scope)

    collections.createScope(scope).get
    waitForScopeToExist(scope)

    collections.createCollection(collSpec).get

    waitForCollectionToExist(collSpec)

    collections.dropCollection(collSpec).get

    waitForCollectionToNotExist(collSpec)

    collections.createCollection(collSpec).get

    waitForCollectionToExist(collSpec)
  }

  @IgnoreWhen(isProtostellarWillWorkLater = true) // Needs ING-358
  @Test
  def dropCollection_shouldFailWhen_collectionDoesNotExist(): Unit = {
    val scope = randomString
    collections.createScope(scope).get
    waitForScopeToExist(scope)

    val result = collections.dropCollection(CollectionSpec("does_not_exist", scope))
    result match {
      case Failure(_: CollectionNotFoundException) =>
      case _                                       => assert(false, s"unexpected $result")
    }
  }

  @IgnoreWhen(isProtostellarWillWorkLater = true) // Needs ING-358
  @Test
  def dropScope_shouldFailWhen_scopeDoesNotExist(): Unit = {
    val result = collections.dropScope("does_not_exist")
    result match {
      case Failure(_: ScopeNotFoundException) =>
      case _                                  => assert(false, s"unexpected $result")
    }
  }

  /**
    * Creates a random string in the right size for collections and scopeps which only support
    * up to 30 chars it seems.
    *
    * @return the random string to use
    */
  private def randomString = UUID.randomUUID.toString.substring(0, 10)

}
