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
    Util.waitUntilCondition(() => {
      val result = collections.scopeExists(scope)

      println(s"Waiting for scope ${scope} to exist, result: ${result}")

      result match {
        case Success(true) => true
        case _             => false
      }
    })
  }

  def waitForScopeToNotExist(scope: String) = {
    Util.waitUntilCondition(() => {
      val result = collections.scopeExists(scope)

      println(s"Waiting for scope ${scope} to not exist, result: ${result}")

      result match {
        case Success(false) => true
        case _              => false
      }
    })
  }

  def waitForCollectionToExist(collSpec: CollectionSpec): Unit = {
    Util.waitUntilCondition(() => {
      val result = collections.collectionExists(collSpec)

      println(s"Waiting for collection ${collSpec} to exist, result: ${result}")

      result match {
        case Success(true) => true
        case _             => false
      }
    })
  }

  def waitForCollectionToNotExist(collSpec: CollectionSpec): Unit = {
    Util.waitUntilCondition(() => {
      val result = collections.collectionExists(collSpec)

      println(s"Waiting for collection ${collSpec} to not exist, result: ${result}")

      result match {
        case Success(false) => true
        case _              => false
      }
    })
  }

  @Test
  def createScope(): Unit = {
    val scope = randomString

    assert(!collections.scopeExists(scope).get)

    collections.createScope(scope).get

    waitForScopeToExist(scope)
  }

  @Test
  def createCollection(): Unit = {
    val scope      = randomString
    val collection = randomString
    val collSpec   = CollectionSpec(collection, scope)

    assert(!collections.collectionExists(collSpec).get)

    collections.createCollection(collSpec) match {
      case Success(_)                           => assert(false)
      case Failure(err: ScopeNotFoundException) =>
      case Failure(_)                           => assert(false)
    }

    collections.createScope(scope).get
    collections.createCollection(collSpec).get

    waitForCollectionToExist(collSpec)
  }

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

  @Test
  def createCollectionTwice(): Unit = {
    val scope      = randomString
    val collection = randomString
    val collSpec   = CollectionSpec(collection, scope)

    collections.createScope(scope).get
    collections.createCollection(collSpec).get

    waitForScopeToExist(scope)
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
    collections.createCollection(collSpec).get

    waitForScopeToExist(scope)
    waitForCollectionToExist(collSpec)

    collections.dropCollection(collSpec).get

    waitForCollectionToNotExist(collSpec)

    collections.createCollection(collSpec).get
  }

  @Test
  def dropCollection_shouldFailWhen_collectionDoesNotExist(): Unit = {
    val scope = randomString
    collections.createScope(scope).get

    val result = collections.dropCollection(CollectionSpec("does_not_exist", scope))
    result match {
      case Failure(_: CollectionNotFoundException) =>
      case _                                       => assert(false, s"unexpected $result")
    }
  }

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
