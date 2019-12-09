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
  CollectionAlreadyExistsException,
  ScopeAlreadyExistsException,
  ScopeNotFoundException
}
import com.couchbase.client.scala.Cluster
import com.couchbase.client.scala.manager.collection._
import com.couchbase.client.scala.util.ScalaIntegrationTest
import com.couchbase.client.test._
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api._

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
    collections = bucket.collections
  }

  @AfterAll
  def tearDown(): Unit = {
    cluster.disconnect()
  }

  @Test
  def createScope(): Unit = {
    val scope = randomString

    assert(!collections.scopeExists(scope).get)

    collections.createScope(scope).get

    assert(collections.scopeExists(scope).get)
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

    assert(collections.collectionExists(collSpec).get)
  }

  @Test
  def createScopeTwice(): Unit = {
    val scope = randomString
    collections.createScope(scope).get

    collections.createScope(scope) match {
      case Success(_)                                => assert(false)
      case Failure(err: ScopeAlreadyExistsException) =>
      case Failure(_)                                => assert(false)
    }
  }

  @Test
  def createCollectionTwice(): Unit = {
    val scope      = randomString
    val collection = randomString
    val collSpec   = CollectionSpec(collection, scope)

    collections.createScope(scope).get
    collections.createCollection(collSpec).get

    collections.createCollection(collSpec) match {
      case Success(_)                                     => assert(false)
      case Failure(err: CollectionAlreadyExistsException) =>
      case Failure(_)                                     => assert(false)
    }
  }

  @Test
  def dropScope(): Unit = {
    val scope = randomString

    collections.createScope(scope).get
    collections.dropScope(scope).get

    assert(!collections.scopeExists(scope).get)

    collections.createScope(scope).get
  }

  @Test
  def dropCollection(): Unit = {
    val scope      = randomString
    val collection = randomString
    val collSpec   = CollectionSpec(collection, scope)

    collections.createScope(scope).get
    collections.createCollection(collSpec).get
    collections.dropCollection(collSpec).get

    assert(!collections.collectionExists(collSpec).get)

    collections.createCollection(collSpec).get
  }

  /**
    * Creates a random string in the right size for collections and scopeps which only support
    * up to 30 chars it seems.
    *
    * @return the random string to use
    */
  private def randomString = UUID.randomUUID.toString.substring(0, 10)

}
