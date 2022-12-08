/*
 * Copyright (c) 2020 Couchbase, Inc.
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
package com.couchbase.client.scala.query

import com.couchbase.client.core.service.ServiceType
import com.couchbase.client.scala.json.JsonObject
import com.couchbase.client.scala.kv.MutationState
import com.couchbase.client.scala.manager.collection.CollectionSpec
import com.couchbase.client.scala.query.QuerySpec.RequireMB50132
import com.couchbase.client.scala.util.ScalaIntegrationTest
import com.couchbase.client.scala.{Cluster, Collection, Scope, TestUtils}
import com.couchbase.client.test.{Capabilities, Flaky, IgnoreWhen, Util}
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api._

import scala.concurrent.duration._
import scala.util.{Failure, Success}

@Disabled @Flaky
@TestInstance(Lifecycle.PER_CLASS)
@IgnoreWhen(
  missesCapabilities = Array(Capabilities.QUERY, Capabilities.COLLECTIONS),
  clusterVersionIsBelow = RequireMB50132
)
class ScopeLevelQuerySpec extends ScalaIntegrationTest {

  private var cluster: Cluster   = _
  private var scope: Scope       = _
  private var coll: Collection   = _
  private var bucketName: String = _
  private val ScopeName          = "scopey_doo"
  private val CollectionName     = "coll"

  @BeforeAll
  def beforeAll(): Unit = {
    cluster = connectToCluster()
    val bucket = cluster.bucket(config.bucketname)
    bucket.waitUntilReady(WaitUntilReadyDefault)

    println("Creating scope and waiting for it to exist.")

    bucket.collections.createScope(ScopeName).get
    Util.waitUntilCondition(() => {
      val result    = bucket.collections.scopeExists(ScopeName)
      val allScopes = bucket.collections.getAllScopes().get

      println(s"Scope exists result: ${result}, all scopes = ${allScopes}")

      result.isSuccess && result.get
    })

    println("Creating collection and waiting for it to exist")

    val collSpec = CollectionSpec(CollectionName, ScopeName)
    bucket.collections.createCollection(collSpec).get
    Util.waitUntilCondition(() => {
      val result = bucket.collections.collectionExists(collSpec)
      result.isSuccess && result.get
    })

    scope = bucket.scope(ScopeName)
    coll = scope.collection(CollectionName)

    TestUtils.waitForService(bucket, ServiceType.QUERY)
    TestUtils.waitForIndexerToHaveKeyspace(cluster, config.bucketname())

    println("Waiting for primary index on collection to be created successfully")

    Util.waitUntilCondition(
      () => {
        val result =
          scope.query(s"""CREATE PRIMARY INDEX on `${CollectionName}`""")
        println(result)
        result.isSuccess
      },
      java.time.Duration.ofMinutes(3)
    )

    bucketName = config.bucketname()

    println("Ready to run tests")
  }

  @AfterAll
  def afterAll(): Unit = {
    cluster.disconnect()
  }

  @Disabled("Returns a 'queryport.expectedTimestamp from [127.0.0.1:9101]' error from query")
  @Test
  def scopeLevelQueryConsistentWith(): Unit = {
    val docId = TestUtils.docId()
    val mr    = coll.upsert(docId, JsonObject.create.put("foo", "bar")).get

    scope.query(
      s"""select * from `${CollectionName}`""",
      QueryOptions().scanConsistency(QueryScanConsistency.ConsistentWith(MutationState.from(mr)))
    ) match {
      case Success(result) =>
        assert(result.rows.size == 1)

      case Failure(err) => throw err
    }
  }

  @Test
  def scopeLevelQuerySleep(): Unit = {
    val docId = TestUtils.docId()
    val mr    = coll.upsert(docId, JsonObject.create.put("foo", "bar")).get

    // TODO this is a temporary solution as consistentWith should be used instead, but currently returns an error
    Thread.sleep(10000)

    scope.query(s"""select META().id from `${CollectionName}`""") match {
      case Success(result) =>
        println(result)
        assert(result.rowsAs[JsonObject].get.map(_.str("id")).toSet.contains(docId))

      case Failure(err) => throw err
    }
  }

  @Disabled("Returns a 'queryport.expectedTimestamp from [127.0.0.1:9101]' error from query")
  @Test
  def fullyQualifiedCollectionNameConsistentWith(): Unit = {
    val docId = TestUtils.docId()
    val mr    = coll.upsert(docId, JsonObject.create.put("foo", "bar")).get

    cluster.query(
      s"""select * from default:`${config.bucketname}`.`${ScopeName}`.`${CollectionName}`""",
      QueryOptions().scanConsistency(QueryScanConsistency.ConsistentWith(MutationState.from(mr)))
    ) match {
      case Success(result) =>
        assert(result.rows.size == 1)

      case Failure(err) => throw err
    }
  }

  @Test
  def fullyQualifiedCollectionNameSleep(): Unit = {
    val docId = TestUtils.docId()
    coll.upsert(docId, JsonObject.create.put("foo", "bar")).get

    // TODO this is a temporary solution as consistentWith should be used instead, but currently returns an error
    Thread.sleep(10000)

    cluster.query(
      s"""select META().id from default:`${config.bucketname}`.`${ScopeName}`.`${CollectionName}`"""
    ) match {
      case Success(result) =>
        println(result)
        assert(result.rowsAs[JsonObject].get.map(_.str("id")).toSet.contains(docId))

      case Failure(err) => throw err
    }
  }

}
