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

package com.couchbase.client.scala.search

import com.couchbase.client.core.api.search.result.CoreSearchTermRange

import java.util.concurrent.TimeUnit
import com.couchbase.client.core.service.ServiceType
import com.couchbase.client.core.util.ConsistencyUtil
import com.couchbase.client.scala.json.JsonObject
import com.couchbase.client.scala.kv.MutationState
import com.couchbase.client.scala.manager.search.SearchIndex
import com.couchbase.client.scala.search.facet.SearchFacet
import com.couchbase.client.scala.search.queries.SearchQuery
import com.couchbase.client.scala.search.result.SearchFacetResult.{TermRange, TermSearchFacetResult}
import com.couchbase.client.scala.search.result.SearchResult
import com.couchbase.client.scala.util.ScalaIntegrationTest
import com.couchbase.client.scala.{Cluster, Collection, TestUtils}
import com.couchbase.client.test.{Capabilities, IgnoreWhen, Util}
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api._

import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

@IgnoreWhen(
  missesCapabilities = Array(Capabilities.SEARCH),
  clusterVersionIsBelow = ConsistencyUtil.CLUSTER_VERSION_MB_50101
)
@TestInstance(Lifecycle.PER_CLASS)
@Disabled // Michael N - disabled due to flakyness in the CI env
class SearchSpec extends ScalaIntegrationTest {

  private var cluster: Cluster  = _
  private var coll: Collection  = _
  private var ms: MutationState = _

  @BeforeAll
  def beforeAll(): Unit = {
    cluster = connectToCluster()
    val bucket = cluster.bucket(config.bucketname)
    coll = bucket.defaultCollection
    bucket.waitUntilReady(WaitUntilReadyDefault)
    TestUtils.waitForService(bucket, ServiceType.SEARCH)

    val result1 =
      coll
        .upsert(
          "test-1",
          JsonObject(
            "name"    -> "John Smith",
            "address" -> "123 Fake Street",
            "type"    -> "user",
            "age"     -> 32,
            "dob"     -> "2000-03-01T00:00:00"
          )
        )
        .get

    val result2 =
      coll
        .upsert(
          "test-2",
          JsonObject(
            "name"    -> "Richard Smith",
            "address" -> "123 Fake Street",
            "type"    -> "admin",
            "age"     -> 27,
            "dob"     -> "1995-03-01T00:00:00"
          )
        )
        .get

    ms = MutationState(Seq(result1.mutationToken.get, result2.mutationToken.get))

    val index = SearchIndex(indexName, config.bucketname)
    cluster.searchIndexes.upsertIndex(index).get
    ConsistencyUtil.waitUntilSearchIndexPresent(cluster.async.core, index.name)
    Util.waitUntilCondition(() => {
      val fetched = cluster.searchIndexes.getIndex(indexName).get
      fetched.numPlanPIndexes > 0
    })

    // Despite the above wait for numPlanPIndexes, still get "mismatched partition" errors intermittently, which
    // we try to workaround below by retrying tests
    // grahamp: at some point need to find a better solution, but spent enough time on it already. Advice from FTS team
    // is already implemented (poll until getIndex returns success).
  }

  private def indexName = "idx-" + config.bucketname

  @AfterAll
  def afterAll(): Unit = {
    cluster.searchIndexes.dropIndex(indexName)
    cluster.disconnect()
  }

  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  @Test
  def simple(): Unit = {
    def runTest(): Unit = {
      Thread.sleep(50)

      cluster.searchQuery(
        indexName,
        SearchQuery.matchPhrase("John Smith")
      ) match {
        case Success(result) =>
          println(result)

          if (result.metaData.errors.nonEmpty) {
            println("Running test again as errors")
            runTest()
          } else if (result.rows.size < 1) {
            println("Running test again as non enough rows")
            runTest()
          } else {
            result.metaData.errors.foreach(err => println(s"Err: ${err}"))
            println(s"Rows: ${result.rows}")
            assert(1 == result.rows.size)
          }
        case Failure(ex) =>
          println(ex.getMessage)
          println("Running test again as error")
          runTest()
      }
    }

    runTest()
  }

  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  @Test
  def consistentWith(): Unit = {
    def runTest(): Unit = {
      Thread.sleep(50)

      cluster.searchQuery(
        indexName,
        SearchQuery.matchPhrase("John Smith"),
        SearchOptions().scanConsistency(SearchScanConsistency.ConsistentWith(ms))
      ) match {
        case Success(result) =>
          println(result)

          if (result.metaData.errors.nonEmpty) {
            println("Running test again as errors")
            runTest()
          } else {
            result.metaData.errors.foreach(err => println(s"Err: ${err}"))
            println(s"Rows: ${result.rows}")
            assert(1 == result.rows.size)
          }
        case Failure(ex) =>
          println(ex.getMessage)
          println("Running test again as error")
          runTest()
      }
    }

    runTest()
  }

  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  @Test
  def facets(): Unit = {
    def runTest(): Unit = {
      Thread.sleep(50)

      cluster.searchQuery(
        indexName,
        SearchQuery.matchPhrase("Smith"),
        SearchOptions()
          .scanConsistency(SearchScanConsistency.ConsistentWith(ms))
          .facets(
            Map(
              "type_facet" -> SearchFacet.TermFacet("type", Some(10))
            )
          )
      ) match {
        case Success(result) =>
          println(result)
          println(result.facets)

          if (result.metaData.errors.nonEmpty) {
            println("Running test again as errors")
            runTest()
          } else if (result.facets.isEmpty) {
            println("Running test again as not enough facets")
            runTest()
          } else {
            result.facets("type_facet") match {
              case x: TermSearchFacetResult =>
                if (x.total == 2) {
                  assert(x.name == "type_facet")
                  assert(x.field == "type")
                  assert(x.total == 2)
                  assert(x.missing == 0)
                  assert(x.other == 0)
                  assert(
                    x.terms.toSet == Set(
                      TermRange(new CoreSearchTermRange("user", 1)),
                      TermRange(new CoreSearchTermRange("admin", 1))
                    )
                  )
                } else {
                  println("Running test again as not enough facet total")
                  runTest()
                }
              case _ => assert(false)
            }
          }

        case Failure(exception) =>
          println(exception)
          println("Running test again as error")
          runTest()
      }
    }

    runTest()
  }

  /** FTS testing on CI rarely runs smoothly, so retry the test until success
    * (or timeout, handled by Junit)
    */
  private def rerunIfNeeded(
      searchResult: Try[SearchResult],
      test: () => Unit,
      onSuccess: (SearchResult) => Unit
  ) = {

    val rerunTest: Boolean = searchResult match {
      case Success(result) =>
        println(result)

        if (result.metaData.errors.nonEmpty) {
          println("Running test again as errors")
          true
        } else if (result.rows.size < 1) {
          println("Running test again as not enough rows")
          true
        } else {
          result.metaData.errors.foreach(err => println(s"Err: ${err}"))
          println(s"Rows: ${result.rows}")
          assert(1 == result.rows.size)

          onSuccess(result)

          false
        }

      case Failure(ex) =>
        println(ex.getMessage)
        println("Running test again as error")
        true
    }

    if (rerunTest) {
      Thread.sleep(50)
      test()
    }
  }

  @Timeout(value = 1, unit = TimeUnit.MINUTES)
  // The score:none parameter was added in 6.6, so using CREATE_AS_DELETED support as a handy proxy for that cluster version
  @IgnoreWhen(missesCapabilities = Array(Capabilities.CREATE_AS_DELETED))
  @Test
  def disableScoring_shouldSucceedOnCluster_6_6_plus(): Unit = {
    def runTest(): Unit = {
      val result = cluster.searchQuery(
        indexName,
        SearchQuery.matchPhrase("John Smith"),
        SearchOptions().disableScoring(true)
      )

      rerunIfNeeded(result, runTest, (r: SearchResult) => {
        assert(r.rows.head.score <= 0.0001)
        assert(r.rows.head.score >= -0.0001)
      })
    }

    runTest()
  }

  @Timeout(value = 1, unit = TimeUnit.MINUTES)
  @IgnoreWhen(hasCapabilities = Array(Capabilities.CREATE_AS_DELETED))
  @Test
  def disableScoring_shouldBeGracefullyIgnoredOnCluster_6_5_minus(): Unit = {
    def runTest(): Unit = {
      val result = cluster.searchQuery(
        indexName,
        SearchQuery.matchPhrase("John Smith"),
        SearchOptions().disableScoring(true)
      )

      rerunIfNeeded(result, runTest, (r: SearchResult) => {
        assert(r.rows.head.score >= 0.0001)
      })
    }

    runTest()
  }
}
