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

import java.time.Duration
import java.util.concurrent.TimeUnit

import com.couchbase.client.scala.json.JsonObject
import com.couchbase.client.scala.kv.MutationState
import com.couchbase.client.scala.manager.search.SearchIndex
import com.couchbase.client.scala.search.facet.SearchFacet
import com.couchbase.client.scala.search.facet.SearchFacet.{DateRange, NumericRange}
import com.couchbase.client.scala.search.queries.SearchQuery
import com.couchbase.client.scala.search.result.SearchFacetResult.{
  DateRangeSearchFacetResult,
  NumericRangeSearchFacetResult,
  TermRange,
  TermSearchFacetResult
}
import com.couchbase.client.scala.util.ScalaIntegrationTest
import com.couchbase.client.scala.{Cluster, Collection}
import com.couchbase.client.test.{Capabilities, IgnoreWhen, Util}
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api._

import scala.util.{Failure, Success}

@IgnoreWhen(missesCapabilities = Array(Capabilities.SEARCH))
@TestInstance(Lifecycle.PER_CLASS)
class SearchSpec extends ScalaIntegrationTest {

  private var cluster: Cluster  = _
  private var coll: Collection  = _
  private var ms: MutationState = _

  @BeforeAll
  def beforeAll(): Unit = {
    cluster = connectToCluster()
    val bucket = cluster.bucket(config.bucketname)
    coll = bucket.defaultCollection

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
              case TermSearchFacetResult(name, field, total, missing, other, terms) =>
                if (total == 2) {
                  assert(name == "type_facet")
                  assert(field == "type")
                  assert(total == 2)
                  assert(missing == 0)
                  assert(other == 0)
                  assert(terms.toSet == Set(TermRange("user", 1), TermRange("admin", 1)))
                }
                else {
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

}
