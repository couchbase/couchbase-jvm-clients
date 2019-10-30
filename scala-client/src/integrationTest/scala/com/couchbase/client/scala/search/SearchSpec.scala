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

import com.couchbase.client.scala.env.ClusterEnvironment
import com.couchbase.client.scala.json.JsonObject
import com.couchbase.client.scala.kv.MutationState
import com.couchbase.client.scala.manager.search.{SearchIndex, SearchIndexNotFoundException}
import com.couchbase.client.scala.search.queries.{MatchAllQuery, SearchQuery}
import com.couchbase.client.scala.util.ScalaIntegrationTest
import com.couchbase.client.scala.{Cluster, Collection}
import com.couchbase.client.test.{Capabilities, ClusterAwareIntegrationTest, IgnoreWhen}
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test, TestInstance}

import scala.util.{Failure, Success}

@IgnoreWhen(missesCapabilities = Array(Capabilities.SEARCH))
@TestInstance(Lifecycle.PER_CLASS)
class SearchSpec extends ScalaIntegrationTest {

  private var cluster: Cluster  = _
  private var coll: Collection  = _
  private val IndexName         = "test-index"
  private var ms: MutationState = _

  @BeforeAll
  def beforeAll(): Unit = {
    cluster = connectToCluster()
    val bucket = cluster.bucket(config.bucketname)
    coll = bucket.defaultCollection

    val result =
      coll.insert("test", JsonObject("name" -> "John Smith", "address" -> "123 Fake Street")).get

    ms = MutationState(Seq(result.mutationToken.get))

    // The index may be pre-existing but on an old bucket that's been deleted.  Start again.
    cluster.searchIndexes.dropIndex(IndexName) match {
      case Success(_) =>
      case Failure(err: SearchIndexNotFoundException) =>
    }

    val index = SearchIndex(IndexName, config.bucketname)
    cluster.searchIndexes.upsertIndex(index).get

    var done = false
    while (!done) {
      val result = cluster.searchIndexes.getIndex(IndexName)
      if (result.isSuccess) done = true
      else Thread.sleep(100)
    }

    // grahamp: Above still doesn't seem enough to solve these errors:
    // Search Query Failed: "rest_index: Query, indexName: test-index, err: bleve: bleveIndexTargets, err: pindex: no planPIndexes for indexName: test-index"
    // Adding a sleep until a better solution presents itself.
    Thread.sleep(2000)
  }

  @AfterAll
  def afterAll(): Unit = {
    cluster.disconnect()
  }

  @Test
  def simple() {
    cluster.searchQuery(
      IndexName,
      SearchQuery.matchPhrase("John Smith"),
      SearchOptions().scanConsistency(SearchScanConsistency.ConsistentWith(ms))
    ) match {
      case Success(result) =>
        assert(result.errors.isEmpty)
        assert(1 == result.rows.size)
        result.rows.foreach(row => {
          val fields = row.fieldsAs[JsonObject]
          assert(fields.isFailure)
        })
      case Failure(exception) =>
        println(exception)
        assert(false)
    }
  }
}
