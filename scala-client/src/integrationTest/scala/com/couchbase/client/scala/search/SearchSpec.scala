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

import com.couchbase.client.scala.json.JsonObject
import com.couchbase.client.scala.kv.MutationState
import com.couchbase.client.scala.manager.search.SearchIndex
import com.couchbase.client.scala.search.queries.SearchQuery
import com.couchbase.client.scala.util.ScalaIntegrationTest
import com.couchbase.client.scala.{Cluster, Collection}
import com.couchbase.client.test.{Capabilities, IgnoreWhen, Util}
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api._

import scala.util.{Failure, Success}

@IgnoreWhen(missesCapabilities = Array(Capabilities.SEARCH))
@TestInstance(Lifecycle.PER_CLASS)
@Disabled // SCBC-156
class SearchSpec extends ScalaIntegrationTest {

  private var cluster: Cluster  = _
  private var coll: Collection  = _
  private var ms: MutationState = _

  @BeforeAll
  def beforeAll(): Unit = {
    cluster = connectToCluster()
    val bucket = cluster.bucket(config.bucketname)
    coll = bucket.defaultCollection

    val result =
      coll.insert("test", JsonObject("name" -> "John Smith", "address" -> "123 Fake Street")).get

    ms = MutationState(Seq(result.mutationToken.get))

    val index = SearchIndex(indexName, config.bucketname)
    cluster.searchIndexes.upsertIndex(index).get
  }

  private def indexName = "idx-" + config.bucketname

  @AfterAll
  def afterAll(): Unit = {
    cluster.searchIndexes.dropIndex(indexName)
    cluster.disconnect()
  }

  @Test
  def simple() {
    // The wait is to try and get around an issue on CI where the search service repeatedly returns
    // "pindex not available" errors
    Util.waitUntilCondition(
      () => {
        cluster.searchQuery(
          indexName,
          SearchQuery.matchPhrase("John Smith"),
          SearchOptions().scanConsistency(SearchScanConsistency.ConsistentWith(ms))
        ) match {
          case Success(result) =>
            result.metaData.errors.foreach(err => println(s"Err: ${err}"))
            println(s"Rows: ${result.rows}")
            1 == result.rows.size && result.rows.head.id == "test"
          case Failure(ex) =>
            println(ex.getMessage)
            Thread.sleep(500) // let's also sleep a bit longer in between to give the server
            // a chance to recover ...
            false
        }
      },
      Duration.ofMinutes(5)
    )
  }
}
