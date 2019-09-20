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
import com.couchbase.client.scala.util.ScalaIntegrationTest
import com.couchbase.client.scala.{Cluster, Collection}
import com.couchbase.client.test.{Capabilities, ClusterAwareIntegrationTest, IgnoreWhen}
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test, TestInstance}

import scala.util.{Failure, Success}

@IgnoreWhen(missesCapabilities = Array(Capabilities.SEARCH))
@TestInstance(Lifecycle.PER_CLASS)
class SearchSpec extends ScalaIntegrationTest {

  private var cluster: Cluster = _
  private var coll: Collection = _

  @BeforeAll
  def beforeAll(): Unit = {
    cluster = connectToCluster()
    val bucket = cluster.bucket(config.bucketname)
    coll = bucket.defaultCollection
  }

  @AfterAll
  def afterAll(): Unit = {
    cluster.shutdown()
  }

  @Test
  def simple() {
    cluster.searchQuery(SearchQuery("travel-sample-index-unstored",
      SearchQuery.queryString("united")).limit(10)) match {
      case Success(result) =>
        assert(result.errors.isEmpty)
        assert(10 == result.allRowsOrErrors.get.size)
      case Failure(exception) =>
        assert(false)
    }
  }
}
