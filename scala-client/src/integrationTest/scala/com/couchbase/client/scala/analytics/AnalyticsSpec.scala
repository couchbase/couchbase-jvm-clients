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
package com.couchbase.client.scala.analytics

import com.couchbase.client.core.error.{AnalyticsException, ParsingFailureException}
import com.couchbase.client.scala.json.{JsonObject, JsonObjectSafe}
import com.couchbase.client.scala.{Cluster, Collection}
import com.couchbase.client.scala.util.ScalaIntegrationTest
import com.couchbase.client.test.{Capabilities, IgnoreWhen}
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test, TestInstance}
import org.junit.jupiter.api.TestInstance.Lifecycle

import scala.util.{Failure, Success}

@TestInstance(Lifecycle.PER_CLASS)
@IgnoreWhen(missesCapabilities = Array(Capabilities.ANALYTICS))
class AnalyticsSpec extends ScalaIntegrationTest {

  private var cluster: Cluster   = _
  private var coll: Collection   = _
  private var bucketName: String = _

  @BeforeAll
  def beforeAll(): Unit = {
    cluster = connectToCluster()
    val bucket = cluster.bucket(config.bucketname)
    coll = bucket.defaultCollection
  }

  @AfterAll
  def afterAll(): Unit = {
    cluster.disconnect()
  }

  @Test
  def performsDataverseQuery(): Unit = {
    cluster.analyticsQuery("SELECT DataverseName FROM Metadata.`Dataverse`") match {
      case Success(result) =>
        val rows = result.rowsAs[JsonObjectSafe].get

        assert(rows.nonEmpty)
        rows.foreach(row => assert(row.get("DataverseName").isSuccess))

        val meta = result.metaData

        println(meta)

        assert(!meta.clientContextId.isEmpty)
        assert(meta.signatureAs[JsonObject].isSuccess)
        assert(!meta.requestId.isEmpty)
        assert(meta.status == AnalyticsStatus.Success)
        assert(meta.warnings.isEmpty)

      case Failure(err) => assert(false)
    }
  }

  @Test
  def failsOnError(): Unit = {
    cluster.analyticsQuery("SELECT 1=") match {
      case Success(result)                       => assert(false)
      case Failure(err: ParsingFailureException) =>
      case Failure(err)                          => assert(false)
    }
  }
}
