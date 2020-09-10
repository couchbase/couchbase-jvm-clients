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
package com.couchbase.client.scala.query

import com.couchbase.client.core.service.ServiceType
import com.couchbase.client.scala.{Cluster, TestUtils}
import com.couchbase.client.scala.env.ClusterEnvironment
import com.couchbase.client.scala.json.JsonObject
import com.couchbase.client.scala.util.ScalaIntegrationTest
import com.couchbase.client.test.{Capabilities, IgnoreWhen}
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test, TestInstance}

@IgnoreWhen(missesCapabilities = Array(Capabilities.GLOBAL_CONFIG, Capabilities.QUERY))
@TestInstance(Lifecycle.PER_CLASS)
class ClusterLevelQuerySpec extends ScalaIntegrationTest {
  private var env: ClusterEnvironment = _
  private var cluster: Cluster        = _

  @BeforeAll
  def beforeAll(): Unit = {
    cluster = connectToCluster()
  }

  @AfterAll
  def afterAll(): Unit = {
    cluster.disconnect()
  }

  @Test
  def performsClusterLevelQueryWithoutOpenBucket(): Unit = {
    val result = cluster.query("select 1=1").get
    assert(1 == result.rowsAs[JsonObject].get.size)
  }
}
