/*
 * Copyright (c) 2018 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"));
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

package com.couchbase.client.scala.diagnostics

import com.couchbase.client.core.diagnostics.{PingResult, PingState}
import com.couchbase.client.core.service.ServiceType
import com.couchbase.client.scala.util.ScalaIntegrationTest
import com.couchbase.client.scala.{Bucket, Cluster}
import com.couchbase.client.test.{ClusterAwareIntegrationTest, ClusterType, IgnoreWhen}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api._

import scala.jdk.CollectionConverters._
import scala.concurrent.duration._
@TestInstance(Lifecycle.PER_CLASS)
@IgnoreWhen(clusterTypes = Array(ClusterType.MOCKED))
class PingSpec extends ScalaIntegrationTest {
  private var cluster: Cluster   = _
  private var bucket: Bucket     = _
  private var bucketName: String = _

  @BeforeAll
  def setup(): Unit = {
    cluster = connectToCluster()
    bucketName = ClusterAwareIntegrationTest.config().bucketname()
    bucket = cluster.bucket(bucketName)
    bucket.waitUntilReady(10.seconds)
  }

  @AfterAll
  def tearDown(): Unit = {
    cluster.disconnect()
  }

  @Test
  def ping(): Unit = {
    val pr: PingResult = bucket.ping().get
    assert(!pr.endpoints().isEmpty)
    val psh = pr.endpoints().asScala(ServiceType.KV).get(0)
    assertEquals(PingState.OK, psh.state)
  }
}
