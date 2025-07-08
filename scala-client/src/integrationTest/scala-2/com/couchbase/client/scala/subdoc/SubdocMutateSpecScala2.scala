/*
 * Copyright (c) 2025 Couchbase, Inc.
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
package com.couchbase.client.scala.subdoc

import com.couchbase.client.core.error.UnambiguousTimeoutException
import com.couchbase.client.scala.json.JsonObject
import com.couchbase.client.scala.kv.{MutateInOptions, StoreSemantics}
import com.couchbase.client.scala.kv.MutateInSpec
import com.couchbase.client.scala.util.ScalaIntegrationTest
import com.couchbase.client.scala.{Cluster, Collection, TestUtils}
import com.couchbase.client.test.{Capabilities, ClusterType, IgnoreWhen}
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test, TestInstance}

import scala.concurrent.duration._
import scala.util.{Failure, Try}

@TestInstance(Lifecycle.PER_CLASS)
@IgnoreWhen(clusterTypes = Array(ClusterType.MOCKED))
class SubdocMutateSpecScala2 extends ScalaIntegrationTest {
  private var cluster: Cluster = _
  private var coll: Collection = _

  @BeforeAll
  def beforeAll(): Unit = {
    cluster = connectToCluster()
    val bucket = cluster.bucket(config.bucketname)
    coll = bucket.defaultCollection
    bucket.waitUntilReady(WaitUntilReadyDefault)
  }

  @AfterAll
  def afterAll(): Unit = {
    cluster.disconnect()
  }

  @Test
  def create_as_deleted_on_non_existent_bucket_reactive(): Unit = {
    val docId        = TestUtils.docId()
    val doesNotExist = cluster.bucket("hokey_kokey")

    Try(
      doesNotExist.defaultCollection.reactive
        .mutateIn(
          docId,
          Seq(MutateInSpec.insert("txn", JsonObject.create).xattr),
          MutateInOptions()
            .timeout(1 second)
            .document(StoreSemantics.Upsert)
            .accessDeleted(true)
            .createAsDeleted(true)
        )
        .block()
    ) match {
      case Failure(_: UnambiguousTimeoutException) =>
      case x                                       => assert(false, s"Unexpected result $x")
    }
  }
}
