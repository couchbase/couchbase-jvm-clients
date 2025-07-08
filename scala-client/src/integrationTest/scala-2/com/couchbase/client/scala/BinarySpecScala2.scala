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
package com.couchbase.client.scala

import com.couchbase.client.scala.kv.{DecrementOptions, IncrementOptions}
import com.couchbase.client.scala.util.ScalaIntegrationTest
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test, TestInstance}

@TestInstance(Lifecycle.PER_CLASS)
class BinarySpecScala2 extends ScalaIntegrationTest {

  private var cluster: Cluster       = _
  private var coll: BinaryCollection = _

  @BeforeAll
  def beforeAll(): Unit = {
    cluster = connectToCluster()
    val bucket = cluster.bucket(config.bucketname)
    coll = bucket.defaultCollection.binary
    bucket.waitUntilReady(WaitUntilReadyDefault)
  }

  @AfterAll
  def afterAll(): Unit = {
    cluster.disconnect()
  }

  @Test
  def blockingIncrementReactive(): Unit = {
    val docId  = TestUtils.docId()
    val result = coll.reactive.increment(docId, 3, IncrementOptions().initial(0)).block()
    assert(result.content == 0) // initial value returned
  }

  @Test
  def blockingDecrementReactive(): Unit = {
    val docId  = TestUtils.docId()
    val result = coll.reactive.decrement(docId, 3, DecrementOptions().initial(0)).block()
    assert(result.content == 0) // initial value returned
  }
}
