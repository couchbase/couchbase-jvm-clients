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
package com.couchbase.client.scala.transactions

import com.couchbase.client.core.error.DocumentNotFoundException
import com.couchbase.client.core.msg.kv.DurabilityLevel
import com.couchbase.client.scala.json.JsonObject
import com.couchbase.client.scala.transactions.config.TransactionOptions
import com.couchbase.client.scala.transactions.error.TransactionFailedException
import com.couchbase.client.scala.util.ScalaIntegrationTest
import com.couchbase.client.scala.{Cluster, Collection}
import com.couchbase.client.test.{ClusterType, IgnoreWhen}
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test, TestInstance}

import java.util.UUID
import scala.util.Failure

@TestInstance(Lifecycle.PER_CLASS)
@IgnoreWhen(clusterTypes = Array(ClusterType.MOCKED))
class TransactionsSpecScala2 extends ScalaIntegrationTest {
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
  def reactiveApiAllFeatures(): Unit = {
    val docIdInsert    = UUID.randomUUID.toString
    val contentInsert  = JsonObject.create.put("content", "inserted")
    val docIdReplace   = UUID.randomUUID.toString
    val contentReplace = JsonObject.create.put("content", "replaced")
    val docIdRemove    = UUID.randomUUID.toString

    coll.upsert(docIdReplace, contentInsert)
    coll.upsert(docIdRemove, contentInsert)

    val mono = cluster.reactive.transactions.run(
      (ctx: ReactiveTransactionAttemptContext) => {
        ctx
          .insert(coll.reactive, docIdInsert, contentInsert)
          .`then`(
            ctx
              .get(coll.reactive, docIdReplace)
              .flatMap(docReplace => ctx.replace(docReplace, contentReplace))
          )
          .`then`(
            ctx
              .get(coll.reactive, docIdRemove)
              .map(docRemove => ctx.remove(docRemove))
          )
          .`then`(
            ctx
              .query("SELECT 'hello' as GREETING")
              .doOnNext(qr => assert(qr.metaData().metrics.get.resultCount == 1))
          )
          .`then`()
      },
      TransactionOptions().durabilityLevel(DurabilityLevel.NONE)
    )

    val result = mono.block()

    assert(coll.get(docIdInsert).get.contentAs[JsonObject].get == contentInsert)
    assert(coll.get(docIdReplace).get.contentAs[JsonObject].get == contentReplace)
    assert(coll.get(docIdRemove).isFailure)
  }

  @Test
  def reactiveApiFailure(): Unit = {
    val mono = cluster.reactive.transactions.run(
      (ctx: ReactiveTransactionAttemptContext) => {
        ctx
          .get(coll.reactive, "doc-not-found")
          .map(_ => ())
      },
      TransactionOptions().durabilityLevel(DurabilityLevel.NONE)
    )

    try {
      mono.block()
    } catch {
      case e: TransactionFailedException =>
        e.getCause match {
          case _: DocumentNotFoundException =>
          case _                            => fail()
        }
    }
  }
}
