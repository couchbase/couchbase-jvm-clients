/*
 * Copyright 2024 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import com.couchbase.client.scala.transactions.error.{
  TransactionCommitAmbiguousException,
  TransactionFailedException
}
import com.couchbase.client.scala.{Cluster, Collection}
import com.couchbase.client.scala.util.ScalaIntegrationTest
import com.couchbase.client.test.{Capabilities, ClusterType, IgnoreWhen}
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.{BeforeAll, Test, TestInstance}
import org.junit.jupiter.api.TestInstance.Lifecycle

import java.util.UUID
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

/** All transactions testing is done by FIT.  However currently the Scala performer only supports testing the blocking
  * API, so we do some basic testing of the async and reactive APIs here. */
@IgnoreWhen(
  clusterTypes = Array(ClusterType.MOCKED),
  // Using COLLECTIONS as a proxy for 7.0, since some tests include query
  missesCapabilities = Array(Capabilities.COLLECTIONS)
)
@TestInstance(Lifecycle.PER_CLASS)
class TransactionsSpec extends ScalaIntegrationTest {
  private var cluster: Cluster = _
  private var coll: Collection = _

  @BeforeAll
  def beforeAll(): Unit = {
    cluster = connectToCluster()

    val bucket = cluster.bucket(config.bucketname)
    coll = bucket.defaultCollection
    bucket.waitUntilReady(WaitUntilReadyDefault)
  }

  @Test
  def asyncApiAllFeaturesNoWaiting(): Unit = {
    val docIdInsert    = UUID.randomUUID.toString
    val contentInsert  = JsonObject.create.put("content", "inserted")
    val docIdReplace   = UUID.randomUUID.toString
    val contentReplace = JsonObject.create.put("content", "replaced")
    val docIdRemove    = UUID.randomUUID.toString

    coll.upsert(docIdReplace, contentInsert)
    coll.upsert(docIdRemove, contentInsert)

    val future = cluster.async.transactions.run(
      ctx => {
        implicit val executionContext: ExecutionContext = ctx.executionContext

        // We intentionally start the futures without waiting for them, which isn't good programming practice, but acts
        // as a good stress test.
        ctx.insert(coll.async, docIdInsert, contentInsert)

        ctx
          .get(coll.async, docIdReplace)
          .map(docReplace => ctx.replace(docReplace, contentReplace))

        ctx
          .get(coll.async, docIdRemove)
          .map(docRemove => ctx.remove(docRemove))

        ctx
          .query("SELECT 'hello' as GREETING")
          .map(qr => assert(qr.metaData().metrics.get.resultCount == 1))
        // We use NONE to allow this to run on CI without hitting 'Synchronous Durability is currently not available on this bucket'
      },
      TransactionOptions().durabilityLevel(DurabilityLevel.NONE)
    )

    try {
      val result = Await.result(future, 5.seconds)
    } catch {
      case e: TransactionCommitAmbiguousException =>
        println(s"Transaction possibly reached commit point: ${e}")
      case e: TransactionFailedException => println(s"Transaction did not reach commit point: ${e}")
    }

    assert(coll.get(docIdInsert).get.contentAs[JsonObject].get == contentInsert)
    assert(coll.get(docIdReplace).get.contentAs[JsonObject].get == contentReplace)
    assert(coll.get(docIdRemove).isFailure)
  }

  @Test
  def asyncApiAllFeatures(): Unit = {
    val docIdInsert    = UUID.randomUUID.toString
    val contentInsert  = JsonObject.create.put("content", "inserted")
    val docIdReplace   = UUID.randomUUID.toString
    val contentReplace = JsonObject.create.put("content", "replaced")
    val docIdRemove    = UUID.randomUUID.toString

    coll.upsert(docIdReplace, contentInsert)
    coll.upsert(docIdRemove, contentInsert)

    val future: Future[TransactionResult] = cluster.async.transactions.run(ctx => {
      implicit val executionContext: ExecutionContext = ctx.executionContext

      for {
        _          <- ctx.insert(coll.async, docIdInsert, contentInsert)
        docReplace <- ctx.get(coll.async, docIdReplace)
        _          <- ctx.replace(docReplace, contentReplace)
        docRemove  <- ctx.get(coll.async, docIdRemove)
        _          <- ctx.remove(docRemove)
        qr         <- ctx.query("SELECT 'hello' as GREETING")
        _ = assert(qr.metaData().metrics.get.resultCount == 1)
      } yield ()
    })

    try {
      val result: TransactionResult = Await.result(future, 5.seconds)
    } catch {
      case e: TransactionCommitAmbiguousException =>
        println(s"Transaction possibly reached commit point: ${e}")
      case e: TransactionFailedException => println(s"Transaction did not reach commit point: ${e}")
    }

    assert(coll.get(docIdInsert).get.contentAs[JsonObject].get == contentInsert)
    assert(coll.get(docIdReplace).get.contentAs[JsonObject].get == contentReplace)
    assert(coll.get(docIdRemove).isFailure)
  }

  @Test
  def asyncApiFailure(): Unit = {
    val future = cluster.async.transactions.run(
      ctx => {
        implicit val executionContext: ExecutionContext = ctx.executionContext

        ctx
          .get(coll.async, "doc-not-found")
          .map(_ => ())
      },
      TransactionOptions().durabilityLevel(DurabilityLevel.NONE)
    )

    try {
      Await.result(future, 5.seconds)
    } catch {
      case e: TransactionFailedException =>
        e.getCause match {
          case _: DocumentNotFoundException =>
          case _                            => fail()
        }
      case _ =>
        fail()
    }
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
