package com.couchbase.client.scala

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.TimeUnit

import com.couchbase.client.core.error.DocumentNotFoundException
import com.couchbase.client.scala.kv.{
  DecrementOptions,
  GetOptions,
  IncrementOptions,
  InsertOptions,
  MutateInOptions,
  MutateInSpec,
  ReplaceOptions,
  UpsertOptions
}
import com.couchbase.client.scala.util.ScalaIntegrationTest
import com.couchbase.client.test.{ClusterType, IgnoreWhen}
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test, TestInstance}

import scala.concurrent.duration._
import scala.util.{Failure, Success}

/** Helper class for generically testing expiry. */
case class DocAndOperation(
    op: (String) => Unit,
    docId: String = TestUtils.docId(),
    upsertDocFirst: Boolean = true
)

/** Tests related to TTL/expiry on key-value operations. */
@TestInstance(Lifecycle.PER_CLASS)
@IgnoreWhen(clusterTypes = Array(ClusterType.MOCKED)) // Mock does not support expiry
class KeyValueExpirySpec extends ScalaIntegrationTest {

  private var cluster: Cluster = _
  private var coll: Collection = _

  @BeforeAll
  def beforeAll(): Unit = {
    cluster = connectToCluster()

    val bucket = cluster.bucket(config.bucketname)
    coll = bucket.defaultCollection
    bucket.waitUntilReady(Duration(30, TimeUnit.SECONDS))
  }

  @AfterAll
  def afterAll(): Unit = {
    cluster.disconnect()
  }

  @Test
  def insert_without_fetching_expiry(): Unit = {
    val docId = TestUtils.docId()

    val content = ujson.Obj("hello" -> "world")
    assert(coll.insert(docId, content, InsertOptions().expiry(5.seconds)).isSuccess)

    coll.get(docId) match {
      case Success(result) => assert(result.expiry.isEmpty)
      case Failure(err)    => assert(false, s"unexpected error $err")
    }
  }

  /** Expiry tests by their nature involve sleeps, and we don't want expiry tests to balloon the test runtime too much.
    * So combine the tests into one: perform setup for all tests, do one sleep, perform validation for all tests.
    *
    * These tests are inherently slightly racey, so we use long expiry windows of a few seconds to ensure reliability.
    */
  @Test
  def all_expiry_operations(): Unit = {
    val content        = ujson.Obj("hello" -> "world")
    val expiryDuration = 3.second
    val nearFuture     = Instant.now.plus(expiryDuration.toSeconds, ChronoUnit.SECONDS)

    val insertWithInstant = DocAndOperation(
      (docId) => assert(coll.insert(docId, content, InsertOptions().expiry(nearFuture)).isSuccess),
      upsertDocFirst = false
    )
    val insertWithDuration = DocAndOperation(
      (docId) =>
        assert(coll.insert(docId, content, InsertOptions().expiry(expiryDuration)).isSuccess),
      upsertDocFirst = false
    )

    val replaceWithInstant = DocAndOperation(
      (docId) => assert(coll.replace(docId, content, ReplaceOptions().expiry(nearFuture)).isSuccess)
    )
    val replaceWithDuration = DocAndOperation(
      (docId) =>
        assert(coll.replace(docId, content, ReplaceOptions().expiry(expiryDuration)).isSuccess)
    )

    val upsertWithInstant = DocAndOperation(
      (docId) => assert(coll.upsert(docId, content, UpsertOptions().expiry(nearFuture)).isSuccess)
    )
    val upsertWithDuration = DocAndOperation(
      (docId) =>
        assert(coll.upsert(docId, content, UpsertOptions().expiry(expiryDuration)).isSuccess)
    )

    val mutateInWithInstant = DocAndOperation(
      (docId) =>
        assert(
          coll
            .mutateIn(
              docId,
              Seq(MutateInSpec.upsert("foo", "bar").xattr),
              MutateInOptions().expiry(nearFuture)
            )
            .isSuccess
        )
    )
    val mutateInWithDuration = DocAndOperation(
      (docId) =>
        assert(
          coll
            .mutateIn(
              docId,
              Seq(MutateInSpec.upsert("foo", "bar").xattr),
              MutateInOptions().expiry(expiryDuration)
            )
            .isSuccess
        )
    )

    val incrementWithInstant = DocAndOperation(
      (docId) =>
        assert(
          coll.binary
            .increment(docId, 1, IncrementOptions().initial(0).expiry(nearFuture))
            .isSuccess
        ),
      upsertDocFirst = false
    )
    val incrementWithDuration = DocAndOperation(
      (docId) =>
        assert(
          coll.binary
            .increment(docId, 1, IncrementOptions().initial(0).expiry(expiryDuration))
            .isSuccess
        ),
      upsertDocFirst = false
    )

    val decrementWithInstant = DocAndOperation(
      (docId) =>
        assert(
          coll.binary
            .decrement(docId, 1, DecrementOptions().initial(0).expiry(nearFuture))
            .isSuccess
        ),
      upsertDocFirst = false
    )
    val decrementWithDuration = DocAndOperation(
      (docId) =>
        assert(
          coll.binary
            .decrement(docId, 1, DecrementOptions().initial(0).expiry(expiryDuration))
            .isSuccess
        ),
      upsertDocFirst = false
    )

    test_with_expiry(
      Seq(
        insertWithInstant,
        insertWithDuration,
        replaceWithInstant,
        replaceWithDuration,
        upsertWithInstant,
        upsertWithDuration,
        mutateInWithInstant,
        mutateInWithDuration,
        incrementWithInstant,
        incrementWithDuration,
        decrementWithInstant,
        decrementWithDuration
      )
    )
  }

  private def test_with_expiry(
      operations: Seq[DocAndOperation],
      sleepFor: Duration = 6.seconds
  ): Unit = {
    val content = ujson.Obj("hello" -> "world")

    // Execute all operations
    operations.foreach(operation => {
      if (operation.upsertDocFirst) {
        coll.upsert(operation.docId, content)
      }

      operation.op(operation.docId)

      // Immediately after, the doc should exist
      coll.get(operation.docId, GetOptions().withExpiry(true)) match {
        case Success(result) => assert(result.expiry.isDefined)
        case Failure(err)    => assert(false, s"unexpected error $err")
      }
    })

    Thread.sleep(sleepFor.toMillis)

    // After a sleep the doc should be gone
    operations.foreach(operation => {
      coll.get(operation.docId) match {
        case Failure(x: DocumentNotFoundException) =>
        case x                                     => assert(false, s"Unexpected result $x")
      }
    })
  }

}
