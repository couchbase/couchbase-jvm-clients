package com.couchbase.client.scala

import com.couchbase.client.core.error.{DocumentDoesNotExistException, TemporaryLockFailureException}
import com.couchbase.client.scala.durability.ClientVerified
import com.couchbase.client.scala.util.Validate
import org.scalatest.FunSuite

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

class ReadFromReplicaSpec extends FunSuite {
  val (cluster, bucket, coll) = (for {
    cluster <- Cluster.connect("localhost", "Administrator", "password")
    bucket <- cluster.bucket("default")
    coll <- bucket.defaultCollection()
  } yield (cluster, bucket, coll)) match {
    case Success(result) => result
    case Failure(err) => throw err
  }

  private val reactive = coll.reactive
  private val async = coll.async

  test("any async") {
    val docId = TestUtils.docId()
    val content = ujson.Obj("hello" -> "world")

    // TODO get number of replicas from cluster and wait until they're all written, on all tests

    assert(coll.insert(docId, content).isSuccess)

    val results = async.getFromReplica(docId, ReplicaMode.Any)

    results.foreach(future => {
      val result = Await.result(future, Duration.Inf)

      result.contentAs[ujson.Obj] match {
        case Success(body) => assert(body("hello").str == "world")
        case Failure(err) => assert(false, s"unexpected error $err")
      }
    })
  }

  test("any blocking") {
    val docId = TestUtils.docId()
    val content = ujson.Obj("hello" -> "world")
    assert(coll.insert(docId, content).isSuccess)

    val results = coll.getFromReplica(docId, ReplicaMode.Any)

    for (result <- results) {
      assert(result.contentAs[ujson.Obj].get("hello").str == "world")
    }
  }

  test("any reactive") {
    val docId = TestUtils.docId()
    val content = ujson.Obj("hello" -> "world")
    assert(coll.insert(docId, content).isSuccess)

    val results = reactive.getFromReplica(docId, ReplicaMode.Any)

    results.doOnNext(result => {
      assert(result.contentAs[ujson.Obj].get("hello").str == "world")
    })
      .blockLast()
  }


  test("all async") {
    val docId = TestUtils.docId()
    val content = ujson.Obj("hello" -> "world")

    assert(coll.insert(docId, content).isSuccess)

    val results = async.getFromReplica(docId, ReplicaMode.All)

    results.foreach(future => {
      val result = Await.result(future, Duration.Inf)

      result.contentAs[ujson.Obj] match {
        case Success(body) => assert(body("hello").str == "world")
        case Failure(err) => assert(false, s"unexpected error $err")
      }
    })
  }

  test("all blocking") {
    val docId = TestUtils.docId()
    val content = ujson.Obj("hello" -> "world")
    assert(coll.insert(docId, content).isSuccess)

    val results = coll.getFromReplica(docId, ReplicaMode.All)

    for (result <- results) {
      assert(result.contentAs[ujson.Obj].get("hello").str == "world")
    }
  }

  test("all reactive") {
    val docId = TestUtils.docId()
    val content = ujson.Obj("hello" -> "world")
    assert(coll.insert(docId, content).isSuccess)

    val results = reactive.getFromReplica(docId, ReplicaMode.All)

    results.doOnNext(result => {
      assert(result.contentAs[ujson.Obj].get("hello").str == "world")
    })
      .blockLast()
  }

}
