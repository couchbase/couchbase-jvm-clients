package com.couchbase.client.scala

import com.couchbase.client.core.error.{DocumentDoesNotExistException, TemporaryLockFailureException}
import com.couchbase.client.scala.util.Validate
import org.scalatest.FunSuite

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

class GetFromReplicaSpec extends FunSuite {

    val cluster = Cluster.connect("localhost", "Administrator", "password")
    val bucket = cluster.bucket("default")
    val coll = bucket.defaultCollection


  private val reactive = coll.reactive
  private val async = coll.async

  test("any async") {
    val docId = TestUtils.docId()
    val content = ujson.Obj("hello" -> "world")

    // TODO get number of replicas from cluster and wait until they're all written, on all tests

    assert(coll.insert(docId, content).isSuccess)

    val future = async.getAnyReplica(docId)

      val result = Await.result(future, Duration.Inf)

      result.contentAs[ujson.Obj] match {
        case Success(body) => assert(body("hello").str == "world")
        case Failure(err) => assert(false, s"unexpected error $err")
      }
  }

  test("any blocking") {
    val docId = TestUtils.docId()
    val content = ujson.Obj("hello" -> "world")
    assert(coll.insert(docId, content).isSuccess)

    val result = coll.getAnyReplica(docId)

      assert(result.contentAs[ujson.Obj].get("hello").str == "world")
  }

  test("any reactive") {
    val docId = TestUtils.docId()
    val content = ujson.Obj("hello" -> "world")
    assert(coll.insert(docId, content).isSuccess)

    val results = reactive.getAnyReplica(docId)

    val result = results.block()
      assert(result.contentAs[ujson.Obj].get("hello").str == "world")
  }


  test("all async") {
    val docId = TestUtils.docId()
    val content = ujson.Obj("hello" -> "world")

    assert(coll.insert(docId, content).isSuccess)

    val results = async.getAllReplicas(docId)

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

    val results = coll.getAllReplicas(docId)

    for (result <- results) {
      assert(result.contentAs[ujson.Obj].get("hello").str == "world")
    }
  }

  test("all reactive") {
    val docId = TestUtils.docId()
    val content = ujson.Obj("hello" -> "world")
    assert(coll.insert(docId, content).isSuccess)

    val results = reactive.getAllReplicas(docId)

    results.doOnNext(result => {
      assert(result.contentAs[ujson.Obj].get("hello").str == "world")
    })
      .blockLast()
  }


}
