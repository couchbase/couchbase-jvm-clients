package com.couchbase.client.scala.subdoc

import com.couchbase.client.core.error.subdoc.PathNotFoundException
import com.couchbase.client.core.error.{DocumentDoesNotExistException, TemporaryLockFailureException}
import com.couchbase.client.scala.{Cluster, TestUtils}
import com.couchbase.client.scala.api.LookupInSpec
import org.scalatest.FunSuite

import scala.concurrent.duration._
import scala.util.{Failure, Success}

class SubdocGetSpec extends FunSuite {
  // TODO support Jenkins
  val (cluster, bucket, coll) = (for {
    cluster <- Cluster.connect("localhost", "Administrator", "password")
    bucket <- cluster.bucket("default")
    coll <- bucket.defaultCollection()
  } yield (cluster, bucket, coll)) match {
    case Success(result) => result
    case Failure(err) => throw err
  }



  test("lookupIn") {
    val docId = TestUtils.docId()
    coll.remove(docId)
    val content = ujson.Obj("hello" -> "world",
      "foo" -> "bar",
      "age" -> 22)
    val insertResult = coll.insert(docId, content).get

    coll.lookupIn(docId, LookupInSpec.get("foo").get("age")) match {
      case Success(result) =>
        assert(result.cas != 0)
        assert(result.cas == insertResult.cas)
        assert(result.contentAs[String]("foo").get == "bar")
        assert(result.documentAsBytes.isEmpty)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }


  test("path does not exist single") {
    val docId = TestUtils.docId()
    coll.remove(docId)
    val content = ujson.Obj("hello" -> "world")
    val insertResult = coll.insert(docId, content).get

    coll.lookupIn(docId, LookupInSpec.get("not_exist")) match {
      case Success(result) => assert(false, s"should not succeed")
      case Failure(err: PathNotFoundException) =>
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }


  test("path does not exist multi") {
    val docId = TestUtils.docId()
    coll.remove(docId)
    val content = ujson.Obj("hello" -> "world")
    val insertResult = coll.insert(docId, content).get

    coll.lookupIn(docId, LookupInSpec.get("not_exist").get("hello")) match {
      case Success(result) =>
        result.contentAs[String]("not_exist") match {
          case Success(body) => assert(false, s"should not succeed")
          case Failure(err: PathNotFoundException) =>
          case Failure(err) => assert(false, s"unexpected error $err")
        }
        assert(result.contentAs[String]("hello").get == "world")
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  test("lookupIn with doc") {
    val docId = TestUtils.docId()
    coll.remove(docId)
    val content = ujson.Obj("hello" -> "world",
      "foo" -> "bar",
      "age" -> 22)
    val insertResult = coll.insert(docId, content).get

    coll.lookupIn(docId, LookupInSpec.get("foo").get("age").getDoc) match {
      case Success(result) =>
        assert(result.contentAs[String]("foo").get == "bar")
        assert(result.documentAsBytes.isDefined)
        result.documentAs[ujson.Obj] match {
          case Success(body) =>
            assert(body("hello").str == "world")
            assert(body("age").num == 22)
          case Failure(err) => assert(false, s"unexpected error $err")
        }
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  // TODO this throws and shouldn't. need to implement single subdoc path. check old client AsyncLookupInBuilder
//  test("exists single") {
//    val docId = TestUtils.docId()
//    coll.remove(docId)
//    val content = ujson.Obj("hello" -> ujson.Arr("world"))
//    val insertResult = coll.insert(docId, content).get
//
//    coll.lookupIn(docId,
//      LookupInOps.exists("does_not_exist")) match {
//      case Success(result) =>
//        result.fieldAs[Boolean]("does_not_exist") match {
//          case Failure(err: PathNotFoundException) =>
//          case Success(v) => assert(false, s"should not succeed")
//          case Failure(err) => assert(false, s"unexpected error $err")
//        }
//        assert(result.fieldAs[Int]("hello").get == 1)
//      case Failure(err) => assert(false, s"unexpected error $err")
//    }
//  }

  test("exists multi") {
    val docId = TestUtils.docId()
    coll.remove(docId)
    val content = ujson.Obj("hello" -> ujson.Arr("world"),
      "foo" -> "bar",
      "age" -> 22)
    val insertResult = coll.insert(docId, content).get

    coll.lookupIn(docId,
      LookupInSpec.count("hello")
        .exists("age")
        .exists("does_not_exist")) match {
      case Success(result) =>
        assert(result.contentAs[Boolean]("age").get)
        result.contentAs[Boolean]("does_not_exist") match {
          case Failure(err: PathNotFoundException) =>
          case Success(v) => assert(false, s"should not succeed")
          case Failure(err) => assert(false, s"unexpected error $err")
        }
        result.contentAs[String]("age") match {
          case Failure(err: IllegalStateException) =>
          case Success(v) => assert(false, s"should not succeed")
          case Failure(err) => assert(false, s"unexpected error $err")
        }
        assert(result.contentAs[Int]("hello").get == 1)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }


  test("count") {
    val docId = TestUtils.docId()
    coll.remove(docId)
    val content = ujson.Obj("hello" -> ujson.Arr("world"),
      "foo" -> "bar",
      "age" -> 22)
    val insertResult = coll.insert(docId, content).get

    coll.lookupIn(docId,
      LookupInSpec.count("hello")
        .exists("age")
        .exists("does_not_exist")) match {
      case Success(result) =>
        assert(result.contentAs[Boolean]("age").get)
        result.contentAs[Boolean]("does_not_exist") match {
          case Failure(err: PathNotFoundException) =>
          case Success(v) => assert(false, s"should not succeed")
          case Failure(err) => assert(false, s"unexpected error $err")
        }
        result.contentAs[String]("age") match {
          case Failure(err: IllegalStateException) =>
          case Success(v) => assert(false, s"should not succeed")
          case Failure(err) => assert(false, s"unexpected error $err")
        }
        assert(result.contentAs[Int]("hello").get == 1)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }
}
