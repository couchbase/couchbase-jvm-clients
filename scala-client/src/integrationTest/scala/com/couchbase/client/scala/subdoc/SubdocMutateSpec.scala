package com.couchbase.client.scala.subdoc

import com.couchbase.client.core.error.subdoc.{MultiMutationException, PathNotFoundException, SubDocumentException}
import com.couchbase.client.core.msg.kv.SubDocumentOpResponseStatus
import com.couchbase.client.scala.api.{LookupInSpec, MutateInSpec}
import com.couchbase.client.scala.{Cluster, TestUtils}
import org.scalatest.FunSuite

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success}

class SubdocMutateSpec extends FunSuite {
  // TODO support Jenkins
  val (cluster, bucket, coll) = (for {
    cluster <- Cluster.connect("localhost", "Administrator", "password")
    bucket <- cluster.bucket("default")
    coll <- bucket.defaultCollection()
  } yield (cluster, bucket, coll)) match {
    case Success(result) => result
    case Failure(err) => throw err
  }


  def getContent(docId: String): ujson.Obj = {
    coll.get(docId) match {
      case Success(result) =>
        result.contentAs[ujson.Obj] match {
          case Success(content) =>
            content
          case Failure(err) =>
            assert(false, s"unexpected error $err")
            null
        }
      case Failure(err) =>
        assert(false, s"unexpected error $err")
        null
    }
  }

  def prepare(content: ujson.Value): (String, Long) = {
    val docId = TestUtils.docId()
    coll.remove(docId)
    val insertResult = coll.insert(docId, content).get
    (docId, insertResult.cas)
  }

  test("no commands") {
    val docId = TestUtils.docId()
    coll.mutateIn(docId, MutateInSpec.empty) match {
      case Success(result) => assert(false, s"unexpected success")
      case Failure(err: IllegalArgumentException) =>
      case Failure(err) =>
        assert(false, s"unexpected error $err")
    }
  }


  test("mutateIn insert string") {
    val content = ujson.Obj("hello" -> "world",
      "foo" -> "bar",
      "age" -> 22)
    val (docId, cas) = prepare(content)

    coll.mutateIn(docId, MutateInSpec.insert("foo2", "bar2")) match {
      case Success(result) => assert(result.cas != cas)
      case Failure(err) =>
        assert(false, s"unexpected error $err")
    }

    assert(getContent(docId)("foo2").str == "bar2")
  }

  // TODO test expand macros
  // TODO test createPath
  // TODO test xattrs
  // TODO test expiration
  // TODO test two commands

    test("mutateIn remove") {
      val content = ujson.Obj("hello" -> "world",
        "foo" -> "bar",
        "age" -> 22)
      val (docId, cas) = prepare(content)

      coll.mutateIn(docId, MutateInSpec.remove("foo")) match {
        case Success(result) => assert(result.cas != cas)
        case Failure(err) => assert(false, s"unexpected error $err")
      }

      assertThrows[NoSuchElementException](getContent(docId)("foo"))
    }


  private def checkSingleOpSuccess(content: ujson.Obj, ops: MutateInSpec) = {
    val (docId, cas) = prepare(content)

    coll.mutateIn(docId, ops) match {
      case Success(result) =>
        assert(result.cas != cas)
      case Failure(err) => assert(false, s"unexpected error $err")
    }

    coll.get(docId).get.contentAs[ujson.Obj].get
  }

  private def checkSingleOpFailure(content: ujson.Obj, ops: MutateInSpec, expected: SubDocumentOpResponseStatus) = {
    val (docId, cas) = prepare(content)

    coll.mutateIn(docId, ops) match {
      case Success(result) => assert(false, "should not succeed")
      case Failure(err: MultiMutationException) =>
        assert(err.firstFailureStatus() == expected)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  test("mutateIn insert string already there") {
    checkSingleOpFailure(ujson.Obj("foo" -> "bar"), MutateInSpec.insert("foo", "bar2"), SubDocumentOpResponseStatus.PATH_EXISTS)
  }


  // TODO get these working

  //  test("mutateIn insert bool") {
  //    val content = ujson.Obj()
  //    val (docId, cas) = prepare(content)
  //
  //    coll.mutateIn(docId, MutateInOps.insert("foo2", false)) match {
  //      case Success(result) => assert(result.cas != cas)
  //      case Failure(err) =>
  //        assert(false, s"unexpected error $err")
  //    }
  //
  //    assert(!getContent(docId)("foo2").bool)
  //  }
  //
  //  test("mutateIn insert int") {
  //    val content = ujson.Obj()
  //    val (docId, cas) = prepare(content)
  //
  //    coll.mutateIn(docId, MutateInOps.insert("foo2", 42)) match {
  //      case Success(result) => assert(result.cas != cas)
  //      case Failure(err) =>
  //        assert(false, s"unexpected error $err")
  //    }
  //
  //    assert(getContent(docId)("foo2").num == 42)
  //  }
  //
  //
  //  test("mutateIn insert double") {
  //    val content = ujson.Obj()
  //    val (docId, cas) = prepare(content)
  //
  //    coll.mutateIn(docId, MutateInOps.insert("foo2", 42.3)) match {
  //      case Success(result) => assert(result.cas != cas)
  //      case Failure(err) =>
  //        assert(false, s"unexpected error $err")
  //    }
  //
  //    assert(getContent(docId)("foo2").num == 42.3)
  //  }

  test("replace string") {
    val content = ujson.Obj("hello" -> "world",
      "foo" -> "bar",
      "age" -> 22)
    val (docId, cas) = prepare(content)

    coll.mutateIn(docId, MutateInSpec.replace("foo", "bar2")) match {
      case Success(result) => assert(result.cas != cas)
      case Failure(err) =>
        assert(false, s"unexpected error $err")
    }

    assert(getContent(docId)("foo").str == "bar2")
  }

  test("replace string does not exist") {
    checkSingleOpFailure(ujson.Obj(), MutateInSpec.replace("foo", "bar2"), SubDocumentOpResponseStatus.PATH_NOT_FOUND)
  }

  test("upsert string") {
    val updatedContent = checkSingleOpSuccess(ujson.Obj("foo" -> "bar"), MutateInSpec.upsert("foo", "bar2"))
    assert(updatedContent("foo").str == "bar2")
  }

  test("upsert string does not exist") {
    val updatedContent = checkSingleOpSuccess(ujson.Obj(), MutateInSpec.upsert("foo", "bar2"))
    assert(updatedContent("foo").str == "bar2")
  }

  test("array append") {
    val updatedContent = checkSingleOpSuccess(ujson.Obj("foo" -> ujson.Arr("hello")),
      MutateInSpec.arrayAppend("foo", "world"))
    assert(updatedContent("foo").arr.map(_.str) == ArrayBuffer("hello", "world"))
  }

  test("array prepend") {
    val updatedContent = checkSingleOpSuccess(ujson.Obj("foo" -> ujson.Arr("hello")),
      MutateInSpec.arrayPrepend("foo", "world"))
    assert(updatedContent("foo").arr.map(_.str) == ArrayBuffer("world", "hello"))
  }

  test("array insert") {
    val updatedContent = checkSingleOpSuccess(ujson.Obj("foo" -> ujson.Arr("hello", "world")),
      MutateInSpec.arrayInsert("foo[1]", "cruel"))
    assert(updatedContent("foo").arr.map(_.str) == ArrayBuffer("hello", "cruel", "world"))
  }

  test("array insert unique does not exist") {
    val updatedContent = checkSingleOpSuccess(ujson.Obj("foo" -> ujson.Arr("hello", "world")),
      MutateInSpec.arrayAddUnique("foo", "cruel"))
    assert(updatedContent("foo").arr.map(_.str) == ArrayBuffer("hello", "world", "cruel"))
  }

  test("array insert unique does exist") {
    val updatedContent = checkSingleOpFailure(ujson.Obj("foo" -> ujson.Arr("hello", "cruel", "world")),
      MutateInSpec.arrayAddUnique("foo", "cruel"),
      SubDocumentOpResponseStatus.PATH_EXISTS)
  }


    test("counter +5") {
      val updatedContent = checkSingleOpSuccess(ujson.Obj("foo" -> 10),
        MutateInSpec.increment("foo", 5))
      assert(updatedContent("foo").num == 15)
    }

  test("counter -5") {
    val updatedContent = checkSingleOpSuccess(ujson.Obj("foo" -> 10),
      MutateInSpec.decrement("foo", 3))
    assert(updatedContent("foo").num == 7)
  }
}
