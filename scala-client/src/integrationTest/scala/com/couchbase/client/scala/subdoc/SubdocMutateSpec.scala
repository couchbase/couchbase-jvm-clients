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


  def prepareXattr(content: ujson.Value): (String, Long) = {
    val docId = TestUtils.docId()
    coll.remove(docId)
    val insertResult = coll.mutateIn(docId, MutateInSpec.insert("x", content, xattr = true), insertDocument = true).get
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

  // TODO test createPath
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

  private def checkSingleOpSuccessXattr(content: ujson.Obj, ops: MutateInSpec) = {
    val (docId, cas) = prepareXattr(content)

    coll.mutateIn(docId, ops) match {
      case Success(result) =>
        assert(result.cas != cas)
      case Failure(err) => assert(false, s"unexpected error $err")
    }

    coll.lookupIn(docId, LookupInSpec.get("x", xattr = true)).get.contentAs[ujson.Obj]("x").get
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

  private def checkSingleOpFailureXattr(content: ujson.Obj, ops: MutateInSpec, expected: SubDocumentOpResponseStatus) = {
    val (docId, cas) = prepareXattr(content)

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






  test("insert xattr") {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj(), MutateInSpec.insert("x.foo", "bar2", xattr = true))
    assert(updatedContent("foo").str == "bar2")
  }

  test("remove xattr") {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj("foo" -> "bar"), MutateInSpec.remove("x.foo", xattr = true))
    assertThrows[NoSuchElementException](updatedContent("foo"))
  }

  test("remove xattr does not exist") {
    checkSingleOpFailureXattr(ujson.Obj(), MutateInSpec.remove("x.foo", xattr = true), SubDocumentOpResponseStatus.PATH_NOT_FOUND)
  }

  test("insert string already there xattr") {
    checkSingleOpFailureXattr(ujson.Obj("foo" -> "bar"), MutateInSpec.insert("x.foo", "bar2", xattr = true), SubDocumentOpResponseStatus.PATH_EXISTS)
  }

  test("replace string xattr") {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj("foo" -> "bar"), MutateInSpec.replace("x.foo", "bar2", xattr = true))
    assert(updatedContent("foo").str == "bar2")
  }

  test("replace string does not exist xattr") {
    checkSingleOpFailure(ujson.Obj(), MutateInSpec.replace("x.foo", "bar2", xattr = true), SubDocumentOpResponseStatus.PATH_NOT_FOUND)
  }

  test("upsert string xattr") {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj("foo" -> "bar"), MutateInSpec.upsert("x.foo", "bar2", xattr = true))
    assert(updatedContent("foo").str == "bar2")
  }

  test("upsert string does not exist xattr") {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj(), MutateInSpec.upsert("x.foo", "bar2", xattr = true))
    assert(updatedContent("foo").str == "bar2")
  }

  test("array append xattr") {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj("foo" -> ujson.Arr("hello")),
      MutateInSpec.arrayAppend("x.foo", "world", xattr = true))
    assert(updatedContent("foo").arr.map(_.str) == ArrayBuffer("hello", "world"))
  }

  test("array prepend xattr") {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj("foo" -> ujson.Arr("hello")),
      MutateInSpec.arrayPrepend("x.foo", "world", xattr = true))
    assert(updatedContent("foo").arr.map(_.str) == ArrayBuffer("world", "hello"))
  }

  test("array insert xattr") {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj("foo" -> ujson.Arr("hello", "world")),
      MutateInSpec.arrayInsert("x.foo[1]", "cruel", xattr = true))
    assert(updatedContent("foo").arr.map(_.str) == ArrayBuffer("hello", "cruel", "world"))
  }

  test("array insert unique does not exist xattr") {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj("foo" -> ujson.Arr("hello", "world")),
      MutateInSpec.arrayAddUnique("x.foo", "cruel", xattr = true))
    assert(updatedContent("foo").arr.map(_.str) == ArrayBuffer("hello", "world", "cruel"))
  }

  test("array insert unique does exist xattr") {
    checkSingleOpFailureXattr(ujson.Obj("foo" -> ujson.Arr("hello", "cruel", "world")),
      MutateInSpec.arrayAddUnique("x.foo", "cruel", xattr = true),
      SubDocumentOpResponseStatus.PATH_EXISTS)
  }

  test("counter +5 xatr") {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj("foo" -> 10),
      MutateInSpec.increment("x.foo", 5, xattr = true))
    assert(updatedContent("foo").num == 15)
  }

  test("counter -5 xatr") {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj("foo" -> 10),
      MutateInSpec.decrement("x.foo", 3, xattr = true))
    assert(updatedContent("foo").num == 7)
  }



  test("insert expand macro xattr do not flag") {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj(), MutateInSpec.insert("x.foo", "${Mutation.CAS}", xattr = true))
    assert(updatedContent("foo").str == "${Mutation.CAS}")
  }

  test("insert expand macro xattr") {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj(), MutateInSpec.insert("x.foo", "${Mutation.CAS}", xattr = true, expandMacro = true))
    assert(updatedContent("foo").str != "${Mutation.CAS}")
  }


  
  test("insert xattr createPath") {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj(), MutateInSpec.insert("x.foo.baz", "bar2", xattr = true, createPath = true))
    assert(updatedContent("foo").obj("baz").str == "bar2")
  }

  test("insert string already there xattr createPath") {
    // TODO this should return PATH_EXISTS surely?  maybe another bug related to not doing single path
    checkSingleOpFailureXattr(ujson.Obj("foo" -> ujson.Obj("baz" -> "bar")), MutateInSpec.insert("x.foo.baz", "bar2"), SubDocumentOpResponseStatus.PATH_NOT_FOUND)
  }

  test("upsert string xattr createPath") {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj("foo" -> ujson.Obj("baz" -> "bar")), MutateInSpec.upsert("x.foo", "bar2", xattr = true, createPath = true))
    assert(updatedContent("foo").str == "bar2")
  }

  test("upsert string does not exist xattr createPath") {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj(), MutateInSpec.upsert("x.foo.baz", "bar2", xattr = true, createPath = true))
    assert(updatedContent("foo").obj("baz").str == "bar2")
  }

  test("array append xattr createPath") {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj(),
      MutateInSpec.arrayAppend("x.foo", "world", xattr = true, createPath = true))
    assert(updatedContent("foo").arr.map(_.str) == ArrayBuffer("world"))
  }

  test("array prepend xattr createPath") {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj(),
      MutateInSpec.arrayPrepend("x.foo", "world", xattr = true, createPath = true))
    assert(updatedContent("foo").arr.map(_.str) == ArrayBuffer("world"))
  }

  // TODO failing with bad input server error
  //  test("array insert xattr createPath") {
//    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj(),
//      MutateInSpec.arrayInsert("x.foo[0]", "cruel", xattr = true, createPath = true))
//    assert(updatedContent("foo").arr.map(_.str) == ArrayBuffer("cruel"))
//  }
//
//  test("array insert unique does not exist xattr createPath") {
//    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj(),
//      MutateInSpec.arrayAddUnique("x.foo", "cruel", xattr = true, createPath = true))
//    assert(updatedContent("foo").arr.map(_.str) == ArrayBuffer("hello", "world", "cruel"))
//  }


  test("counter +5 xattr createPath") {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj(),
      MutateInSpec.increment("x.foo", 5, xattr = true, createPath = true))
    assert(updatedContent("foo").num == 5)
  }

  test("counter -5 xattr createPath") {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj(),
      MutateInSpec.decrement("x.foo", 3, xattr = true, createPath = true))
    assert(updatedContent("foo").num == -3)
  }

}
