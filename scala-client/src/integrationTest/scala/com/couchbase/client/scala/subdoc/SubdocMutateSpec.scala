package com.couchbase.client.scala.subdoc

import com.couchbase.client.core.error.DocumentAlreadyExistsException
import com.couchbase.client.core.error.subdoc.{MultiMutationException, PathNotFoundException, SubDocumentException}
import com.couchbase.client.core.msg.kv.SubDocumentOpResponseStatus
import com.couchbase.client.scala.{Cluster, TestUtils}
import org.scalatest.FunSuite

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success}
import scala.concurrent.duration._
import com.couchbase.client.scala.kv.MutateInSpec._
import com.couchbase.client.scala.kv.LookupInSpec._
import com.couchbase.client.scala.kv.{DocumentCreation, MutateInMacro, MutateInSpec}

class SubdocMutateSpec extends FunSuite {

    val cluster = Cluster.connect("localhost", "Administrator", "password")
    val bucket = cluster.bucket("default")
    val coll = bucket.defaultCollection



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
    val insertResult = coll.mutateIn(docId, Array(insert("x", content).xattr), document = DocumentCreation.Insert).get
    (docId, insertResult.cas)
  }

  test("no commands") {
    val docId = TestUtils.docId()
    coll.mutateIn(docId, Array[MutateInSpec]()) match {
      case Success(result) => assert(false, s"unexpected success")
      case Failure(err: IllegalArgumentException) =>
      case Failure(err) =>
        assert(false, s"unexpected error $err")
    }
  }


  test("insert string") {
    val content = ujson.Obj("hello" -> "world",
      "foo" -> "bar",
      "age" -> 22)
    val (docId, cas) = prepare(content)

    coll.mutateIn(docId, Array(insert("foo2", "bar2"))) match {
      case Success(result) => assert(result.cas != cas)
      case Failure(err) =>
        assert(false, s"unexpected error $err")
    }

    assert(getContent(docId)("foo2").str == "bar2")
  }

  test("upsert existing doc") {
    val content = ujson.Obj("hello" -> "world",
      "foo" -> "bar",
      "age" -> 22)
    val (docId, cas) = prepare(content)

    coll.mutateIn(docId, Array(insert("foo2", "bar2")), document = DocumentCreation.Upsert) match {
      case Success(result) => assert(result.cas != cas)
      case Failure(err) =>
        assert(false, s"unexpected error $err")
    }

    assert(getContent(docId)("foo2").str == "bar2")
  }

  test("insert existing doc") {
    val content = ujson.Obj("hello" -> "world")
    val (docId, cas) = prepare(content)

    coll.mutateIn(docId, Array(insert("foo2", "bar2")), document = DocumentCreation.Insert) match {
      case Success(result) => assert(false)
      case Failure(err: DocumentAlreadyExistsException) =>
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  test("insert not-existing doc") {
    val docId = TestUtils.docId()

    coll.mutateIn(docId, Array(insert("foo2", "bar2")), document = DocumentCreation.Insert) match {
      case Success(result) =>
      case Failure(err) => assert(false, s"unexpected error $err")
    }

    assert(getContent(docId)("foo2").str == "bar2")
  }

  test("upsert not-existing doc") {
    val docId = TestUtils.docId()

    coll.mutateIn(docId, Array(insert("foo2", "bar2")), document = DocumentCreation.Upsert) match {
      case Success(result) =>
      case Failure(err) => assert(false, s"unexpected error $err")
    }

    assert(getContent(docId)("foo2").str == "bar2")
  }


  test("remove") {
    val content = ujson.Obj("hello" -> "world",
      "foo" -> "bar",
      "age" -> 22)
    val (docId, cas) = prepare(content)

    coll.mutateIn(docId, Array(remove("foo"))) match {
      case Success(result) => assert(result.cas != cas)
      case Failure(err) => assert(false, s"unexpected error $err")
    }

    assertThrows[NoSuchElementException](getContent(docId)("foo"))
  }


  private def checkSingleOpSuccess(content: ujson.Obj, ops: Seq[MutateInSpec]) = {
    val (docId, cas) = prepare(content)

    coll.mutateIn(docId, ops) match {
      case Success(result) =>
        assert(result.cas != cas)
      case Failure(err) => assert(false, s"unexpected error $err")
    }

    coll.get(docId).get.contentAs[ujson.Obj].get
  }

  private def checkSingleOpSuccessXattr(content: ujson.Obj, ops: Seq[MutateInSpec]) = {
    val (docId, cas) = prepareXattr(content)

    coll.mutateIn(docId, ops) match {
      case Success(result) =>
        assert(result.cas != cas)
      case Failure(err) => assert(false, s"unexpected error $err")
    }

    coll.lookupIn(docId, Array(get("x").xattr)).get.contentAs[ujson.Obj](0).get
  }

  private def checkSingleOpFailure(content: ujson.Obj, ops: Seq[MutateInSpec], expected: SubDocumentOpResponseStatus) = {
    val (docId, cas) = prepare(content)

    coll.mutateIn(docId, ops) match {
      case Success(result) => assert(false, "should not succeed")
      case Failure(err: MultiMutationException) =>
        assert(err.firstFailureStatus() == expected)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  private def checkSingleOpFailureXattr(content: ujson.Obj, ops: Seq[MutateInSpec], expected: SubDocumentOpResponseStatus) = {
    val (docId, cas) = prepareXattr(content)

    coll.mutateIn(docId, ops) match {
      case Success(result) => assert(false, "should not succeed")
      case Failure(err: MultiMutationException) =>
        assert(err.firstFailureStatus() == expected)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  test("insert string already there") {
    checkSingleOpFailure(ujson.Obj("foo" -> "bar"), Array(insert("foo", "bar2")), SubDocumentOpResponseStatus.PATH_EXISTS)
  }

  test("insert bool") {
    val content = ujson.Obj()
    val (docId, cas) = prepare(content)

    coll.mutateIn(docId, Array(insert("foo2", false))) match {
      case Success(result) => assert(result.cas != cas)
      case Failure(err) =>
        assert(false, s"unexpected error $err")
    }

    assert(!getContent(docId)("foo2").bool)
  }

  test("replace bool") {
    val updatedContent = checkSingleOpSuccess(ujson.Obj("hello" -> "world"), 
      Array(replace("hello", false)))
    assert(!updatedContent("hello").bool)
  }

  test("insert int") {
    val updatedContent = checkSingleOpSuccess(ujson.Obj(),
      Array(insert("hello", false)))
    assert(!updatedContent("hello").bool)
  }

  test("replace int") {
    val updatedContent = checkSingleOpSuccess(ujson.Obj("hello" -> "world"),
      Array(replace("hello", 42)))
    assert(updatedContent("hello").num == 42)
  }

  test("replace long") {
    val updatedContent = checkSingleOpSuccess(ujson.Obj("hello" -> "world"),
      Array(replace("hello", Long.MaxValue)))
    assert(updatedContent("hello").num == Long.MaxValue)
  }

  test("replace double") {
    val updatedContent = checkSingleOpSuccess(ujson.Obj("hello" -> "world"),
      Array(replace("hello", 42.3)))
    assert(updatedContent("hello").num == 42.3)
  }

  test("replace short") {
    val updatedContent = checkSingleOpSuccess(ujson.Obj("hello" -> "world"),
      Array(replace("hello", Short.MaxValue)))
    assert(updatedContent("hello").num == Short.MaxValue)
  }
  
  test("replace string") {
    val content = ujson.Obj("hello" -> "world",
      "foo" -> "bar",
      "age" -> 22)
    val (docId, cas) = prepare(content)

    coll.mutateIn(docId, Array(replace("foo", "bar2"))) match {
      case Success(result) => assert(result.cas != cas)
      case Failure(err) =>
        assert(false, s"unexpected error $err")
    }

    assert(getContent(docId)("foo").str == "bar2")
  }

  test("replace string does not exist") {
    checkSingleOpFailure(ujson.Obj(), Array(replace("foo", "bar2")), SubDocumentOpResponseStatus.PATH_NOT_FOUND)
  }

  test("upsert string") {
    val updatedContent = checkSingleOpSuccess(ujson.Obj("foo" -> "bar"), Array(upsert("foo", "bar2")))
    assert(updatedContent("foo").str == "bar2")
  }

  test("upsert string does not exist") {
    val updatedContent = checkSingleOpSuccess(ujson.Obj(), Array(upsert("foo", "bar2")))
    assert(updatedContent("foo").str == "bar2")
  }

  test("array append") {
    val updatedContent = checkSingleOpSuccess(ujson.Obj("foo" -> ujson.Arr("hello")),
      Array(arrayAppend("foo", "world")))
    assert(updatedContent("foo").arr.map(_.str) == ArrayBuffer("hello", "world"))
  }

  test("array prepend") {
    val updatedContent = checkSingleOpSuccess(ujson.Obj("foo" -> ujson.Arr("hello")),
      Array(arrayPrepend("foo", "world")))
    assert(updatedContent("foo").arr.map(_.str) == ArrayBuffer("world", "hello"))
  }

  test("array insert") {
    val updatedContent = checkSingleOpSuccess(ujson.Obj("foo" -> ujson.Arr("hello", "world")),
      Array(arrayInsert("foo[1]", "cruel")))
    assert(updatedContent("foo").arr.map(_.str) == ArrayBuffer("hello", "cruel", "world"))
  }

  test("array insert unique does not exist") {
    val updatedContent = checkSingleOpSuccess(ujson.Obj("foo" -> ujson.Arr("hello", "world")),
      Array(arrayAddUnique("foo", "cruel")))
    assert(updatedContent("foo").arr.map(_.str) == ArrayBuffer("hello", "world", "cruel"))
  }

  test("array insert unique does exist") {
    val updatedContent = checkSingleOpFailure(ujson.Obj("foo" -> ujson.Arr("hello", "cruel", "world")),
      Array(arrayAddUnique("foo", "cruel")),
      SubDocumentOpResponseStatus.PATH_EXISTS)
  }

  test("counter +5") {
    val updatedContent = checkSingleOpSuccess(ujson.Obj("foo" -> 10),
      Array(increment("foo", 5)))
    assert(updatedContent("foo").num == 15)
  }

  test("counter +5 returned") {
    val content = ujson.Obj("foo" -> 10)
    val (docId, cas) = prepare(content)

    coll.mutateIn(docId, Array(increment("foo", 5))) match {
      case Success(result) =>
        assert(result.contentAs[Long](0).get == 15)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  test("counter -5") {
    val updatedContent = checkSingleOpSuccess(ujson.Obj("foo" -> 10),
      Array(decrement("foo", 3)))
    assert(updatedContent("foo").num == 7)
  }


  test("insert xattr") {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj(), Array(insert("x.foo", "bar2").xattr))
    assert(updatedContent("foo").str == "bar2")
  }

  test("remove xattr") {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj("foo" -> "bar"), Array(remove("x.foo").xattr))
    assertThrows[NoSuchElementException](updatedContent("foo"))
  }

  test("remove xattr does not exist") {
    checkSingleOpFailureXattr(ujson.Obj(), Array(remove("x.foo").xattr), SubDocumentOpResponseStatus.PATH_NOT_FOUND)
  }

  test("insert string already there xattr") {
    checkSingleOpFailureXattr(ujson.Obj("foo" -> "bar"), Array(insert("x.foo", "bar2").xattr), SubDocumentOpResponseStatus.PATH_EXISTS)
  }

  test("replace string xattr") {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj("foo" -> "bar"), Array(replace("x.foo", "bar2").xattr))
    assert(updatedContent("foo").str == "bar2")
  }

  test("replace string does not exist xattr") {
    checkSingleOpFailure(ujson.Obj(), Array(replace("x.foo", "bar2").xattr), SubDocumentOpResponseStatus.PATH_NOT_FOUND)
  }

  test("upsert string xattr") {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj("foo" -> "bar"), Array(upsert("x.foo", "bar2").xattr))
    assert(updatedContent("foo").str == "bar2")
  }

  test("upsert string does not exist xattr") {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj(), Array(upsert("x.foo", "bar2").xattr))
    assert(updatedContent("foo").str == "bar2")
  }

  test("array append xattr") {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj("foo" -> ujson.Arr("hello")),
      Array(arrayAppend("x.foo", "world").xattr))
    assert(updatedContent("foo").arr.map(_.str) == ArrayBuffer("hello", "world"))
  }

  test("array prepend xattr") {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj("foo" -> ujson.Arr("hello")),
      Array(arrayPrepend("x.foo", "world").xattr))
    assert(updatedContent("foo").arr.map(_.str) == ArrayBuffer("world", "hello"))
  }

  test("array insert xattr") {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj("foo" -> ujson.Arr("hello", "world")),
      Array(arrayInsert("x.foo[1]", "cruel").xattr))
    assert(updatedContent("foo").arr.map(_.str) == ArrayBuffer("hello", "cruel", "world"))
  }

  test("array insert unique does not exist xattr") {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj("foo" -> ujson.Arr("hello", "world")),
      Array(arrayAddUnique("x.foo", "cruel").xattr))
    assert(updatedContent("foo").arr.map(_.str) == ArrayBuffer("hello", "world", "cruel"))
  }

  test("array insert unique does exist xattr") {
    checkSingleOpFailureXattr(ujson.Obj("foo" -> ujson.Arr("hello", "cruel", "world")),
      Array(arrayAddUnique("x.foo", "cruel").xattr),
      SubDocumentOpResponseStatus.PATH_EXISTS)
  }

  test("counter +5 xatr") {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj("foo" -> 10),
      Array(increment("x.foo", 5).xattr))
    assert(updatedContent("foo").num == 15)
  }

  test("counter -5 xatr") {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj("foo" -> 10),
      Array(decrement("x.foo", 3).xattr))
    assert(updatedContent("foo").num == 7)
  }


  test("insert expand macro xattr do not flag") {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj(), Array(insert("x.foo", "${Mutation.CAS}").xattr))
    assert(updatedContent("foo").str == "${Mutation.CAS}")
  }

  test("insert expand macro xattr") {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj(),
      Array(insert("x.foo", MutateInMacro.MutationCAS).xattr))
    assert(updatedContent("foo").str != "${Mutation.CAS}")
  }


  test("insert xattr createPath") {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj(), Array(insert("x.foo.baz", "bar2").xattr.createPath))
    assert(updatedContent("foo").obj("baz").str == "bar2")
  }

  test("insert string already there xattr createPath") {
    // TODO this should return PATH_EXISTS surely?  maybe another bug related to not doing single path
    checkSingleOpFailureXattr(ujson.Obj("foo" -> ujson.Obj("baz" -> "bar")), Array(insert("x.foo.baz", "bar2")), SubDocumentOpResponseStatus.PATH_NOT_FOUND)
  }

  test("upsert string xattr createPath") {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj("foo" -> ujson.Obj("baz" -> "bar")), Array(upsert("x.foo", "bar2").xattr.createPath))
    assert(updatedContent("foo").str == "bar2")
  }

  test("upsert string does not exist xattr createPath") {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj(), Array(upsert("x.foo.baz", "bar2").xattr.createPath))
    assert(updatedContent("foo").obj("baz").str == "bar2")
  }

  test("array append xattr createPath") {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj(),
      Array(arrayAppend("x.foo", "world").xattr.createPath))
    assert(updatedContent("foo").arr.map(_.str) == ArrayBuffer("world"))
  }

  test("array prepend xattr createPath") {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj(),
      Array(arrayPrepend("x.foo", "world").xattr.createPath))
    assert(updatedContent("foo").arr.map(_.str) == ArrayBuffer("world"))
  }

  // TODO failing with bad input server error
  //  test("array insert xattr createPath") {
  //    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj(),
  //      Array(arrayInsert("x.foo[0]", "cruel").xattr.createPath)
  //    assert(updatedContent("foo").arr.map(_.str) == ArrayBuffer("cruel"))
  //  }
  //
  //  test("array insert unique does not exist xattr createPath") {
  //    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj(),
  //      Array(arrayAddUnique("x.foo", "cruel").xattr.createPath)
  //    assert(updatedContent("foo").arr.map(_.str) == ArrayBuffer("hello", "world", "cruel"))
  //  }


  test("counter +5 xattr createPath") {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj(),
      Array(increment("x.foo", 5).xattr.createPath))
    assert(updatedContent("foo").num == 5)
  }

  test("counter -5 xattr createPath") {
    val updatedContent = checkSingleOpSuccessXattr(ujson.Obj(),
      Array(decrement("x.foo", 3).xattr.createPath))
    assert(updatedContent("foo").num == -3)
  }


  test("insert createPath") {
    val updatedContent = checkSingleOpSuccess(ujson.Obj(), Array(insert("foo.baz", "bar2").createPath))
    assert(updatedContent("foo").obj("baz").str == "bar2")
  }

  test("insert string already there createPath") {
    checkSingleOpFailure(ujson.Obj("foo" -> ujson.Obj("baz" -> "bar")), Array(insert("foo.baz", "bar2")), SubDocumentOpResponseStatus.PATH_EXISTS)
  }

  test("upsert string createPath") {
    val updatedContent = checkSingleOpSuccess(ujson.Obj("foo" -> ujson.Obj("baz" -> "bar")), Array(upsert("foo", "bar2").createPath))
    assert(updatedContent("foo").str == "bar2")
  }

  test("upsert string does not exist createPath") {
    val updatedContent = checkSingleOpSuccess(ujson.Obj(), Array(upsert("foo.baz", "bar2").createPath))
    assert(updatedContent("foo").obj("baz").str == "bar2")
  }

  test("array append createPath") {
    val updatedContent = checkSingleOpSuccess(ujson.Obj(),
      Array(arrayAppend("foo", "world").createPath))
    assert(updatedContent("foo").arr.map(_.str) == ArrayBuffer("world"))
  }

  test("array prepend createPath") {
    val updatedContent = checkSingleOpSuccess(ujson.Obj(),
      Array(arrayPrepend("foo", "world").createPath))
    assert(updatedContent("foo").arr.map(_.str) == ArrayBuffer("world"))
  }

  // TODO failing with bad input server error
  //  test("array insert createPath") {
  //    val updatedContent = checkSingleOpSuccess(ujson.Obj(),
  //      Array(arrayInsert("foo[0]", "cruel").createPath)
  //    assert(updatedContent("foo").arr.map(_.str) == ArrayBuffer("cruel"))
  //  }
  //
  //  test("array insert unique does not exist createPath") {
  //    val updatedContent = checkSingleOpSuccess(ujson.Obj(),
  //      Array(arrayAddUnique("foo", "cruel").createPath)
  //    assert(updatedContent("foo").arr.map(_.str) == ArrayBuffer("hello", "world", "cruel"))
  //  }


  test("counter +5 createPath") {
    val updatedContent = checkSingleOpSuccess(ujson.Obj(),
      Array(increment("foo", 5).createPath))
    assert(updatedContent("foo").num == 5)
  }

  test("counter -5 createPath") {
    val updatedContent = checkSingleOpSuccess(ujson.Obj(),
      Array(decrement("foo", 3).createPath))
    assert(updatedContent("foo").num == -3)
  }

  test("expiration") {
    val content = ujson.Obj("hello" -> "world")
    val (docId, cas) = prepare(content)

    coll.mutateIn(docId, Array(insert("foo2", "bar2")), expiration = 10.seconds) match {
      case Success(result) => assert(result.cas != cas)
      case Failure(err) => assert(false, s"unexpected error $err")
    }

    coll.get(docId, withExpiration = true) match {
      case Success(result) =>
        assert(result.expiration.isDefined)
        assert(result.expiration.get.toSeconds != 0)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }


  test("> 16") {
    val content = ujson.Obj("hello" -> "world")
    val (docId, cas) = prepare(content)

    coll.mutateIn(docId, Array(insert("foo0", "bar0")
      , insert("foo1", "bar1")
      , insert("foo2", "bar2")
      , insert("foo3", "bar3")
      , insert("foo4", "bar4")
      , insert("foo5", "bar5")
      , insert("foo6", "bar6")
      , insert("foo7", "bar7")
      , insert("foo8", "bar8")
      , insert("foo9", "bar9")
      , insert("foo10", "bar10")
      , insert("foo11", "bar11")
      , insert("foo12", "bar12")
      , insert("foo13", "bar13")
      , insert("foo14", "bar14")
      , insert("foo15", "bar15")
      , insert("foo16", "bar16"))) match {
      case Success(result) => assert(false, "should not succeed")
      case Failure(err: IllegalArgumentException) =>
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  test("two commands succeed") {
    val content = ujson.Obj("hello" -> "world")
    val (docId, cas) = prepare(content)

    coll.mutateIn(docId, Array(insert("foo0", "bar0")
      , insert("foo1", "bar1")
      , insert("foo2", "bar2"))) match {
      case Success(result) =>
      case Failure(err) => assert(false, s"unexpected error $err")
    }

    val updated = getContent(docId)
    assert(updated("foo1").str == "bar1")
    assert(updated("foo2").str == "bar2")
  }


  test("two commands one fails") {
    val content = ujson.Obj("foo1" -> "bar_orig_1", "foo2" -> "bar_orig_2")
    val (docId, cas) = prepare(content)

    coll.mutateIn(docId, Array(insert("foo0", "bar0")
      , insert("foo1", "bar1")
      , remove("foo3"))) match {
      case Success(result) =>
      case Failure(err: MultiMutationException) =>
      case Failure(err) => assert(false, s"unexpected error $err")
    }

    val updated = getContent(docId)
    assert(updated("foo1").str == "bar_orig_1")
  }

  test("write and read primitive boolean") {
    val docId = TestUtils.docId()
    assert(coll.mutateIn(docId, Array(upsert("foo", true)), document = DocumentCreation.Insert).isSuccess)

    (for {
      result <- coll.lookupIn(docId, Array(get("foo")))
      content <- result.contentAs[Boolean](0)
    } yield content) match {
      case Success(content: Boolean) =>
        assert(content)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  test("write and read primitive int") {
    val docId = TestUtils.docId()
    assert(coll.mutateIn(docId, Array(upsert("foo", 42)), document = DocumentCreation.Insert).isSuccess)

    (for {
      result <- coll.lookupIn(docId, Array(get("foo")))
      content <- result.contentAs[Int](0)
    } yield content) match {
      case Success(content) => assert(content == 42)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  test("write and read primitive double") {
    val docId = TestUtils.docId()
    assert(coll.mutateIn(docId, Array(upsert("foo", 42.3)), document = DocumentCreation.Insert).isSuccess)

    (for {
      result <- coll.lookupIn(docId, Array(get("foo")))
      content <- result.contentAs[Double](0)
    } yield content) match {
      case Success(content) => assert(content == 42.3)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  test("write and read primitive long") {
    val docId = TestUtils.docId()
    assert(coll.mutateIn(docId, Array(upsert("foo", Long.MaxValue)), document = DocumentCreation.Insert).isSuccess)

    (for {
      result <- coll.lookupIn(docId, Array(get("foo")))
      content <- result.contentAs[Long](0)
    } yield content) match {
      case Success(content) => assert(content == Long.MaxValue)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  test("write and read primitive short") {
    val docId = TestUtils.docId()
    assert(coll.mutateIn(docId, Array(upsert("foo", Short.MaxValue)), document = DocumentCreation.Insert).isSuccess)

    (for {
      result <- coll.lookupIn(docId, Array(get("foo")))
      content <- result.contentAs[Short](0)
    } yield content) match {
      case Success(content) => assert(content == Short.MaxValue)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }
}
