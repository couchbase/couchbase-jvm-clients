package com.couchbase.client.scala

import com.couchbase.client.core.error.{DocumentDoesNotExistException, TemporaryLockFailureException}
import com.couchbase.client.scala.util.Validate
import org.scalatest.{FunSpec, FunSuite}

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

class KeyValueSpec extends FunSuite {
  // TODO support Jenkins
  val (cluster, bucket, coll) = (for {
    cluster <- Cluster.connect("localhost", "Administrator", "password")
    bucket <- cluster.bucket("default")
    coll <- bucket.defaultCollection()
  } yield (cluster, bucket, coll)) match {
    case Success(result) => result
    case Failure(err) => throw err
  }

  test("insert") {
    val docId = TestUtils.docId()
    coll.remove(docId)
    val content = ujson.Obj("hello" -> "world")
    assert(coll.insert(docId, content).isSuccess)

    coll.get(docId) match {
      case Success(result) =>
        result.contentAs[ujson.Obj] match {
          case Success(body) =>
            assert(body("hello").str == "world")
          case Failure(err) => assert(false, s"unexpected error $err")
        }
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  test("exists") {
    val docId = TestUtils.docId()
    coll.remove(docId)

    coll.exists(docId) match {
      case Success(result) => assert(!result.exists)
      case Failure(err) => assert(false, s"unexpected error $err")
    }

    assert(coll.insert(docId, ujson.Obj()).isSuccess)

    coll.exists(docId) match {
      case Success(result) => assert(result.exists)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  private def cleanupDoc(docIdx: Int = 0): String = {
    val docId = TestUtils.docId(docIdx)
    coll.remove(docId)
    docId
  }

  test("insert returns cas") {
    val docId = cleanupDoc()

    val content = ujson.Obj("hello" -> "world")
    coll.insert(docId, content) match {
      case Success(result) => assert(result.cas != 0)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }


  test("insert without expiry") {
    val docId = cleanupDoc()

    val content = ujson.Obj("hello" -> "world")
    assert(coll.insert(docId, content, expiration = 5.seconds).isSuccess)

    coll.get(docId) match {
      case Success(result) => assert(result.expiration.isEmpty)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  test("insert with expiry") {
    val docId = cleanupDoc()

    val content = ujson.Obj("hello" -> "world")
    assert(coll.insert(docId, content, expiration = 5.seconds).isSuccess)

    coll.get(docId, withExpiration = true) match {
      case Success(result) => assert(result.expiration.isDefined)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  test("get and lock") {
    val docId = TestUtils.docId()
    coll.remove(docId)
    val content = ujson.Obj("hello" -> "world")
    val insertResult = coll.insert(docId, content).get

    coll.getAndLock(docId) match {
      case Success(result) =>
        assert(result.cas != 0)
        assert(result.cas != insertResult.cas)
        assert(result.contentAs[ujson.Obj].get == content)
      case Failure(err) => assert(false, s"unexpected error $err")
    }

    coll.getAndLock(docId) match {
      case Success(result) => assert(false, "should not have been able to relock locked doc")
      case Failure(err: TemporaryLockFailureException) =>
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  test("get and touch") {
    val docId = TestUtils.docId()
    coll.remove(docId)
    val content = ujson.Obj("hello" -> "world")
    val insertResult = coll.insert(docId, content, expiration = 10.seconds).get

    assert (insertResult.cas != 0)

    coll.getAndTouch(docId, 1.second) match {
      case Success(result) =>
        assert(result.cas != 0)
        assert(result.cas != insertResult.cas)
        assert(result.contentAs[ujson.Obj].get == content)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  test("remove") {
    val docId = TestUtils.docId()
    coll.remove(docId)
    val content = ujson.Obj("hello" -> "world")
    assert(coll.insert(docId, content).isSuccess)

    assert(coll.remove(docId).isSuccess)

    coll.get(docId) match {
      case Success(result) => assert(false, s"doc $docId exists and should not")
      case Failure(err: DocumentDoesNotExistException) =>
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  test("upsert when doc does not exist") {
    val docId = TestUtils.docId()
    coll.remove(docId)
    val content = ujson.Obj("hello" -> "world")
    val upsertResult = coll.upsert(docId, content)

    upsertResult match {
      case Success(result) =>
        assert(result.cas != 0)
        assert(result.mutationToken.isEmpty)
      case Failure(err) => assert(false, s"unexpected error $err")
    }

    coll.get(docId) match {
      case Success(result) =>
        assert(result.cas == upsertResult.get.cas)
        assert(result.contentAs[ujson.Obj].get == content)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }


  test("upsert when doc does exist") {
    val docId = TestUtils.docId()
    coll.remove(docId)
    val content = ujson.Obj("hello" -> "world")
    val insertResult = coll.insert(docId, content)

    assert (insertResult.isSuccess)

    val content2 = ujson.Obj("hello" -> "world2")
    val upsertResult = coll.upsert(docId, content2)

    upsertResult match {
      case Success(result) =>
        assert(result.cas != 0)
        assert(result.cas != insertResult.get.cas)
        assert(result.mutationToken.isEmpty)
      case Failure(err) => assert(false, s"unexpected error $err")
    }

    coll.get(docId) match {
      case Success(result) =>
        assert(result.cas == upsertResult.get.cas)
        assert(result.contentAs[ujson.Obj].get == content2)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }


  test("replace when doc does not exist") {
    val docId = TestUtils.docId()
    coll.remove(docId)
    val content = ujson.Obj("hello" -> "world")
    val upsertResult = coll.replace(docId, content)

    upsertResult match {
      case Success(result) => assert(false, s"doc should not exist")
      case Failure(err: DocumentDoesNotExistException) =>
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }


  test("replace when doc does exist with cas=0") {
    val docId = TestUtils.docId()
    coll.remove(docId)
    val content = ujson.Obj("hello" -> "world")
    val insertResult = coll.insert(docId, content)

    assert (insertResult.isSuccess)

    val content2 = ujson.Obj("hello" -> "world2")
    val replaceResult = coll.replace(docId, content2)

    replaceResult match {
      case Success(result) =>
        assert(result.cas != 0)
        assert(result.cas != insertResult.get.cas)
        assert(result.mutationToken.isEmpty)
      case Failure(err) => assert(false, s"unexpected error $err")
    }

    coll.get(docId) match {
      case Success(result) =>
        assert(result.cas == replaceResult.get.cas)
        assert(result.contentAs[ujson.Obj].get == content2)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }


  test("replace when doc does exist with cas") {
    val docId = TestUtils.docId()
    coll.remove(docId)
    val content = ujson.Obj("hello" -> "world")
    val insertResult = coll.insert(docId, content)

    assert (insertResult.isSuccess)

    val content2 = ujson.Obj("hello" -> "world2")
    val replaceResult = coll.replace(docId, content2, insertResult.get.cas)

    replaceResult match {
      case Success(result) =>
        assert(result.cas != 0)
        assert(result.cas != insertResult.get.cas)
        assert(result.mutationToken.isEmpty)
      case Failure(err) => assert(false, s"unexpected error $err")
    }

    coll.get(docId) match {
      case Success(result) =>
        assert(result.cas == replaceResult.get.cas)
        assert(result.contentAs[ujson.Obj].get == content2)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  test("validations") {
    val validations: Try[Any] = for {
      _ <- Validate.notNullOrEmpty("", "id")
    } yield null

    assert(validations.isFailure)
  }
}
