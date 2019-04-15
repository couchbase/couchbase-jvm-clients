package com.couchbase.client.scala

import com.couchbase.client.core.error.{DocumentDoesNotExistException, TemporaryLockFailureException}
import org.scalatest.FunSuite

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import reactor.core.publisher.{Mono => JavaMono}
import reactor.core.scala.publisher.{Mono => ScalaMono}

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.control.NonFatal


class ReactiveKeyValueSpec extends FunSuite {
    val cluster = Cluster.connect("localhost", "Administrator", "password")
    val bucket = cluster.bucket("default")
    val blocking = bucket.defaultCollection

  val coll = blocking.reactive

  def wrap[T](in: ScalaMono[T]): Try[T] = {
    try {
      Success(in.block())
    }
    catch {
      case NonFatal(err) => Failure(err)
    }
  }

  test("insert") {
    val docId = TestUtils.docId()
    coll.remove(docId)
    val content = ujson.Obj("hello" -> "world")
    assert(wrap(coll.insert(docId, content)).isSuccess)

    wrap(coll.get(docId)) match {
      case Success(Some(result)) =>
        result.contentAs[ujson.Obj] match {
          case Success(body) =>
            assert(body("hello").str == "world")
          case Failure(err) => assert(false, s"unexpected error $err")
        }
      case Success(None) => assert(false, s"unexpected error doc not found")
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  test("exists") {
    val docId = TestUtils.docId()
    coll.remove(docId)

    wrap(coll.exists(docId)) match {
      case Success(result) => assert(!result.exists)
      case Failure(err) => assert(false, s"unexpected error $err")
    }

    assert(wrap(coll.insert(docId, ujson.Obj())).isSuccess)

    wrap(coll.exists(docId)) match {
      case Success(result) => assert(result.exists)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  private def cleanupDoc(docIdx: Int = 0): String = {
    val docId = TestUtils.docId(docIdx)
    docId
  }

  test("insert returns cas") {
    val docId = cleanupDoc()

    val content = ujson.Obj("hello" -> "world")
    wrap(coll.insert(docId, content)) match {
      case Success(result) => assert(result.cas != 0)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }


  test("insert without expiry") {
    val docId = cleanupDoc()

    val content = ujson.Obj("hello" -> "world")
    assert(wrap(coll.insert(docId, content, expiration = 5.seconds)).isSuccess)

    wrap(coll.get(docId)) match {
      case Success(Some(result)) => assert(result.expiration.isEmpty)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  test("insert with expiry") {
    val docId = cleanupDoc()

    val content = ujson.Obj("hello" -> "world")
    assert(wrap(coll.insert(docId, content, expiration = 5.seconds)).isSuccess)

    wrap(coll.get(docId, withExpiration = true)) match {
      case Success(Some(result)) => assert(result.expiration.isDefined)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  test("get and lock") {
    val docId = TestUtils.docId()
    coll.remove(docId)
    val content = ujson.Obj("hello" -> "world")
    val insertResult = wrap(coll.insert(docId, content)).get

    wrap(coll.getAndLock(docId)) match {
      case Success(Some(result)) =>
        assert(result.cas != 0)
        assert(result.cas != insertResult.cas)
        assert(result.contentAs[ujson.Obj].get == content)
      case Failure(err) => assert(false, s"unexpected error $err")
    }

    wrap(coll.getAndLock(docId)) match {
      case Success(Some(result)) => assert(false, "should not have been able to relock locked doc")
      case Failure(err: TemporaryLockFailureException) =>
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  test("get and touch") {
    val docId = TestUtils.docId()
    coll.remove(docId)
    val content = ujson.Obj("hello" -> "world")
    val insertResult = wrap(coll.insert(docId, content, expiration = 10.seconds)).get

    assert (insertResult.cas != 0)

    wrap(coll.getAndTouch(docId, 1.second)) match {
      case Success(Some(result)) =>
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
    assert(wrap(coll.insert(docId, content)).isSuccess)

    assert(wrap(coll.remove(docId)).isSuccess)

    wrap(coll.get(docId)) match {
      case Success(Some(result)) => assert(false, s"doc $docId exists and should not")
      case Failure(err: DocumentDoesNotExistException) =>
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  test("upsert when doc does not exist") {
    val docId = TestUtils.docId()
    coll.remove(docId)
    val content = ujson.Obj("hello" -> "world")
    val upsertResult = wrap(coll.upsert(docId, content))

    upsertResult match {
      case Success(result) =>
        assert(result.cas != 0)
        assert(result.mutationToken.isEmpty)
      case Failure(err) => assert(false, s"unexpected error $err")
    }

    wrap(coll.get(docId)) match {
      case Success(Some(result)) =>
        assert(result.cas == upsertResult.get.cas)
        assert(result.contentAs[ujson.Obj].get == content)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }


  test("upsert when doc does exist") {
    val docId = TestUtils.docId()
    coll.remove(docId)
    val content = ujson.Obj("hello" -> "world")
    val insertResult = wrap(coll.insert(docId, content))

    assert (insertResult.isSuccess)

    val content2 = ujson.Obj("hello" -> "world2")
    val upsertResult = wrap(coll.upsert(docId, content2))

    upsertResult match {
      case Success(result) =>
        assert(result.cas != 0)
        assert(result.cas != insertResult.get.cas)
        assert(result.mutationToken.isEmpty)
      case Failure(err) => assert(false, s"unexpected error $err")
    }

    wrap(coll.get(docId)) match {
      case Success(Some(result)) =>
        assert(result.cas == upsertResult.get.cas)
        assert(result.contentAs[ujson.Obj].get == content2)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }


  test("replace when doc does not exist") {
    val docId = TestUtils.docId()
    coll.remove(docId)
    val content = ujson.Obj("hello" -> "world")
    val upsertResult = wrap(coll.replace(docId, content))

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
    val insertResult = wrap(coll.insert(docId, content))

    assert (insertResult.isSuccess)

    val content2 = ujson.Obj("hello" -> "world2")
    val replaceResult = wrap(coll.replace(docId, content2))

    replaceResult match {
      case Success(result) =>
        assert(result.cas != 0)
        assert(result.cas != insertResult.get.cas)
        assert(result.mutationToken.isEmpty)
      case Failure(err) => assert(false, s"unexpected error $err")
    }

    wrap(coll.get(docId)) match {
      case Success(Some(result)) =>
        assert(result.cas == replaceResult.get.cas)
        assert(result.contentAs[ujson.Obj].get == content2)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }


  test("replace when doc does exist with cas") {
    val docId = TestUtils.docId()
    coll.remove(docId)
    val content = ujson.Obj("hello" -> "world")
    val insertResult = wrap(coll.insert(docId, content))

    assert (insertResult.isSuccess)

    val content2 = ujson.Obj("hello" -> "world2")
    val replaceResult = wrap(coll.replace(docId, content2, insertResult.get.cas))

    replaceResult match {
      case Success(result) =>
        assert(result.cas != 0)
        assert(result.cas != insertResult.get.cas)
        assert(result.mutationToken.isEmpty)
      case Failure(err) => assert(false, s"unexpected error $err")
    }

    wrap(coll.get(docId)) match {
      case Success(Some(result)) =>
        assert(result.cas == replaceResult.get.cas)
        assert(result.contentAs[ujson.Obj].get == content2)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  test("initialise reactively") {
    val coll: ReactiveCollection = ReactiveCluster.connect("localhost", "Administrator", "password")
      .flatMap(cluster => cluster.bucket("default"))
      .flatMap(bucket => bucket.scope(DefaultResources.DefaultScope))
      .flatMap(scope => scope.defaultCollection)
      .block()
  }

  test("initialise async") {
    val cluster = Await.result(
      AsyncCluster.connect("localhost", "Administrator", "password"),
      Duration.Inf)
    implicit val ec = ExecutionContext.Implicits.global

    val coll: Future[AsyncCollection] = cluster.bucket("default")
      .flatMap(bucket => bucket.scope(DefaultResources.DefaultScope))
      .flatMap(scope => scope.defaultCollection)

    val c: AsyncCollection = Await.result(coll, Duration.Inf)
  }
}
