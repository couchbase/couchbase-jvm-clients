package com.couchbase.client.scala.subdoc

import com.couchbase.client.core.error.subdoc.PathNotFoundException
import com.couchbase.client.scala.api.{LookupInOps, MutateInOps}
import com.couchbase.client.scala.{Cluster, TestUtils}
import org.scalatest.FunSuite

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


  def prepare(content: ujson.Value): (String, Long) = {
    val docId = TestUtils.docId()
    coll.remove(docId)
    val insertResult = coll.insert(docId, content).get
    (docId, insertResult.cas)
  }

  test("mutateIn insert string") {
    val content = ujson.Obj("hello" -> "world",
      "foo" -> "bar",
      "age" -> 22)
    val (docId, cas) = prepare(content)

    coll.mutateIn(docId, MutateInOps.insert("foo2", "bar2")) match {
      case Success(result) => assert(result.cas != cas)
      case Failure(err) => assert(false, s"unexpected error $err")
    }

    coll.get(docId) match {
      case Success(result) =>
        result.contentAs[ujson.Obj] match {
          case Success(content) =>
            assert(content("foo2").str == "bar2")
          case Failure(err) => assert(false, s"unexpected error $err")
        }
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }


}
