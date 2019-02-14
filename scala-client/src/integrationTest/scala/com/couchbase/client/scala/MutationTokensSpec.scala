package com.couchbase.client.scala

import com.couchbase.client.core.env.{IoConfig, IoEnvironment}
import com.couchbase.client.core.error.{DocumentDoesNotExistException, TemporaryLockFailureException}
import com.couchbase.client.scala.env.ClusterEnvironment
import com.couchbase.client.scala.util.Validate
import org.scalatest.FunSuite

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

class MutationTokensSpec extends FunSuite {
  val (cluster, bucket, coll) = (for {
    env <- Try(ClusterEnvironment
      .builder("localhost", "Administrator", "password")
      .ioConfig(IoConfig.mutationTokensEnabled(true))
      .build())
    cluster <- Cluster.connect(env)
    bucket <- cluster.bucket("default")
    coll <- bucket.defaultCollection()
  } yield (cluster, bucket, coll)) match {
    case Success(result) => result
    case Failure(err) => throw err
  }

  test("insert") {
    val docId = TestUtils.docId()
    val content = ujson.Obj("hello" -> "world")
    coll.insert(docId, content) match {
      case Success(result) =>
        assert(result.mutationToken.isDefined)
        assert(result.mutationToken.get.bucket() == "default")
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

}
