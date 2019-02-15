package com.couchbase.client.scala

import com.couchbase.client.core.env.IoConfig
import com.couchbase.client.core.error.DurabilityLevelNotAvailableException
import com.couchbase.client.scala.durability._
import com.couchbase.client.scala.env.ClusterEnvironment
import org.scalatest.FunSuite

import scala.util.{Failure, Success, Try}

class DurabilitySpec extends FunSuite {
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

//  test("replicateTo = 2") {
//    val docId = TestUtils.docId()
//    val content = ujson.Obj("hello" -> "world")
//    coll.insert(docId, content, durability = ClientVerified(ReplicateTo.Two)) match {
//      case Success(result) =>
//      case Failure(err) => assert(false, s"unexpected error $err")
//    }
//  }
//
//  test("persistTo = 2") {
//    val docId = TestUtils.docId()
//    val content = ujson.Obj("hello" -> "world")
//    coll.insert(docId, content, durability = ClientVerified(ReplicateTo.None, PersistTo.Two)) match {
//      case Success(result) =>
//      case Failure(err) => assert(false, s"unexpected error $err")
//    }
//  }

  test("majority") {
    val docId = TestUtils.docId()
    val content = ujson.Obj("hello" -> "world")
    coll.insert(docId, content, durability = Majority) match {
      case Failure(err: DurabilityLevelNotAvailableException) =>
    }
  }

//  test("MajorityAndPersistOnMaster") {
//    val docId = TestUtils.docId()
//    val content = ujson.Obj("hello" -> "world")
//    coll.insert(docId, content, durability = MajorityAndPersistOnMaster) match {
//      case Success(result) =>
//      case Failure(err) => assert(false, s"unexpected error $err")
//    }
//  }

}
