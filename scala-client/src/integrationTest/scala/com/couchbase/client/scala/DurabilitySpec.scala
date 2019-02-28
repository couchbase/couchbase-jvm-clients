package com.couchbase.client.scala

import com.couchbase.client.core.env.IoConfig
import com.couchbase.client.core.error.DurabilityLevelNotAvailableException
import com.couchbase.client.scala.durability.Durability.{Disabled, Majority, MajorityAndPersistOnMaster, PersistToMajority}
import com.couchbase.client.scala.durability._
import com.couchbase.client.scala.env.ClusterEnvironment
import org.scalatest.FunSuite

import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

class DurabilitySpec extends FunSuite {
  // TODO get testing against mock working

    val env = ClusterEnvironment
      .builder("localhost", "Administrator", "password")
      .ioConfig(IoConfig.mutationTokensEnabled(true))
      .build()
    val cluster = Cluster.connect(env)
    val bucket = cluster.bucket("default")
    val coll = bucket.defaultCollection


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

  // TODO see if can support Junit5-style test exclusions
//  test("majority") {
//    val docId = TestUtils.docId()
//    val content = ujson.Obj("hello" -> "world")
//    coll.insert(docId, content, durability = Majority) match {
//      case Success(result) =>assert(false, s"success not expected")
//      case Failure(err: DurabilityLevelNotAvailableException) =>
//      case Failure(err) => assert(false, s"unexpected error $err")
//    }
//  }

  test("Disabled") {
    val docId = TestUtils.docId()
    val content = ujson.Obj("hello" -> "world")
    coll.insert(docId, content, durability = Disabled) match {
      case Success(_) =>
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  test("Majority, timeout too short") {
    val docId = TestUtils.docId()
    val content = ujson.Obj("hello" -> "world")
    coll.insert(docId, content, durability = Majority, timeout = Duration.Zero) match {
      case Success(_) =>assert(false, s"unexpected success")
      case Failure(err: IllegalArgumentException) =>
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }


  // Durability on server side currently seems unstable, will retest later
  ignore("Majority") {
    val docId = TestUtils.docId()
    val content = ujson.Obj("hello" -> "world")
    coll.insert(docId, content, durability = Majority) match {
      case Success(_) =>
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  ignore("MajorityAndPersistOnMaster") {
    val docId = TestUtils.docId()
    val content = ujson.Obj("hello" -> "world")
    coll.insert(docId, content, durability = MajorityAndPersistOnMaster) match {
      case Success(_) =>
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  ignore("PersistToMajority") {
    val docId = TestUtils.docId()
    val content = ujson.Obj("hello" -> "world")
    coll.insert(docId, content, durability = PersistToMajority) match {
      case Success(_) =>
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

}
