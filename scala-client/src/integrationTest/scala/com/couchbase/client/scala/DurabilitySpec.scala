package com.couchbase.client.scala

import com.couchbase.client.core.error.{DurabilityLevelNotAvailableException, TimeoutException}
import com.couchbase.client.scala.durability.Durability._
import com.couchbase.client.scala.durability._
import com.couchbase.client.scala.env.{ClusterEnvironment, IoConfig}
import com.couchbase.client.scala.util.ScalaIntegrationTest
import com.couchbase.client.test.{Capabilities, ClusterAwareIntegrationTest, IgnoreWhen}
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test, TestInstance}

import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

@TestInstance(Lifecycle.PER_CLASS)
@IgnoreWhen(missesCapabilities = Array(Capabilities.SYNC_REPLICATION))
class DurabilitySpec extends ScalaIntegrationTest {

  private var cluster: Cluster = _
  private var coll: Collection = _

  @BeforeAll
  def beforeAll(): Unit = {
    cluster = connectToCluster()
    val bucket = cluster.bucket(config.bucketname)
    coll = bucket.defaultCollection

  }

  @AfterAll
  def afterAll(): Unit = {
    cluster.disconnect()
  }

  @IgnoreWhen(replicasLessThan = 2)
  @Test
  def replicateTo_2(): Unit = {
    val docId   = TestUtils.docId()
    val content = ujson.Obj("hello" -> "world")
    coll.insert(docId, content, durability = ClientVerified(ReplicateTo.Two)) match {
      case Success(result) =>
      case Failure(err)    => assert(false, s"unexpected error $err")
    }
  }

  @IgnoreWhen(replicasLessThan = 2)
  @Test
  def persistTo_2(): Unit = {
    val docId   = TestUtils.docId()
    val content = ujson.Obj("hello" -> "world")
    coll.insert(docId, content, durability = ClientVerified(ReplicateTo.None, PersistTo.Two)) match {
      case Success(result) =>
      case Failure(err)    => assert(false, s"unexpected error $err")
    }
  }

  @Test
  def Disabled(): Unit = {
    val docId   = TestUtils.docId()
    val content = ujson.Obj("hello" -> "world")
    coll.insert(docId, content, durability = Durability.Disabled) match {
      case Success(_)   =>
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  @Test
  def MajorityTimeoutTooShort(): Unit = {
    val docId   = TestUtils.docId()
    val content = ujson.Obj("hello" -> "world")
    coll.insert(docId, content, durability = Durability.Majority, timeout = Duration.Zero) match {
      case Success(_)                     => assert(false, s"unexpected success")
      case Failure(err: TimeoutException) =>
      case Failure(err)                   => assert(false, s"unexpected error $err")
    }
  }

  @Test
  def Majority(): Unit = {
    val docId   = TestUtils.docId()
    val content = ujson.Obj("hello" -> "world")
    coll.insert(docId, content, durability = Durability.Majority) match {
      case Success(_)   =>
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  @Test
  def MajorityAndPersistToActive(): Unit = {
    val docId   = TestUtils.docId()
    val content = ujson.Obj("hello" -> "world")
    coll.insert(docId, content, durability = Durability.MajorityAndPersistToActive) match {
      case Success(_)   =>
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

  @Test
  def PersistToMajority(): Unit = {
    val docId   = TestUtils.docId()
    val content = ujson.Obj("hello" -> "world")
    coll.insert(docId, content, durability = Durability.PersistToMajority) match {
      case Success(_)   =>
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

}
