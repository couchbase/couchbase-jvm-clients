package com.couchbase.client.scala

import com.couchbase.client.scala.env.{ClusterEnvironment, IoConfig}
import com.couchbase.client.scala.util.ScalaIntegrationTest
import com.couchbase.client.test.{ClusterAwareIntegrationTest, ClusterType, IgnoreWhen}
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test, TestInstance}
import org.junit.jupiter.api.TestInstance.Lifecycle

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Failure, Success}

@TestInstance(Lifecycle.PER_CLASS)
class GetFromReplicaSpec extends ScalaIntegrationTest {

  private var cluster: Cluster             = _
  private var coll: Collection             = _
  private var reactive: ReactiveCollection = _
  private var async: AsyncCollection       = _

  @BeforeAll
  def beforeAll(): Unit = {
    cluster = connectToCluster()
    val bucket = cluster.bucket(config.bucketname)
    coll = bucket.defaultCollection
    reactive = coll.reactive
    async = coll.async
  }

  @AfterAll
  def afterAll(): Unit = {
    cluster.disconnect()
  }

  @Test
  def any_async(): Unit = {
    val docId   = TestUtils.docId()
    val content = ujson.Obj("hello" -> "world")

    assert(coll.insert(docId, content).isSuccess)

    val future = async.getAnyReplica(docId)

    val result = Await.result(future, Duration.Inf)

    result.contentAs[ujson.Obj] match {
      case Success(body) => assert(body("hello").str == "world")
      case Failure(err)  => assert(false, s"unexpected error $err")
    }
  }

  @Test
  def any_blocking(): Unit = {
    val docId   = TestUtils.docId()
    val content = ujson.Obj("hello" -> "world")
    assert(coll.insert(docId, content).isSuccess)

    val result = coll.getAnyReplica(docId).get

    assert(result.contentAs[ujson.Obj].get("hello").str == "world")
    assert(!result.isReplica)
  }

  @Test
  def any_reactive(): Unit = {
    val docId   = TestUtils.docId()
    val content = ujson.Obj("hello" -> "world")
    assert(coll.insert(docId, content).isSuccess)

    val results = reactive.getAnyReplica(docId)

    val result = results.block()
    assert(result.contentAs[ujson.Obj].get("hello").str == "world")
  }
  @IgnoreWhen(clusterTypes = Array(ClusterType.MOCKED))
  @Test
  def all_async(): Unit = {
    val docId   = TestUtils.docId()
    val content = ujson.Obj("hello" -> "world")

    assert(coll.insert(docId, content).isSuccess)

    val results = async.getAllReplicas(docId)

    results.foreach(future => {
      val result = Await.result(future, Duration.Inf)

      result.contentAs[ujson.Obj] match {
        case Success(body) => assert(body("hello").str == "world")
        case Failure(err)  => assert(false, s"unexpected error $err")
      }
    })
  }

  @IgnoreWhen(clusterTypes = Array(ClusterType.MOCKED))
  @Test
  def all_blocking(): Unit = {
    val docId   = TestUtils.docId()
    val content = ujson.Obj("hello" -> "world")
    assert(coll.insert(docId, content).isSuccess)

    val results = coll.getAllReplicas(docId)

    for (result <- results) {
      assert(result.contentAs[ujson.Obj].get("hello").str == "world")
    }
  }

  @IgnoreWhen(clusterTypes = Array(ClusterType.MOCKED))
  @Test
  def all_reactive(): Unit = {
    val docId   = TestUtils.docId()
    val content = ujson.Obj("hello" -> "world")
    assert(coll.insert(docId, content).isSuccess)

    val results = reactive.getAllReplicas(docId)

    results
      .doOnNext(result => {
        assert(result.contentAs[ujson.Obj].get("hello").str == "world")
      })
      .blockLast()
  }

}
