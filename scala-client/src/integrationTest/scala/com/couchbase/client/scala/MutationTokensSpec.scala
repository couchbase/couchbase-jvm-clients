package com.couchbase.client.scala

import com.couchbase.client.scala.env.{ClusterEnvironment, IoConfig}
import com.couchbase.client.scala.util.ScalaIntegrationTest
import com.couchbase.client.test.ClusterAwareIntegrationTest
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test, TestInstance}

import scala.util.{Failure, Success}

@TestInstance(Lifecycle.PER_CLASS)
class MutationTokensSpec extends ScalaIntegrationTest {

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

  @Test
  def insert(): Unit = {
    val docId   = TestUtils.docId()
    val content = ujson.Obj("hello" -> "world")
    coll.insert(docId, content) match {
      case Success(result) =>
        assert(result.mutationToken.isDefined)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

}
