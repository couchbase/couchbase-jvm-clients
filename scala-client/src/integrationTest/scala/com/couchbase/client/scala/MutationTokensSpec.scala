package com.couchbase.client.scala

import com.couchbase.client.scala.env.{ClusterEnvironment, IoConfig}
import com.couchbase.client.scala.util.ScalaIntegrationTest
import com.couchbase.client.test.ClusterAwareIntegrationTest
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test, TestInstance}

import scala.util.{Failure, Success}

@TestInstance(Lifecycle.PER_CLASS)
class MutationTokensSpec extends ScalaIntegrationTest {

  private var env: ClusterEnvironment = null
  private var cluster: Cluster = null
  private var coll: Collection = null

  @BeforeAll
  def beforeAll(): Unit = {
    val config = ClusterAwareIntegrationTest.config()
    val x: ClusterEnvironment.Builder = environment.ioConfig(IoConfig().mutationTokensEnabled(true))
    env = x.build.get
    cluster = Cluster.connect(env).get
    val bucket = cluster.bucket(config.bucketname)
    coll = bucket.defaultCollection

  }

  @AfterAll
  def afterAll(): Unit = {
    cluster.shutdown()
    env.shutdown()
  }

  @Test
  def insert() {
    val docId = TestUtils.docId()
    val content = ujson.Obj("hello" -> "world")
    coll.insert(docId, content) match {
      case Success(result) =>
        assert(result.mutationToken.isDefined)
      case Failure(err) => assert(false, s"unexpected error $err")
    }
  }

}
