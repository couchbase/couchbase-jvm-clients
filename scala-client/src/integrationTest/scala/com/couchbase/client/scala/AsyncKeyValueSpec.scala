package com.couchbase.client.scala

import java.util.NoSuchElementException
import java.util.concurrent.{CountDownLatch, Executors, ThreadFactory, TimeUnit}

import com.couchbase.client.core.service.ServiceType
import com.couchbase.client.scala.env.ClusterEnvironment
import com.couchbase.client.scala.json.{JsonObject, JsonObjectSafe}
import com.couchbase.client.scala.util.ScalaIntegrationTest
import com.couchbase.client.test.ClusterAwareIntegrationTest
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.{BeforeAll, Test, TestInstance}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}
import scala.util.{Failure, Success}

@TestInstance(Lifecycle.PER_CLASS)
class AsyncKeyValueSpec extends ScalaIntegrationTest {
  private var cluster: Cluster      = _
  private var blocking: Collection  = _
  private var coll: AsyncCollection = _

  // All async operations, including onComplete, map, flatMap, require an ec
  private val threadPool = Executors.newCachedThreadPool(new ThreadFactory {
    override def newThread(runnable: Runnable): Thread = {
      val thread = new Thread(runnable)
      // Make it a daemon thread so it doesn't block app exit
      thread.setDaemon(true)
      thread.setName("test-thread-" + thread.getId)
      thread
    }
  })
  private implicit val ec = ExecutionContext.fromExecutor(threadPool)

  @BeforeAll
  def beforeAll(): Unit = {
    cluster = connectToCluster()
    val bucket = cluster.bucket(config.bucketname)
    blocking = bucket.defaultCollection
    coll = blocking.async
    bucket.waitUntilReady(Duration(30, TimeUnit.SECONDS))
  }

  @Test
  def upsert(): Unit = {
    val latch = new CountDownLatch(1)
    val id    = TestUtils.docId()
    val json  = JsonObject("foo" -> "bar", "baz" -> "qux")

    val result = coll
      .upsert(id, json)
      .flatMap(_ => coll.get(id))
      .map(_.contentAs[JsonObjectSafe].get)
      .map(_.str("does_not_exist").get)

    result onComplete {
      case Success(value) => assert(false)
      case Failure(err: NoSuchElementException) =>
        latch.countDown()
      case Failure(exception) =>
        println(exception)
        assert(false)
    }

    latch.await()
  }
}
