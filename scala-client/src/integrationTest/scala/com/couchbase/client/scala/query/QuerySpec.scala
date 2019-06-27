package com.couchbase.client.scala.query

import java.util
import java.util.{List, UUID}
import java.util.concurrent.atomic.AtomicReference

import com.couchbase.client.core.error.QueryServiceException
import com.couchbase.client.core.msg.kv.MutationToken
import com.couchbase.client.scala.env.{ClusterEnvironment, IoConfig}
import com.couchbase.client.scala.json.JsonObject
import com.couchbase.client.scala.util.ScalaIntegrationTest
import com.couchbase.client.scala.{Cluster, Collection, TestUtils}
import com.couchbase.client.test.{Capabilities, ClusterAwareIntegrationTest, ClusterType, IgnoreWhen}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api._

import scala.concurrent.duration._
import scala.util.{Failure, Success}

@TestInstance(Lifecycle.PER_CLASS)
@IgnoreWhen(missesCapabilities = Array(Capabilities.QUERY))
class QuerySpec extends ScalaIntegrationTest {

  private var env: ClusterEnvironment = _
  private var cluster: Cluster = _
  private var coll: Collection = _
  private var bucketName: String = _

  @BeforeAll
  def beforeAll(): Unit = {
    val config = ClusterAwareIntegrationTest.config()
    env = environment.ioConfig(IoConfig().mutationTokensEnabled(true)).build
    cluster = Cluster.connect(env)
    val bucket = cluster.bucket(config.bucketname)
    coll = bucket.defaultCollection

    cluster.query("create primary index on `" + config.bucketname + "`")
    bucketName = config.bucketname()
  }

  @AfterAll
  def afterAll(): Unit = {
    cluster.shutdown()
    env.shutdown()
  }


  def getContent(docId: String): ujson.Obj = {
    coll.get(docId) match {
      case Success(result) =>
        result.contentAs[ujson.Obj] match {
          case Success(content) =>
            content
          case Failure(err) =>
            assert(false, s"unexpected error $err")
            null
        }
      case Failure(err) =>
        assert(false, s"unexpected error $err")
        null
    }
  }

  private def prepare(content: ujson.Value) = {
    val docId = TestUtils.docId()
    val insertResult = coll.insert(docId, content).get
    (docId, insertResult.mutationToken)
  }

  @Test
  def hello_world() {
    cluster.query("""select 'hello world' as Greeting""") match {
      case Success(result) =>
        assert(result.rows.size == 1)
        val rows = result.allRowsAs[String].get
        assert(rows.head == """{"Greeting":"hello world"}""")
      case Failure(err) => throw err
    }
  }

  @Test
  def hello_world_content_as_JsonObject() {
    cluster.query("""select 'hello world 2' as Greeting""") match {
      case Success(result) =>
        assert(result.meta.clientContextId.isEmpty)
        assert(result.meta.requestId != null)
        assert(result.rows.size == 1)
        val rows = result.allRowsAs[JsonObject].get
        assert(rows.head.str("Greeting") == "hello world 2")
        val signature = result.meta.signature.get.contentAs[JsonObject].get
        assert(signature.size > 0)

        val out = result.meta
        assert(out.metrics.get.errorCount == 0)
        assert(out.metrics.get.warningCount == 0)
        assert(out.metrics.get.mutationCount == 0)
        assert(out.warnings.size == 0)
        assert(out.status == "success")
        assert(out.profile.isEmpty)

      case Failure(err) => throw err
    }
  }

  @Test
  def hello_world_content_as_JsonObject_for_comp() {
    (for {
      result <- cluster.query("""select 'hello world 2' as Greeting""")
      rows <- result.allRowsAs[JsonObject]
    } yield (result, rows)) match {
      case Success((result, rows)) =>
        assert(result.rows.size == 1)
        val rows = result.allRowsAs[JsonObject].get
        assert(rows.head.str("Greeting") == "hello world 2")
        val signature = result.meta.signature.get.contentAs[JsonObject].get
        assert(signature.size > 0)

        val out = result.meta
        assert(out.metrics.get.errorCount == 0)
        assert(out.metrics.get.warningCount == 0)
        assert(out.metrics.get.mutationCount == 0)
        assert(out.warnings.size == 0)
        assert(out.status == "success")
        assert(out.profile.isEmpty)

      case Failure(err) => throw err
    }
  }

  @Test
  def hello_world_with_quotes() {
    cluster.query("""select "hello world" as Greeting""") match {
      case Success(result) =>
        assert(result.rows.size == 1)
        val rows = result.allRowsAs[String].get
        assert(rows.head == """{"Greeting":"hello world"}""")
      case Failure(err) => throw err
    }
  }

  @Test
  def read_2_docs_use_keys() {
    val (docId1, _) = prepare(ujson.Obj("name" -> "Andy"))
    val (docId2, _) = prepare(ujson.Obj("name" -> "Beth"))

    val statement =s"""select name from `${bucketName}` use keys ['$docId1', '$docId2'];"""
    //    val statement = s"""SELECT * FROM default USE KEYS '$docId1';"""
    cluster.query(statement, QueryOptions().scanConsistency(ScanConsistency.RequestPlus())) match {
      case Success(result) =>
        val rows = result.allRowsAs[ujson.Obj].get
        assert(rows.size == 2)
        assert(rows.head("name").str == """Andy""")
        assert(rows.last("name").str == """Beth""")
      case Failure(err) =>
        throw err
    }
  }

  @Test
  def error_due_to_bad_syntax() {
    val x = cluster.query("""select*from""")
    x match {
      case Success(result) =>
        assert(false)
      case Failure(err: QueryServiceException) =>
      case Failure(err: QueryError) =>
        val msg = err.msg
      // Fix under SCBC-33
//              assert(msg == "syntax error - at end of input")
//              assert(err.code == Success(3000))
      case Failure(err) =>
        throw err
    }
  }

  @Test
  def reactive_hello_world() {

    cluster.reactive.query("""select 'hello world' as Greeting""")
      .flatMap(result => {
        result.rowsAs[String].doOnNext(v => {
          println("GOT A ROW!!" + v)
        }).collectSeq()

          .doOnNext(rows => assert(rows.size == 1))

          .flatMap(_ => result.meta)

          .doOnNext(meta => {
            assert(meta.clientContextId.isEmpty)
          })
      })
      .block()
  }

  @Test
  def reactive_additional() {

    val rowsKeeper = new AtomicReference[Seq[JsonObject]]()

    val out: QueryMeta = cluster.reactive.query("""select 'hello world' as Greeting""")
      .flatMapMany(result => {
        result.rowsAs[JsonObject]
          .collectSeq()
          .doOnNext(rows => {
            rowsKeeper.set(rows)
          })
          .flatMap(_ => result.meta)
      })
      .blockLast().get

    assert(rowsKeeper.get.size == 1)
    assert(out.metrics.get.errorCount == 0)
    assert(out.metrics.get.warningCount == 0)
    assert(out.metrics.get.mutationCount == 0)
    assert(out.warnings.size == 0)
    assert(out.status == "success")
    assert(out.profile.isEmpty)
  }

  @Test
  def reactive_error_due_to_bad_syntax() {
    Assertions.assertThrows(classOf[QueryServiceException], () => {
      cluster.reactive.query("""sselect*from""")
        .flatMapMany(result => {
          result.rowsAs[String]
            .doOnNext(v => assert(false))
            .doOnError(err => println("expected ERR: " + err))
        })
        .blockLast()
    })
  }

  @Test
  def options_profile () {
    cluster.query("""select 'hello world' as Greeting""", QueryOptions().profile(N1qlProfile.Timings)) match {
      case Success(result) =>
        assert(result.meta.profile.nonEmpty)
        val profile = result.meta.profile.get.contentAs[JsonObject].get
        assert(result.meta.profile.size > 0)
        assert(result.meta.clientContextId.isEmpty)
        assert(result.meta.requestId != null)
        assert(result.rows.size == 1)
        val rows = result.allRowsAs[JsonObject].get
        assert(rows.head.str("Greeting") == "hello world")

      case Failure(err) => throw err
    }
  }

  @Test
  def options_named_params () {
    coll.insert(TestUtils.docId(), JsonObject("name" -> "Eric Wimp"))

    cluster.query(
      s"""select * from `${bucketName}` where name=$$nval""",
      QueryOptions().namedParameter("nval", "Eric Wimp")
        .scanConsistency(ScanConsistency.RequestPlus())) match {
      case Success(result) =>
        assert(result.rows.size > 0)
      case Failure(err) => throw err
    }
  }

  @Test
  def options_positional_params () {
    coll.insert(TestUtils.docId(), JsonObject("name" -> "Eric Wimp"))

    cluster.query(
      s"""select * from `${bucketName}` where name=$$1""",
      QueryOptions().positionalParameters("Eric Wimp")
        .scanConsistency(ScanConsistency.RequestPlus())) match {
      case Success(result) => assert(result.rows.size > 0)
      case Failure(err) => throw err
    }
  }

  @Test
  def options_clientContextId () {
    cluster.query(
      """select 'hello world' as Greeting""",
      QueryOptions().clientContextId("test")) match {
      case Success(result) => assert(result.meta.clientContextId.contains("test"))
      case Failure(err) => throw err
    }
  }

  @Test
  def options_disableMetrics () {
    cluster.query(
      """select 'hello world' as Greeting""",
      QueryOptions().disableMetrics(true)) match {
      case Success(result) =>
        assert(result.meta.metrics.get.errorCount == 0)
      case Failure(err) => throw err
    }
  }

  // Can't really test these so just make sure the server doesn't barf on our encodings
  @Test
  def options_unusual () {
    cluster.query(
      """select 'hello world' as Greeting""",
      QueryOptions().maxParallelism(5)
        .pipelineCap(3)
        .pipelineBatch(6)
        .scanCap(8)
        .serverSideTimeout(30.seconds)
        .timeout(30.seconds)
        .readonly(true)) match {
      case Success(result) =>
        assert(result.rows.size == 1)
      case Failure(err) => throw err
    }
  }


  /**
    * This test is intentionally kept generic, since we want to make sure with every query version
    * that we run against we have a version that works. Also, we perform the same query multiple times
    * to make sure a primed and non-primed cache both work out of the box.
    */
  @Test
  def handlesPreparedStatements(): Unit = {
    val id: String = insertDoc
    for (i <- Range(0, 10)) {
      val options: QueryOptions = QueryOptions()
        .scanConsistency(ScanConsistency.RequestPlus())
        .adhoc(false)
      val result: QueryResult = cluster.query("select `" + bucketName + "`.* from `" + bucketName + "` where meta()" +
        ".id=\"" + id + "\"", options).get
      val rows = result.allRowsAs[JsonObject].get
      assert(1 == rows.size)
      assert(FooContent == rows(0))
    }
  }

  @Test
  def handlesPreparedStatementsWithNamedArgs(): Unit = {
    val id: String = insertDoc
    for (i <- Range(0, 10)) {
      val options: QueryOptions = QueryOptions()
        .scanConsistency(ScanConsistency.RequestPlus())
        .adhoc(false)
        .namedParameters("id" -> id)
      val result: QueryResult = cluster.query("select `" + bucketName + "`.* from `" + bucketName + "` where meta()" +
        ".id=$id", options).get
      val rows = result.allRowsAs[JsonObject].get
      assert(1 == rows.size)
      assert(FooContent == rows(0))
    }
  }

  @Test
  def handlesPreparedStatementsWithPositionalArgs(): Unit = {
    val id: String = insertDoc
    for (i <- Range(0, 10)) {
      val options: QueryOptions = QueryOptions()
        .scanConsistency(ScanConsistency.RequestPlus())
        .adhoc(false)
        .positionalParameters(id)
      val result: QueryResult = cluster.query("select `" + bucketName + "`.* from `" + bucketName + "` where meta()" +
        ".id=$1", options).get
      val rows = result.allRowsAs[JsonObject].get
      assert(1 == rows.size)
      assert(FooContent == rows(0))
    }
  }

  /**
    * Inserts a document into the collection and returns the ID of it.
    *
    * It inserts {@link #FOO_CONTENT}.
    */
  private def insertDoc = {
    val id = UUID.randomUUID.toString
    coll.insert(id, FooContent)
    id
  }

  private val FooContent = JsonObject.create.put("foo", "bar")

  @Test
  def consistentWith(): Unit = {
    val id = UUID.randomUUID.toString
    val mt: MutationToken = coll.upsert(id, FooContent).get.mutationToken.get

    val options: QueryOptions = QueryOptions()
      .consistentWith(Seq(mt))

    val result: QueryResult = cluster.query("select `" + bucketName + "`.* from `" + bucketName + "` where meta()" +
      ".id=\"" + id + "\"", options).get

    val rows = result.allRowsAs[JsonObject].get
    assert(1 == rows.size)
  }
}
