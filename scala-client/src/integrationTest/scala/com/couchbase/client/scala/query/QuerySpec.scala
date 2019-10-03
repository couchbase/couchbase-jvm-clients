package com.couchbase.client.scala.query

import java.util.UUID
import java.util.concurrent.atomic.AtomicReference

import com.couchbase.client.core.error.QueryException
import com.couchbase.client.scala.implicits.Codec
import com.couchbase.client.scala.json.JsonObject
import com.couchbase.client.scala.kv.MutationState
import com.couchbase.client.scala.query.QueryScanConsistency.ConsistentWith
import com.couchbase.client.scala.util.ScalaIntegrationTest
import com.couchbase.client.scala.{Cluster, Collection, TestUtils}
import com.couchbase.client.test.{Capabilities, IgnoreWhen}
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api._
import reactor.core.scala.publisher.SMono

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Failure, Success}

@TestInstance(Lifecycle.PER_CLASS)
@IgnoreWhen(missesCapabilities = Array(Capabilities.QUERY))
class QuerySpec extends ScalaIntegrationTest {

  private var cluster: Cluster = _
  private var coll: Collection = _
  private var bucketName: String = _

  @BeforeAll
  def beforeAll(): Unit = {
    cluster = connectToCluster()
    val bucket = cluster.bucket(config.bucketname)
    coll = bucket.defaultCollection

    cluster.query("create primary index on `" + config.bucketname + "`")
    bucketName = config.bucketname()
  }

  @AfterAll
  def afterAll(): Unit = {
    cluster.disconnect()
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
        val rows = result.rowsAs[String].get
        assert(rows.head == """{"Greeting":"hello world"}""")
        // Should be an implicit client context id if none provided
        assert(result.metaData.clientContextId != "")
      case Failure(err) => throw err
    }
  }

  @Test
  def hello_world_content_as_JsonObject() {
    cluster.query("""select 'hello world 2' as Greeting""", QueryOptions().metrics(true)) match {
      case Success(result) =>
        assert(result.metaData.clientContextId != "")
        assert(result.metaData.requestId != null)
        assert(result.rows.size == 1)
        val rows = result.rowsAs[JsonObject].get
        assert(rows.head.str("Greeting") == "hello world 2")
        val signature = result.metaData.signature.get.contentAs[JsonObject].get
        assert(signature.size > 0)

        val out = result.metaData
        assert(out.metrics.get.errorCount == 0)
        assert(out.metrics.get.warningCount == 0)
        assert(out.metrics.get.mutationCount == 0)
        assert(out.warnings.size == 0)
        assert(out.status == QueryStatus.Success)
        assert(out.profileAs[JsonObject].isFailure)

      case Failure(err) => throw err
    }
  }

  @Test
  def hello_world_content_as_JsonObject_for_comp() {
    (for {
      result <- cluster.query("""select 'hello world 2' as Greeting""", QueryOptions().metrics(true))
      rows <- result.rowsAs[JsonObject]
    } yield (result, rows)) match {
      case Success((result, rows)) =>
        assert(result.rows.size == 1)
        val rows = result.rowsAs[JsonObject].get
        assert(rows.head.str("Greeting") == "hello world 2")
        val signature = result.metaData.signature.get.contentAs[JsonObject].get
        assert(signature.size > 0)

        val out = result.metaData
        assert(out.metrics.get.errorCount == 0)
        assert(out.metrics.get.warningCount == 0)
        assert(out.metrics.get.mutationCount == 0)
        assert(out.warnings.size == 0)
        assert(out.status == QueryStatus.Success)

      case Failure(err) => throw err
    }
  }

  @Test
  def hello_world_with_quotes() {
    cluster.query("""select "hello world" as Greeting""") match {
      case Success(result) =>
        assert(result.rows.size == 1)
        val rows = result.rowsAs[String].get
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
    cluster.query(statement, QueryOptions().scanConsistency(QueryScanConsistency.RequestPlus())) match {
      case Success(result) =>
        val rows = result.rowsAs[ujson.Obj].get
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
      case Failure(err: QueryException) =>
        assert(err.msg == "syntax error - at end of input")
        assert(err.code == 3000)
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

          .flatMap(_ => result.metaData)

          .doOnNext(meta => {
            assert(meta.clientContextId != "")
          })
      })
      .block()
  }

  @Test
  def reactive_additional() {

    val rowsKeeper = new AtomicReference[Seq[JsonObject]]()

    val out: QueryMetaData = cluster.reactive.query("""select 'hello world' as Greeting"""
      , QueryOptions().metrics(true))
      .flatMapMany(result => {
        result.rowsAs[JsonObject]
          .collectSeq()
          .doOnNext(rows => {
            rowsKeeper.set(rows)
          })
          .flatMap(_ => result.metaData)
      })
      .blockLast().get

    assert(rowsKeeper.get.size == 1)
    assert(out.metrics.get.errorCount == 0)
    assert(out.metrics.get.warningCount == 0)
    assert(out.metrics.get.mutationCount == 0)
    assert(out.warnings.size == 0)
    assert(out.status == QueryStatus.Success)
  }

  @Test
  def reactive_error_due_to_bad_syntax() {
    Assertions.assertThrows(classOf[QueryException], () => {
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
    cluster.query("""select 'hello world' as Greeting""", QueryOptions().profile(QueryProfile.Timings)) match {
      case Success(result) =>
        assert(result.metaData.profileAs[JsonObject].isSuccess)
        val profile = result.metaData.profileAs[JsonObject].get
        assert(profile.size > 0)
        assert(result.metaData.clientContextId != "")
        assert(result.metaData.requestId != null)
        assert(result.rows.size == 1)
        val rows = result.rowsAs[JsonObject].get
        assert(rows.head.str("Greeting") == "hello world")

      case Failure(err) => throw err
    }
  }

  @Test
  def options_named_params () {
    coll.insert(TestUtils.docId(), JsonObject("name" -> "Eric Wimp"))

    cluster.query(
      s"""select * from `${bucketName}` where name=$$nval""",
      QueryOptions().namedParameters(Map("nval" -> "Eric Wimp"))
        .scanConsistency(QueryScanConsistency.RequestPlus())) match {
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
      QueryOptions().positionalParameters(Seq("Eric Wimp"))
        .scanConsistency(QueryScanConsistency.RequestPlus())) match {
      case Success(result) => {
        assert(result.rows.size > 0)
      }
      case Failure(err) => throw err
    }
  }

  @Test
  def options_clientContextId () {
    cluster.query(
      """select 'hello world' as Greeting""",
      QueryOptions().clientContextId("test")) match {
      case Success(result) => assert(result.metaData.clientContextId.contains("test"))
      case Failure(err) => throw err
    }
  }

  @Test
  def options_disableMetrics () {
    cluster.query(
      """select 'hello world' as Greeting""",
      QueryOptions().metrics(true)) match {
      case Success(result) =>
        assert(result.metaData.metrics.get.errorCount == 0)
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
    val options: QueryOptions = QueryOptions()
      .scanConsistency(QueryScanConsistency.RequestPlus())
      .adhoc(false)
    val result = cluster.query("select `" + bucketName + "`.* from `" + bucketName + "` where meta()" +
      ".id=\"" + id + "\"", options).get

    val rows = result.rowsAs[JsonObject].get
    assert(1 == rows.size)
    assert(FooContent == rows(0))
  }

  @Test
  def handlesPreparedStatementsAsync(): Unit = {
    val id: String = insertDoc
    val options: QueryOptions = QueryOptions()
      .scanConsistency(QueryScanConsistency.RequestPlus())
      .adhoc(false)
    val future = cluster.async.query("select `" + bucketName + "`.* from `" + bucketName + "` where meta()" +
      ".id=\"" + id + "\"", options)
    val result = Await.result(future, Duration.Inf)
    val rows = result.rowsAs[JsonObject].get
    assert(1 == rows.size)
    assert(FooContent == rows(0))
  }

  @Test
  def handlesPreparedStatementsReactive(): Unit = {
    val id: String = insertDoc
    val options: QueryOptions = QueryOptions()
      .scanConsistency(QueryScanConsistency.RequestPlus())
      .adhoc(false)
    val mono: SMono[ReactiveQueryResult] = cluster.reactive.query("select `" + bucketName + "`.* from `" + bucketName + "` where meta()" +
      ".id=\"" + id + "\"", options)
    val result = mono.block()
    val rows = result.rowsAs[JsonObject].collectSeq().block()
    assert(1 == rows.size)
    assert(FooContent == rows(0))
  }

  @Test
  def handlesPreparedStatementsWithNamedArgs(): Unit = {
    val id: String = insertDoc
    val options: QueryOptions = QueryOptions()
      .scanConsistency(QueryScanConsistency.RequestPlus())
      .adhoc(false)
      .namedParameters(Map("id" -> id))
    val result: QueryResult = cluster.query("select `" + bucketName + "`.* from `" + bucketName + "` where meta()" +
      ".id=$id", options).get
    val rows = result.rowsAs[JsonObject].get
    assert(1 == rows.size)
    assert(FooContent == rows(0))
  }

  @Test
  def handlesPreparedStatementsWithPositionalArgs(): Unit = {
    val id: String = insertDoc
    val options: QueryOptions = QueryOptions()
      .scanConsistency(QueryScanConsistency.RequestPlus())
      .positionalParameters(Seq(id))
    val st = "select `" + bucketName + "`.* from `" + bucketName + "` where meta().id=$1"
    val result: QueryResult = cluster.query(st, options).get
    val rows = result.rowsAs[JsonObject].get
    assert(1 == rows.size)
    assert(FooContent == rows(0))
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
    val mt = coll.upsert(id, FooContent).get

    val options: QueryOptions = QueryOptions()
      .scanConsistency(ConsistentWith(MutationState.from(mt)))

    val st = "select `" + bucketName + "`.* from `" + bucketName + "` where meta().id=\"" + id + "\""
    val result: QueryResult = cluster.query(st, options).get

    val rows = result.rowsAs[JsonObject].get
    assert(1 == rows.size)
  }


  // SCBC-70
  @Test
  def caseClassesDecodedToN1QL(): Unit = {
    case class Address(line1: String)
    case class User(name: String, age: Int, addresses: Seq[Address])
    object User {
      implicit val codec: Codec[User] = Codec.codec[User]
    }

    val user = User("user1", 21, Seq(Address("address1")))
    val result = coll.upsert("user1", user).get

    val statement = s"""select * from `${bucketName}` where meta().id like 'user%';"""

    cluster.query(statement, QueryOptions().scanConsistency(ConsistentWith(MutationState.from(result))))
      .flatMap(_.rowsAs[JsonObject]) match {
      case Success(rows: Seq[JsonObject]) =>
        assert(rows.nonEmpty)
        rows.foreach(row => println(row))
      case Failure(err) =>
        println(s"Error: $err")
    }
  }
}
