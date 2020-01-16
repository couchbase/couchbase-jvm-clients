/*
 * Copyright (c) 2019 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.scala.query

import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

import com.couchbase.client.core.error.{ParsingFailureException, QueryException}
import com.couchbase.client.core.service.ServiceType
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

  private var cluster: Cluster   = _
  private var coll: Collection   = _
  private var bucketName: String = _

  @BeforeAll
  def beforeAll(): Unit = {
    cluster = connectToCluster()
    val bucket = cluster.bucket(config.bucketname)
    coll = bucket.defaultCollection

    cluster.queryIndexes.createPrimaryIndex(config.bucketname).get
    cluster.queryIndexes
      .watchIndexes(config.bucketname, Seq(), Duration(1, TimeUnit.MINUTES), watchPrimary = true)
      .get

    bucketName = config.bucketname()
    TestUtils.waitForService(bucket, ServiceType.QUERY)
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
    val docId        = TestUtils.docId()
    val insertResult = coll.insert(docId, content).get
    (docId, insertResult.mutationToken)
  }

  @Test
  def hello_world(): Unit = {
    cluster.query("""select 'hello world' as Greeting""") match {
      case Success(result) =>
        assert(result.rows.size == 1)
        import com.couchbase.client.scala.codec.JsonDeserializer.Passthrough._
        val rows = result.rowsAs[String].get
        assert(rows.head == """{"Greeting":"hello world"}""")
        // Should be an implicit client context id if none provided
        assert(result.metaData.clientContextId != "")
        assert(result.metaData.requestId != "")
      case Failure(err) => throw err
    }
  }

  @Test
  def hello_world_content_as_JsonObject(): Unit = {
    cluster.query("""select 'hello world 2' as Greeting""", QueryOptions().metrics(true)) match {
      case Success(result) =>
        assert(result.metaData.clientContextId != "")
        assert(result.metaData.requestId != null)
        assert(result.rows.size == 1)
        val rows = result.rowsAs[JsonObject].get
        assert(rows.head.str("Greeting") == "hello world 2")
        val signature = result.metaData.signatureAs[JsonObject].get
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
  def rawOptions(): Unit = {
    cluster.query(
      """select 'hello world' as Greeting""",
      QueryOptions().raw(Map("metrics" -> true))
    ) match {
      case Success(result) =>
        val out = result.metaData
        assert(out.metrics.isDefined)
      case Failure(err) => throw err
    }
  }

  @Test
  def hello_world_content_as_JsonObject_for_comp(): Unit = {
    (for {
      result <- cluster.query(
        """select 'hello world 2' as Greeting""",
        QueryOptions().metrics(true)
      )
      rows <- result.rowsAs[JsonObject]
    } yield (result, rows)) match {
      case Success((result, rows)) =>
        assert(result.rows.size == 1)
        val rows = result.rowsAs[JsonObject].get
        assert(rows.head.str("Greeting") == "hello world 2")
        val signature = result.metaData.signatureAs[JsonObject].get
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
  def hello_world_with_quotes(): Unit = {
    cluster.query("""select "hello world" as Greeting""") match {
      case Success(result) =>
        assert(result.rows.size == 1)
        import com.couchbase.client.scala.codec.JsonDeserializer.Passthrough._
        val rows = result.rowsAs[String].get
        assert(rows.head == """{"Greeting":"hello world"}""")
      case Failure(err) => throw err
    }
  }

  @Test
  def read_2_docs_use_keys(): Unit = {
    val (docId1, _) = prepare(ujson.Obj("name" -> "Andy"))
    val (docId2, _) = prepare(ujson.Obj("name" -> "Beth"))

    val statement = s"""select name from `${bucketName}` use keys ['$docId1', '$docId2'];"""
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
  def error_due_to_bad_syntax(): Unit = {
    val x = cluster.query("""select*from""")
    x match {
      case Success(result)                       => assert(false)
      case Failure(err: ParsingFailureException) =>
      case Failure(err)                          => throw err
    }
  }

  @Test
  def reactive_hello_world(): Unit = {
    import com.couchbase.client.scala.codec.JsonDeserializer.Passthrough._

    cluster.reactive
      .query("""select 'hello world' as Greeting""")
      .flatMap(result => {
        result
          .rowsAs[String]
          .doOnNext(v => {
            println("GOT A ROW!!" + v)
          })
          .collectSeq()
          .doOnNext(rows => assert(rows.size == 1))
          .flatMap(_ => result.metaData)
          .doOnNext(meta => {
            assert(meta.clientContextId != "")
          })
      })
      .block()
  }

  @Test
  def reactive_additional(): Unit = {

    val rowsKeeper = new AtomicReference[Seq[JsonObject]]()

    val out: QueryMetaData = cluster.reactive
      .query("""select 'hello world' as Greeting""", QueryOptions().metrics(true))
      .flatMapMany(result => {
        result
          .rowsAs[JsonObject]
          .collectSeq()
          .doOnNext(rows => {
            rowsKeeper.set(rows)
          })
          .flatMap(_ => result.metaData)
      })
      .blockLast()
      .get

    assert(rowsKeeper.get.size == 1)
    assert(out.metrics.get.errorCount == 0)
    assert(out.metrics.get.warningCount == 0)
    assert(out.metrics.get.mutationCount == 0)
    assert(out.warnings.size == 0)
    assert(out.status == QueryStatus.Success)
  }

  @Test
  def reactive_error_due_to_bad_syntax(): Unit = {
    Assertions.assertThrows(
      classOf[ParsingFailureException],
      () => {
        cluster.reactive
          .query("""sselect*from""")
          .flatMapMany(result => {
            result
              .rowsAs[String]
              .doOnNext(v => assert(false))
              .doOnError(err => println("expected ERR: " + err))
          })
          .blockLast()
      }
    )
  }

  @Test
  def options_profile(): Unit = {
    cluster.query(
      """select 'hello world' as Greeting""",
      QueryOptions().profile(QueryProfile.Timings)
    ) match {
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
  def options_named_params(): Unit = {
    coll.insert(TestUtils.docId(), JsonObject("name" -> "Eric Wimp"))

    cluster.query(
      s"""select * from `${bucketName}` where name=$$nval""",
      QueryOptions()
        .parameters(QueryParameters.Named("nval" -> "Eric Wimp"))
        .scanConsistency(QueryScanConsistency.RequestPlus())
    ) match {
      case Success(result) =>
        assert(result.rows.size > 0)
      case Failure(err) => throw err
    }
  }

  @Test
  def options_positional_params(): Unit = {
    coll.insert(TestUtils.docId(), JsonObject("name" -> "Eric Wimp"))

    cluster.query(
      s"""select * from `${bucketName}` where name=$$1""",
      QueryOptions()
        .parameters(QueryParameters.Positional("Eric Wimp"))
        .scanConsistency(QueryScanConsistency.RequestPlus())
    ) match {
      case Success(result) => {
        assert(result.rows.size > 0)
      }
      case Failure(err) => throw err
    }
  }

  @Test
  def options_clientContextId(): Unit = {
    cluster.query("""select 'hello world' as Greeting""", QueryOptions().clientContextId("test")) match {
      case Success(result) => assert(result.metaData.clientContextId.contains("test"))
      case Failure(err)    => throw err
    }
  }

  @Test
  def options_disableMetrics(): Unit = {
    cluster.query("""select 'hello world' as Greeting""", QueryOptions().metrics(true)) match {
      case Success(result) =>
        assert(result.metaData.metrics.get.errorCount == 0)
      case Failure(err) => throw err
    }
  }

  // Can't really test these so just make sure the server doesn't barf on our encodings
  @Test
  def options_unusual(): Unit = {
    cluster.query(
      """select 'hello world' as Greeting""",
      QueryOptions()
        .maxParallelism(5)
        .pipelineCap(3)
        .pipelineBatch(6)
        .scanCap(8)
        .timeout(30.seconds)
        .readonly(true)
    ) match {
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
    val result = cluster
      .query(
        "select `" + bucketName + "`.* from `" + bucketName + "` where meta()" +
          ".id=\"" + id + "\"",
        options
      )
      .get

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
    val future = cluster.async.query(
      "select `" + bucketName + "`.* from `" + bucketName + "` where meta()" +
        ".id=\"" + id + "\"",
      options
    )
    val result = Await.result(future, Duration.Inf)
    val rows   = result.rowsAs[JsonObject].get
    assert(1 == rows.size)
    assert(FooContent == rows(0))
  }

  @Test
  def handlesPreparedStatementsReactive(): Unit = {
    val id: String = insertDoc
    val options: QueryOptions = QueryOptions()
      .scanConsistency(QueryScanConsistency.RequestPlus())
      .adhoc(false)
    val mono: SMono[ReactiveQueryResult] = cluster.reactive.query(
      "select `" + bucketName + "`.* from `" + bucketName + "` where meta()" +
        ".id=\"" + id + "\"",
      options
    )
    val result = mono.block()
    val rows   = result.rowsAs[JsonObject].collectSeq().block()
    assert(1 == rows.size)
    assert(FooContent == rows(0))
  }

  @Test
  def handlesPreparedStatementsWithNamedArgs(): Unit = {
    val id: String = insertDoc
    val options: QueryOptions = QueryOptions()
      .scanConsistency(QueryScanConsistency.RequestPlus())
      .adhoc(false)
      .parameters(QueryParameters.Named("id" -> id))
    val result: QueryResult = cluster
      .query(
        "select `" + bucketName + "`.* from `" + bucketName + "` where meta()" +
          ".id=$id",
        options
      )
      .get
    val rows = result.rowsAs[JsonObject].get
    assert(1 == rows.size)
    assert(FooContent == rows(0))
  }

  @Test
  def handlesPreparedStatementsWithPositionalArgs(): Unit = {
    val id: String = insertDoc
    val options: QueryOptions = QueryOptions()
      .scanConsistency(QueryScanConsistency.RequestPlus())
      .parameters(QueryParameters.Positional(id))
    val st                  = "select `" + bucketName + "`.* from `" + bucketName + "` where meta().id=$1"
    val result: QueryResult = cluster.query(st, options).get
    val rows                = result.rowsAs[JsonObject].get
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

    val st                  = "select `" + bucketName + "`.* from `" + bucketName + "` where meta().id=\"" + id + "\""
    val result: QueryResult = cluster.query(st, options).get

    val rows = result.rowsAs[JsonObject].get
    assert(1 == rows.size)
  }

  case class Address(line1: String)
  // Need define case class & object here - not in caseClassesDecodedToN1QL method
  // or else, scala 2.11 will not compile, with error:
  // User is already defined as (compiler-generated) case class companion object User
  case class User(name: String, age: Int, addresses: Seq[Address])
  object User {
    implicit val codec: Codec[User] = Codec.codec[User]
  }
  // SCBC-70
  @Test
  def caseClassesDecodedToN1QL(): Unit = {
    val user   = User("user1", 21, Seq(Address("address1")))
    val result = coll.upsert("user1", user).get

    val statement = s"""select * from `${bucketName}` where meta().id like 'user%';"""

    cluster
      .query(statement, QueryOptions().scanConsistency(ConsistentWith(MutationState.from(result))))
      .flatMap(_.rowsAs[JsonObject]) match {
      case Success(rows) =>
        assert(rows.nonEmpty)
        rows.foreach(row => println(row))
      case Failure(err) =>
        println(s"Error: $err")
    }
  }

  @Test
  def reactiveQueryDoesNothingIfNotSubscribedTo(): Unit = {
    val docId = TestUtils.docId()
    cluster.reactive.query(
      s"""UPSERT INTO `${bucketName}` (KEY, VALUE) VALUES ("${docId}", { "type" : "hotel", "name" : "new hotel" })"""
    )
    Thread.sleep(50)
    assert(coll.get(docId).isFailure)
  }
}
