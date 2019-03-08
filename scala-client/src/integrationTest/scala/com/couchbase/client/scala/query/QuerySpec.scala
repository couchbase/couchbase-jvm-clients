package com.couchbase.client.scala.query

import com.couchbase.client.core.error.{DecodingFailedException, QueryStreamException}
import com.couchbase.client.scala.json.JsonObject
import com.couchbase.client.scala.{Cluster, TestUtils}
import org.scalatest.FunSuite
import reactor.core.scala.publisher.Flux

import scala.util.{Failure, Success}

class QuerySpec extends FunSuite {

  val cluster = Cluster.connect("localhost", "Administrator", "password")
  val bucket = cluster.bucket("default")
  val coll = bucket.defaultCollection


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

  test("hello world") {
    cluster.query("""select 'hello world' as Greeting""") match {
      case Success(result) =>
        assert(result.rows.size == 1)
        assert(result.rows.head.contentAs[String].get == """{"Greeting":"hello world"}""")
      case Failure(err) => throw err
    }
  }

  test("hello world content as JsonObject") {
    cluster.query("""select 'hello world' as Greeting""") match {
      case Success(result) =>
        assert(result.clientContextId.isEmpty)
        assert(result.requestId != null)
        assert(result.rows.size == 1)
        assert(result.rows.head.contentAs[JsonObject].get.str("Greeting") == "hello world")
        val signature = result.signature.contentAs[JsonObject].get
        assert (signature.size > 0)
      case Failure(err) => throw err
    }
  }

  test("hello world with quotes") {
    cluster.query("""select "hello world" as Greeting""") match {
      case Success(result) =>
        assert(result.rows.size == 1)
        assert(result.rows.head.contentAs[String].get == """{"Greeting":"hello world"}""")
      case Failure(err) => throw err
    }
  }

  test("read 2 docs use keys") {
    val (docId1, _) = prepare(ujson.Obj("name" -> "Andy"))
    val (docId2, _) = prepare(ujson.Obj("name" -> "Beth"))

    val statement =s"""select name from default use keys ['$docId1', '$docId2'];"""
    //    val statement = s"""SELECT * FROM default USE KEYS '$docId1';"""
    cluster.query(statement) match {
      case Success(result) =>
        val rows = result.rows.toSeq
        assert(rows.size == 2)
        assert(rows.head.contentAs[ujson.Obj].get("name").str == """Andy""")
        assert(rows.last.contentAs[ujson.Obj].get("name").str == """Beth""")
      case Failure(err) =>
        throw err
    }
  }

  test("error due to bad syntax") {
    cluster.query("""select*from""") match {
      case Success(result) =>
        assert(false)
      case Failure(err: QueryError) =>
        println(err)
      case Failure(err) =>
        throw err
    }
  }
  //
  //  test("reactive hello world demo") {
  //    cluster.reactive.query("""select 'hello world' as Greeting""")
  //      .flatMapMany(result => {
  //        result.rows
  //          .doOnNext(row =>
  //            // Do something with each row
  //            println(row.contentAs[JsonObject]))
  //
  //          // Errors are raised on the rows flux
  //          .doOnError(err => println(err))
  //
  //          // Make sure to handle metrics & warnings *after* rows, so rows don't buffer
  //          .thenMany(result.other.doOnNext(other => {
  //            // Optionally: do something with metrics & warnings
  //            println(other.metrics)
  //            println(other.warnings)
  //          }))
  //      })
  //      .subscribe()
  //  }

  test("reactive hello world") {
    val rows: Seq[QueryRow] = cluster.reactive.query("""select 'hello world' as Greeting""")
      .flatMapMany(result => {
        result.rows.doOnNext(v => {
          println("GOT A ROW!!" + v)
        }).collectSeq()

        //          .doOnError(err => {
        //          assert(false)
        //        })
      })
      .blockLast()

    assert(rows.size == 1)

    //    val x = cluster.reactive.query("""select 'hello world' as Greeting""")
    //      .flatMapMany(result => result.rows)
    //      .collectList()
    //      .block()
    //
    //
    //        val rows: java.util.List[QueryRow] = cluster.reactive.query("""select 'hello world' as Greeting""")
    //      .flatMap(result => {
    //        val x = result.rows.doOnNext(v => {
    //          println("GOT A ROW!!" + v)
    //        }).collectList()
    //
    //        x
    //
    //        //          .doOnError(err => {
    //        //          assert(false)
    //        //        })
    //      })
    //      .block()
    //
    //    print(x.size)
    //    print(rows.size)
    //    assert(x.size() == 1)
    //    assert(rows.size() == 1)
    //    asse
    //    rt(rows.size == 1)
  }

  test("reactive error due to bad syntax") {
    assertThrows[QueryError](
    cluster.reactive.query("""sselect*from""")
      .flatMapMany(result => {
        result.rows
          .doOnNext(v => assert(false))
          .doOnError(err => println("expected ERR: " + err))
      })
      .blockLast())
  }

}
