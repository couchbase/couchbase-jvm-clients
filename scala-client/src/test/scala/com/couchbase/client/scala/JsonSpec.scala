package com.couchbase.client.scala

import java.util.UUID

import com.couchbase.client.scala.document.Conversions.{Encodable, JsonEncodeParams}
import com.couchbase.client.scala.document.DecodeParams
import org.scalatest.{FlatSpec, Matchers, _}

import scala.util.{Failure, Success, Try}

class JsonSpec extends FlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfter {

  private val DocId = "doc"
  private val (cluster, bucket, coll) = (for {
    cluster <- Cluster.connect("localhost", "Administrator", "password")
    bucket <- cluster.bucket("default")
    coll <- bucket.defaultCollection()
  } yield (cluster, bucket, coll)) match {
    case Success(result) => result
    case Failure(err) => throw err
  }

  before {
    // TODO MVP bucketManager
//    bucket.bucketManager().flush()
    coll.remove(DocId)
  }

  "inserting upickle/ujson AST" should "succeed" in {
    val content = ujson.Obj("value" -> "INSERTED")
    assert(coll.insert(DocId, content).isSuccess)

    (for {
      doc <- coll.get(DocId)
      content <- doc.contentAs[ujson.Obj]

    } yield content) match {
      case Success(result: ujson.Obj) =>
        assert(result("value").str == "INSERTED")
      case Failure(err) => assert(false)
    }
  }

  case class Address(address: String)
  object Address {
    // upickle requires adding this implicit thing to support conversion to/from JSON.  It is at least not much
    // hassle to write.
    implicit val rw: upickle.default.ReadWriter[Address] = upickle.default.macroRW
  }

  case class User(name: String, age: Int, address: Seq[Address])
  object User {
    implicit val rw: upickle.default.ReadWriter[User] = upickle.default.macroRW
  }

  "inserting case class with upickle/ujson via Array[Byte]" should "succeed" in {
    import ujson.BytesRenderer
    import upickle.default._

    val content = User("John Smith", 29, List(Address("123 Fake Street")))

    val bytes: Array[Byte] = transform(content).to(BytesRenderer()).toBytes

    assert(coll.insert(DocId, bytes).isSuccess)

    (for {
      doc <- coll.get(DocId)
      content <- doc.contentAs[ujson.Obj]

    } yield content) match {
      case Success(result: ujson.Obj) =>
        assert(result("name").str == "John Smith")
      case Failure(err) => assert(false)
    }
  }


  "inserting case class with upickle/ujson via AST" should "succeed" in {
    import upickle.default._

    val content = User("John Smith", 29, List(Address("123 Fake Street")))

    // use upickle to do the conversion
    val encoded: ujson.Value = upickle.default.writeJs(content)

    assert(coll.insert(DocId, encoded).isSuccess)

    (for {
      doc <- coll.get(DocId)
      content <- Try(read[User](doc.contentAsBytes))

    } yield content) match {
      case Success(result: User) =>
        assert(result.name == "John Smith")
      case Failure(err) =>
        assert(false)
    }
  }


  "inserting case class using circe AST" should "succeed" in {
    import io.circe._, io.circe.generic.auto._, io.circe.parser._, io.circe.syntax._

    val content = User("John Smith", 29, List(Address("123 Fake Street")))
val json = content.asJson
    val bytes = json.as[Array[Byte]]
    json.
    coll.insert(DocId, json) match {
      case Success(v) =>
      case Failure(err) =>
        println(err)
        assert(false)
    }

//    (for {
//      doc <- coll.get(DocId)
//      content <- Try(decode[User](doc.contentAsBytes))
//
//    } yield content) match {
//      case Success(result: User) =>
//        assert(result.name == "John Smith")
//      case Failure(err) =>
//        assert(false)
//    }
  }

  def docId(idx: Int): String = {
    UUID.randomUUID().toString + "_" + idx
  }
}
