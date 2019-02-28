package com.couchbase.client.scala.encodings

import com.couchbase.client.scala.json.{JsonArray, JsonObject}
import com.couchbase.client.scala.kv.GetResult
import com.couchbase.client.scala.{Address, Cluster, TestUtils, User}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{Matchers, _}

import scala.util.{Failure, Success}


/**
  * The client supports multiple Json libraries.  These tests are to ensure that whatever we write with one lib,
  * can be read by another (and therefore hopefully any other Couchbase SDK also).
  */
class JsonInteropSpec extends FunSuite with Matchers with BeforeAndAfterAll with BeforeAndAfter with GeneratorDrivenPropertyChecks {

    val cluster = Cluster.connect("localhost", "Administrator", "password")
    val bucket = cluster.bucket("default")
    val coll = bucket.defaultCollection


  before {
  }

  trait Source {
    def insert(id: String)
  }

  val ReferenceUser = User("John Smith", 29, List(Address("123 Fake Street")))

  object Source {

    case object JsonObjectAST extends Source {
      def insert(id: String) {
        val content = JsonObject.create
          .put("name", "John Smith")
          .put("age", 29)
          .put("address", JsonArray(
            JsonObject.create.put("address", "123 Fake Street")
          ))
        assert(coll.insert(id, content).isSuccess)
      }
    }

    case object UpickleAST extends Source {
      def insert(id: String) {
        val content = ujson.Obj("name" -> "John Smith",
          "age" -> 29,
          "address" -> ujson.Arr(
            ujson.Obj("address" -> "123 Fake Street")
          ))
        assert(coll.insert(id, content).isSuccess)
      }
    }

    case object UpickleCaseClassToBytes extends Source {
      def insert(id: String): Unit = {
        import ujson.BytesRenderer
        import upickle.default._

        val bytes: Array[Byte] = transform(ReferenceUser).to(BytesRenderer()).toBytes
        assert(coll.insert(id, bytes).isSuccess)
      }
    }

    case object JsonIterCaseClass extends Source {
      def insert(id: String): Unit = {
        import User._
        import com.github.plokhotnyuk.jsoniter_scala.core._

        assert(coll.insert(id, writeToArray(ReferenceUser)).isSuccess)
      }
    }

    case object UpickleCaseClassToAST extends Source {
      def insert(id: String): Unit = {
        val encoded: ujson.Value = upickle.default.writeJs(ReferenceUser)

        assert(coll.insert(id, encoded).isSuccess)
      }
    }

    case object CouchbaseEncodedCaseClass extends Source {
      def insert(id: String): Unit = {
        assert(coll.insert(id, ReferenceUser).isSuccess)
      }
    }

    case object JacksonEncodedString extends Source {
      def insert(id: String): Unit = {
        import com.fasterxml.jackson.databind.ObjectMapper
        import com.fasterxml.jackson.module.scala.DefaultScalaModule

        val mapper = new ObjectMapper()
        mapper.registerModule(DefaultScalaModule)

        assert(coll.insert(id, mapper.writeValueAsString(ReferenceUser)).isSuccess)
      }
    }

    case object JacksonEncodedCaseClass extends Source {
      def insert(id: String): Unit = {
        import com.fasterxml.jackson.databind.ObjectMapper
        import com.fasterxml.jackson.module.scala.DefaultScalaModule

        val mapper = new ObjectMapper()
        mapper.registerModule(DefaultScalaModule)

        assert(coll.insert(id, mapper.writeValueAsBytes(ReferenceUser)).isSuccess)
      }
    }

    case object CirceAST extends Source {

      import io.circe.syntax._

      def insert(id: String): Unit = {
        val json = ReferenceUser.asJson
        assert(coll.insert(id, json).isSuccess)
      }
    }

    case object HardCodedString extends Source {
      def insert(id: String): Unit = {
        val json =
          """{"name":"John Smith",
            |"age":29,
            |"address":[{"address":"123 Fake Street"}]}""".stripMargin
        assert(coll.insert(id, json).isSuccess)
      }
    }

    case object PlayAST extends Source {

      import play.api.libs.json.Json._

      def insert(id: String): Unit = {
        val json = obj("name" -> "John Smith",
          "age" -> 29,
          "address" -> arr(obj("address" -> "123 Fake Street")))

        assert(coll.insert(id, json).isSuccess)
      }
    }

    case object JawnAST extends Source {

      import org.typelevel.jawn.ast._

      def insert(id: String): Unit = {
        val json = JObject.fromSeq(Seq("name" -> JString("John Smith"),
          "age" -> JNum(29),
          "address" -> JArray.fromSeq(Seq(JObject.fromSeq(Seq("address" -> JString("123 Fake Street")))))))

        assert(coll.insert(id, json).isSuccess)
      }
    }

    case object Json4sAST extends Source {

      import play.api.libs.json.Json._

      def insert(id: String): Unit = {
        val json = obj("name" -> "John Smith",
          "address" -> arr(obj("address" -> "123 Fake Street")),
          "age" -> 29)

        assert(coll.insert(id, json).isSuccess)
      }
    }


  }


  trait Sink {
    def decode(in: GetResult)
  }

  object Sink {

    case object JsonObjectAST extends Sink {
      def decode(in: GetResult): Unit = {
        val c = in.contentAs[JsonObject].get
        assert(c.str("name") == "John Smith")
        assert(c.num("age") == 29)
        assert(c.arr("address").obj(0).str("address") == "123 Fake Street")
      }
    }

    case object UpickleAST extends Sink {
      def decode(in: GetResult): Unit = {
        val c = in.contentAs[ujson.Obj].get
        assert(c("name").str == "John Smith")
        assert(c("age").num == 29)
        assert(c("address").arr(0)("address").str == "123 Fake Street")
      }
    }

    case object Jsoniter extends Sink {
      def decode(in: GetResult): Unit = {
        val c = com.github.plokhotnyuk.jsoniter_scala.core.readFromArray[User](in.contentAsBytes)
        assert(c == ReferenceUser)
      }
    }

    case object Upickle extends Sink {
      def decode(in: GetResult): Unit = {
        val c = upickle.default.read[User](in.contentAsBytes)
        assert(c == ReferenceUser)
      }
    }

    case object CouchbaseCaseClass extends Sink {
      def decode(in: GetResult): Unit = {
        val c = in.contentAs[User].get
        assert(c == ReferenceUser)
      }
    }

    case object Jackson extends Sink {
      def decode(in: GetResult): Unit = {
        val mapper = new ObjectMapper()
        mapper.registerModule(DefaultScalaModule)
        val c = mapper.readValue(in.contentAsBytes, classOf[User])
        assert(c == ReferenceUser)
      }
    }

    case object CirceAST extends Sink {
      def decode(in: GetResult): Unit = {
        val c = in.contentAs[io.circe.Json].get
        assert(c.hcursor.downField("name").as[String].right.get == "John Smith")
        assert(c.hcursor.downField("age").as[Int].right.get == 29)
        assert(c.hcursor.downField("address").downArray.downField("address").as[String].right.get == "123 Fake Street")
      }
    }


    case object String extends Sink {
      def decode(in: GetResult): Unit = {
        val raw = in.contentAs[String].get

        val c = upickle.default.read[ujson.Obj](raw)
        assert(c("name").str == "John Smith")
        assert(c("age").num == 29)
        assert(c("address").arr(0)("address").str == "123 Fake Street")
      }
    }

    case object PlayAST extends Sink {
      def decode(in: GetResult): Unit = {
        val c = in.contentAs[play.api.libs.json.JsValue].get
        val address = (c \ "address" \ 0 \ "address").get
        assert(c("name").as[String] == "John Smith")
        assert(c("age").as[Int] == 29)
        assert(address.as[String] == "123 Fake Street")
      }
    }

    case object JawnAST extends Sink {

      import org.typelevel.jawn.ast._

      def decode(in: GetResult): Unit = {
        val c = in.contentAs[JValue].get
        assert(c.get("name").asString == "John Smith")
        assert(c.get("age").asInt == 29)
        assert(c.get("address").asInstanceOf[JArray].get(0).get("address").asString == "123 Fake Street")
      }
    }

    case object Json4sAST extends Sink {

      import org.json4s.JsonAST._

      def decode(in: GetResult): Unit = {
        val c = in.contentAs[JValue].get
        val JString(name) = c \ "name"
        assert(name.toString == "John Smith")
        val JInt(age) = c \ "age"
        assert(age.intValue() == 29)
        val JString(address) = (c \ "address") (0) \ "address"
        assert(address.toString == "123 Fake Street")
      }
    }

  }

  test("test all permutations") {
    val sources: Gen[Source] = Gen.oneOf(Seq(
      Source.JsonObjectAST,
      Source.UpickleAST,
      Source.UpickleCaseClassToBytes,
      Source.JsonIterCaseClass,
      Source.UpickleCaseClassToAST,
      Source.CouchbaseEncodedCaseClass,
      Source.JacksonEncodedString,
      Source.JacksonEncodedCaseClass,
      Source.CirceAST,
      Source.HardCodedString,
      Source.PlayAST,
      Source.JawnAST,
      Source.Json4sAST
    ))
    val sinks: Gen[Sink] = Gen.oneOf(Seq(
      Sink.JsonObjectAST,
      Sink.UpickleAST,
      Sink.Jsoniter,
      Sink.Upickle,
      Sink.CouchbaseCaseClass,
      Sink.Jackson,
      Sink.CirceAST,
      Sink.String,
      Sink.PlayAST,
      Sink.JawnAST,
      Sink.Json4sAST
    ))

    implicit lazy val arbSource: Arbitrary[Source] = Arbitrary(sources)
    implicit lazy val arbSink: Arbitrary[Sink] = Arbitrary(sinks)

    forAll("insert doc with", "read doc with") { (a: Source, b: Sink) =>
      val docId = TestUtils.docId()

      a.insert(docId)
      val result = coll.get(docId).get
      b.decode(result)
    }
  }

  private def compare(source: Source, sink: Sink): Unit = {
    val docId = TestUtils.docId()

    source.insert(docId)
    val result = coll.get(docId).get
    sink.decode(result)
  }

  test("JacksonEncodedString to PlayAST") {
    val source = Source.JacksonEncodedString
    val sink = Sink.PlayAST
    compare(source, sink)
  }

  test("JsonObjectAST to JawnAST") {
    val source = Source.JsonObjectAST
    val sink = Sink.JawnAST
    compare(source, sink)
  }

  test("CirceAST to JsonObjectAST") {
    val source = Source.CirceAST
    val sink = Sink.JsonObjectAST
    compare(source, sink)
  }

  test("CirceAST to Json4sAST") {
    val source = Source.CirceAST
    val sink = Sink.Json4sAST
    compare(source, sink)
  }


}