package com.couchbase.client.scala.encodings

import com.couchbase.client.scala._
import com.couchbase.client.scala.codec.RawJsonTranscoder
import com.couchbase.client.scala.implicits.Codec
import com.couchbase.client.scala.json.{JsonArray, JsonObject}
import com.couchbase.client.scala.kv.{GetOptions, InsertOptions}
import com.couchbase.client.scala.util.ScalaIntegrationTest
import io.circe.Decoder
import com.github.plokhotnyuk.jsoniter_scala.macros.named
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test, TestInstance}

/**
  * The client supports multiple Json libraries.  These tests are to ensure that whatever we write with one lib,
  * can be read by another (and therefore hopefully any other Couchbase SDK also).
  */

@TestInstance(Lifecycle.PER_CLASS)
class JsonInteropSpec extends ScalaIntegrationTest {

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

  trait Source {
    def insert(id: String): Unit
  }

  case class Address(address: String)

  object Address {
    implicit val rw: upickle.default.ReadWriter[Address] = upickle.default.macroRW
    implicit val decoder: io.circe.Decoder[Address] =
      io.circe.generic.semiauto.deriveDecoder[Address]
    implicit val encoder: io.circe.Encoder[Address] =
      io.circe.generic.semiauto.deriveEncoder[Address]
  }

  case class User(name: String, age: Int, addresses: Seq[Address])

  object User {
    implicit val codec: Codec[User]                   = Codec.codec[User]
    implicit val rw: upickle.default.ReadWriter[User] = upickle.default.macroRW

    import com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec
    import com.github.plokhotnyuk.jsoniter_scala.macros.{CodecMakerConfig, JsonCodecMaker}
    implicit val codecJsoniter: JsonValueCodec[User] = JsonCodecMaker.make[User](CodecMakerConfig)

    implicit val decoder: io.circe.Decoder[User] = io.circe.generic.semiauto.deriveDecoder[User]
    implicit val encoder: io.circe.Encoder[User] = io.circe.generic.semiauto.deriveEncoder[User]
  }

  val ReferenceUser = User("John Smith", 29, List(Address("123 Fake Street")))

  case class User2(
      @named("name") n: String,
      @named("age") a: Int,
      @named("addresses") s: Seq[Address]
  )
  object User2 {
    implicit val codec: Codec[User2] = Codec.codec
  }
  val ReferenceUser2 = User2("John Smith", 29, List(Address("123 Fake Street")))

  object Source {

    case object JsonObjectAST extends Source {
      def insert(id: String): Unit = {
        val content = JsonObject.create
          .put("name", "John Smith")
          .put("age", 29)
          .put(
            "addresses",
            JsonArray(
              JsonObject.create.put("address", "123 Fake Street")
            )
          )
        assert(coll.insert(id, content).isSuccess)
      }
    }

    case object UpickleAST extends Source {
      def insert(id: String): Unit = {
        val content = ujson.Obj(
          "name" -> "John Smith",
          "age"  -> 29,
          "addresses" -> ujson.Arr(
            ujson.Obj("address" -> "123 Fake Street")
          )
        )
        assert(coll.insert(id, content).isSuccess)
      }
    }

    case object UpickleCaseClassToBytes extends Source {
      def insert(id: String): Unit = {
        import ujson.BytesRenderer
        import upickle.default._

        val bytes: Array[Byte] = transform(ReferenceUser).to(BytesRenderer()).toBytes
        assert(
          coll.insert(id, bytes, InsertOptions().transcoder(RawJsonTranscoder.Instance)).isSuccess
        )
      }
    }

    case object JsonIterCaseClass extends Source {
      def insert(id: String): Unit = {
        import User._
        import com.github.plokhotnyuk.jsoniter_scala.core._

        assert(
          coll
            .insert(
              id,
              writeToArray(ReferenceUser),
              InsertOptions().transcoder(RawJsonTranscoder.Instance)
            )
            .isSuccess
        )
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
    case object CouchbaseEncodedCaseClassWithFieldNameAlias extends Source {
      def insert(id: String): Unit = {
        assert(coll.insert(id, ReferenceUser2).isSuccess)
      }
    }

    case object JacksonEncodedString extends Source {
      def insert(id: String): Unit = {
        import com.fasterxml.jackson.databind.ObjectMapper
        import com.fasterxml.jackson.module.scala.DefaultScalaModule

        val mapper = new ObjectMapper()
        mapper.registerModule(DefaultScalaModule)
        val json = mapper.writeValueAsString(ReferenceUser)

        assert(
          coll.insert(id, json, InsertOptions().transcoder(RawJsonTranscoder.Instance)).isSuccess
        )
      }
    }

    case object JacksonEncodedCaseClass extends Source {
      def insert(id: String): Unit = {
        import com.fasterxml.jackson.databind.ObjectMapper
        import com.fasterxml.jackson.module.scala.DefaultScalaModule

        val mapper = new ObjectMapper()
        mapper.registerModule(DefaultScalaModule)
        val bytes = mapper.writeValueAsBytes(ReferenceUser)

        assert(
          coll.insert(id, bytes, InsertOptions().transcoder(RawJsonTranscoder.Instance)).isSuccess
        )
      }
    }

    case object CirceAST extends Source {

      import io.circe.syntax._

      def insert(id: String): Unit = {
        val json: io.circe.Json = ReferenceUser.asJson
        assert(coll.insert(id, json).isSuccess)
      }
    }

    case object HardCodedString extends Source {
      def insert(id: String): Unit = {
        val json =
          """{"name":"John Smith",
            |"age":29,
            |"addresses":[{"address":"123 Fake Street"}]}""".stripMargin
        assert(
          coll.insert(id, json, InsertOptions().transcoder(RawJsonTranscoder.Instance)).isSuccess
        )
      }
    }

    case object PlayAST extends Source {

      import play.api.libs.json.Json._

      def insert(id: String): Unit = {
        val json = obj(
          "name"      -> "John Smith",
          "age"       -> 29,
          "addresses" -> arr(obj("address" -> "123 Fake Street"))
        )

        assert(coll.insert(id, json).isSuccess)
      }
    }

    case object JawnAST extends Source {

      import org.typelevel.jawn.ast._

      def insert(id: String): Unit = {
        val json = JObject.fromSeq(
          Seq(
            "name" -> JString("John Smith"),
            "age"  -> JNum(29),
            "addresses" -> JArray
              .fromSeq(Seq(JObject.fromSeq(Seq("address" -> JString("123 Fake Street")))))
          )
        )

        assert(coll.insert(id, json).isSuccess)
      }
    }

    case object Json4sAST extends Source {

      import org.json4s.JsonAST._
      import org.json4s.JsonDSL._

      def insert(id: String): Unit = {
        val json: JValue =
          ("name"        -> "John Smith") ~
            ("age"       -> 29) ~
            ("addresses" -> List("address" -> "123 Fake Street"))

        assert(coll.insert(id, json).isSuccess)
      }
    }

  }

  trait Sink {
    def decode(docId: String): Unit
  }

  object Sink {

    case object JsonObjectAST extends Sink {
      def decode(docId: String): Unit = {
        val in = coll.get(docId).get
        val c  = in.contentAs[JsonObject].get
        assert(c.str("name") == "John Smith")
        assert(c.num("age") == 29)
        assert(c.arr("addresses").obj(0).str("address") == "123 Fake Street")
      }
    }

    case object UpickleAST extends Sink {
      def decode(docId: String): Unit = {
        val in = coll.get(docId).get
        val c  = in.contentAs[ujson.Obj].get
        assert(c("name").str == "John Smith")
        assert(c("age").num == 29)
        assert(c("addresses").arr(0)("address").str == "123 Fake Street")
      }
    }

    case object Jsoniter extends Sink {
      def decode(docId: String): Unit = {
        val in = coll.get(docId, GetOptions().transcoder(RawJsonTranscoder.Instance)).get
        val c = com.github.plokhotnyuk.jsoniter_scala.core
          .readFromArray[User](in.contentAs[Array[Byte]].get)
        assert(c == ReferenceUser)
      }
    }

    case object Upickle extends Sink {
      def decode(docId: String): Unit = {
        val in = coll.get(docId, GetOptions().transcoder(RawJsonTranscoder.Instance)).get
        val c  = upickle.default.read[User](in.contentAs[Array[Byte]].get)
        assert(c == ReferenceUser)
      }
    }

    case object CouchbaseCaseClass extends Sink {
      def decode(docId: String): Unit = {
        val in = coll.get(docId).get
        val c  = in.contentAs[User].get
        assert(c == ReferenceUser)
      }
    }
    case object CouchbaseCaseClassWithFieldNameAlias extends Sink {
      def decode(docId: String): Unit = {
        val in = coll.get(docId).get
        val c  = in.contentAs[User2].get
        assert(c == ReferenceUser2)
      }
    }

    case object Jackson extends Sink {
      import com.fasterxml.jackson.databind.ObjectMapper
      import com.fasterxml.jackson.module.scala.DefaultScalaModule

      def decode(docId: String): Unit = {
        val in     = coll.get(docId, GetOptions().transcoder(RawJsonTranscoder.Instance)).get
        val mapper = new ObjectMapper()
        mapper.registerModule(DefaultScalaModule)
        val c = mapper.readValue(in.contentAs[Array[Byte]].get, classOf[User])
        assert(c == ReferenceUser)
      }
    }

    case object CirceAST extends Sink {
      def decode(docId: String): Unit = {
        val in = coll.get(docId).get
        val c  = in.contentAs[io.circe.Json].get
        def equals[A](r: Decoder.Result[A], value: A) = r match {
          case Left(_)  => false
          case Right(a) => a == a
        }
        assert(equals(c.hcursor.downField("name").as[String], "John Smith"))
        assert(equals(c.hcursor.downField("age").as[Int], 29))
        assert(
          equals(
            c.hcursor.downField("addresses").downArray.downField("address").as[String],
            "123 Fake Street"
          )
        )
      }
    }

    case object String extends Sink {
      def decode(docId: String): Unit = {
        val in  = coll.get(docId, GetOptions().transcoder(RawJsonTranscoder.Instance)).get
        val raw = in.contentAs[String].get

        val c = upickle.default.read[ujson.Obj](raw)
        assert(c("name").str == "John Smith")
        assert(c("age").num == 29)
        assert(c("addresses").arr(0)("address").str == "123 Fake Street")
      }
    }

    case object PlayAST extends Sink {
      def decode(docId: String): Unit = {
        val in         = coll.get(docId).get
        val c          = in.contentAs[play.api.libs.json.JsValue].get
        val addressOpt = (c \ "addresses" \ 0 \ "address")
        val address    = addressOpt.get
        assert(c("name").as[String] == "John Smith")
        assert(c("age").as[Int] == 29)
        assert(address.as[String] == "123 Fake Street")
      }
    }

    case object JawnAST extends Sink {

      import org.typelevel.jawn.ast._

      def decode(docId: String): Unit = {
        val in = coll.get(docId).get
        val c  = in.contentAs[org.typelevel.jawn.ast.JValue].get
        assert(c.get("name").asString == "John Smith")
        assert(c.get("age").asInt == 29)
        assert(
          c.get("addresses")
            .asInstanceOf[JArray]
            .get(0)
            .get("address")
            .asString == "123 Fake Street"
        )
      }
    }

    case object Json4sAST extends Sink {

      import org.json4s.JsonAST._

      def decode(docId: String): Unit = {
        val in            = coll.get(docId).get
        val c             = in.contentAs[JValue].get
        val JString(name) = c \ "name"
        assert(name.toString == "John Smith")
        val JInt(age) = c \ "age"
        assert(age.intValue == 29)
        val JString(address) = (c \ "addresses")(0) \ "address"
        assert(address.toString == "123 Fake Street")
      }
    }

  }

  @Test
  def test_all_permutations(): Unit = {
    val sources = Seq(
      Source.JsonObjectAST,
      Source.UpickleAST,
      Source.UpickleCaseClassToBytes,
      Source.JsonIterCaseClass,
      Source.UpickleCaseClassToAST,
      Source.CouchbaseEncodedCaseClass,
      Source.CouchbaseEncodedCaseClassWithFieldNameAlias,
      //      Source.JacksonEncodedString,
      //      Source.JacksonEncodedCaseClass,
      Source.CirceAST,
      Source.HardCodedString,
      Source.PlayAST,
      Source.JawnAST,
      Source.Json4sAST
    )
    val sinks = Seq(
      Sink.JsonObjectAST,
      Sink.UpickleAST,
      Sink.Jsoniter,
      Sink.Upickle,
      Sink.CouchbaseCaseClass,
      Sink.CouchbaseCaseClassWithFieldNameAlias,
      //      Sink.Jackson,
      Sink.CirceAST,
      Sink.String,
      Sink.PlayAST,
      Sink.JawnAST,
      Sink.Json4sAST
    )

    sources.foreach(source => {
      sinks.foreach(sink => {
        compare(source, sink)
      })
    })
  }

  private def compare(source: Source, sink: Sink): Unit = {
    val docId = TestUtils.docId()

    source.insert(docId)
    sink.decode(docId)
  }

  @Test
  def JacksonEncodedString_to_PlayAST(): Unit = {
    val source = Source.JacksonEncodedString
    val sink   = Sink.PlayAST
    compare(source, sink)
  }

  @Test
  def JsonObjectAST_to_JawnAST(): Unit = {
    val source = Source.JsonObjectAST
    val sink   = Sink.JawnAST
    compare(source, sink)
  }

  @Test
  def CirceAST_to_JsonObjectAST(): Unit = {
    val source = Source.CirceAST
    val sink   = Sink.JsonObjectAST
    compare(source, sink)
  }

  @Test
  def CirceAST_to_Json4sAST(): Unit = {
    val source = Source.CirceAST
    val sink   = Sink.Json4sAST
    compare(source, sink)
  }

  @Test
  def JsonObjectAST_to_Jsoniter(): Unit = {
    val source = Source.JsonObjectAST
    val sink   = Sink.Jsoniter
    compare(source, sink)
  }

  @Test
  def UpickleCaseClassToBytes_to_JsonObjectAST(): Unit = {
    val source = Source.UpickleCaseClassToBytes
    val sink   = Sink.JsonObjectAST
    compare(source, sink)
  }
}
