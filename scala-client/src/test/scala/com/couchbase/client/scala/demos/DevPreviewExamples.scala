package com.couchbase.client.scala.demos

import com.couchbase.client.scala.{Cluster, User}
import com.couchbase.client.scala.codec.Conversions.Codec
import com.couchbase.client.scala.durability.Durability
import com.couchbase.client.scala.implicits.Codecs
import com.couchbase.client.scala.json.{JsonArray, JsonObject}
import com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec
import com.github.plokhotnyuk.jsoniter_scala.macros.{CodecMakerConfig, JsonCodecMaker}
import org.scalatest.FunSuite

import scala.concurrent.duration._
import scala.util.{Failure, Success}

case class Person(name: String, age: Int, animals: Seq[String], attributes: Attributes)
case class Attributes(dimensions: Dimensions, hair: String, hobbies: Seq[Hobby])
case class Dimensions(height: Int, weight: Int)
case class Hobby(name: String, typ: String, details: Details)
case class Details(location: Location)
case class Location(long: Double, lat: Double)

object Person {
  // Add a codec to Person's companion class so Couchbase knows how to convert Person to/from JSON.  This conversion
  // is generated at compile-time so it can be very fast.  Note only a Codec for the top-level case class Person is needed.
  implicit val codec: Codec[Person] = Codecs.codec[Person]
}

class DevPreviewExamples extends FunSuite {

  test("1") {
    // Cluster Creation and Resource Selection
    val cluster = Cluster.connect("localhost", "Administrator", "password")
    val bucket = cluster.bucket("default")
    val coll = bucket.defaultCollection

    // Basic Key/Value Operations (CRUD)
    val json = JsonObject.create
      .put("name", "Joan Deere")
      .put("age", 28)
      .put("animals", JsonArray("kitty", "puppy"))

    coll.upsert("test_id", json) match {
      case Success(_) =>
      case Failure(err) => // error handling
    }

    coll.get("test_id", timeout = 10.seconds).flatMap(_.contentAs[JsonObject]) match {
      case Success(content: JsonObject) =>
        // The 'dyn' call allows looking up fields dynamically:
        println(content.dyn.name.str)

        // There's also accessors:
        println(content.str("name"))

        // The default accessors can throw exceptions (unlike the rest of the Scala SDK).  Use the 'safe' accessors
        // to get a Try-based interface:
        content.safe.str("name") match {
          case Success(field) =>
          case Failure(err) =>
        }
      case Failure(err) => // error handling
    }


    val jsonString =
      """{
        |  "name": "Emmy-lou Dickerson",
        |  "age": 26,
        |  "animals": [
        |    "cat",
        |    "dog",
        |    "parrot"
        |  ],
        |  "attributes": {
        |    "hair": "brown",
        |    "dimensions": {
        |      "height": 67,
        |      "weight": 175
        |    },
        |    "hobbies": [
        |      {
        |        "type": "winter sports",
        |        "name": "curling"
        |      },
        |      {
        |        "type": "summer sports",
        |        "name": "water skiing",
        |        "details": {
        |          "location": {
        |            "lat": 49.28273,
        |            "long": -123.120735
        |          }
        |        }
        |      }
        |    ]
        |  }
        |}""".stripMargin

    coll.upsert("test_id", jsonString) match {
      case Success(_) =>
      case Failure(err) => // error handling
    }

    coll.get("test_id", timeout = 10.seconds).flatMap(_.contentAs[String]) match {
      case Success(content: String) => println(content)
      case Failure(err) => // error handling
    }


    val joan = Person(
      name = "Joan Deere",
      age = 28,
      animals = Seq("kitty", "puppy"),
      Attributes(
        Dimensions(65, 120),
        hair = "brown",
        Seq(Hobby(
          name = "curling",
          typ = "winter",
          Details(Location(-121.886330, 37.338207))
        ))
      )
    )

    coll.upsert("test_id", joan) match {
      case Success(_) =>
      case Failure(err) => // error handling
    }

    val result = coll.get("test_id")

    result.flatMap(_.contentAs[Person]) match {
      case Success(content: Person) => println(content.name)
      case Failure(err) => // error handling
    }

    // Handling multiple Try errors using flatmap
    coll.upsert("test_id", joan)
      .flatMap(_ => coll.get("test_id"))
      .flatMap(_.contentAs[Person]) match {
      case Success(content: Person) =>
      case Failure(err) => // error handling
    }

    // Handling multiple Try errors using a for-comprehension
    (for {
      _ <- coll.upsert("test_id", joan)
      doc <- coll.get("test_id")
      content <- doc.contentAs[Person]
    } yield content) match {
      case Success(content: Person) =>
      case Failure(err) => // error handling
    }

    coll.get("test_id").flatMap(_.contentAs[Person]) match {
      case Success(content: Person) => println(content.name)
      case Failure(err) => // error handling
    }


    // Basic Key/Value Projections
    coll.get("test_id", project = Seq("name", "age", "attributes.hair")).flatMap(_.contentAs[JsonObject]) match {
      case Success(content: JsonObject) =>
        val dyn = content.dyn
        val name = dyn.name.str
        val age = dyn.age.num
        val hair = dyn.attributes.hair.str
      case Failure(err) => // error handling
    }

    // Basic Durability using Synchronous Replication
    coll.insert("test_id", joan, durability = Durability.MajorityAndPersistOnMaster)
  }

}
